#include "qcccrypto.h"
#include  <iostream>
#include "string.h"
#include <pthread.h>
#include <condition_variable>
#include "common/debug.h"
#include "include/scope_guard.h"
#include "common/dout.h"
#include "common/errno.h"
#include <atomic>
#include <utility>

// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "QccCrypto: ";
}
// -----------------------------------------------------------------------------
  class QatCipher {
   private:
    CpaInstanceHandle cyInstHandle{nullptr};
    CpaCySymSessionCtx sessionCtx{nullptr};
    CpaBufferList *pBufferList{nullptr};
    boost::asio::io_context& context;
    yield_context yield;
   protected:
    CpaCySymSessionSetupData* sessionSetupData;
   public:
    template <typename CompletionToken>
    auto async_perform_op(CompletionToken&& token, CpaCySymOpData& pOpData);
    std::function<void(boost::system::error_code)> completion_handler;

    QatCipher (CpaInstanceHandle cyInstHandle, const CpaCySymCipherDirection op_type, Cpa8U *key,
               boost::asio::io_context& context, yield_context yield);
    QatCipher (const QatCipher &qat) = delete;
    QatCipher (QatCipher &&qat) = delete;
    ~QatCipher ();
    void Restart();
    void Final(unsigned char* out, const unsigned char* in, size_t size, Cpa8U *iv);
    static inline void qcc_contig_mem_free(void **ptr) {
      if (*ptr) {
        qaeMemFreeNUMA(ptr);
        *ptr = NULL;
      }
    }

    static inline CpaStatus qcc_contig_mem_alloc(void **ptr, Cpa32U size, Cpa32U alignment = 1) {
      *ptr = qaeMemAllocNUMA(size, 0, alignment);
      if (NULL == *ptr)
      {
        return CPA_STATUS_RESOURCE;
      }
      return CPA_STATUS_SUCCESS;
    }
  };

template <typename CompletionToken>
auto QatCipher::async_perform_op(CompletionToken&& token, CpaCySymOpData& pOpData)
{
  CpaStatus status = CPA_STATUS_SUCCESS;
  using boost::asio::async_completion;
  using Signature = void(boost::system::error_code);
  async_completion<CompletionToken, Signature> init(token);
  
  auto ex = boost::asio::get_associated_executor(init.completion_handler);
  completion_handler = [this, ex, handler = init.completion_handler](boost::system::error_code ec){
    boost::asio::post(ex, std::bind(handler, ec));
  };

  status = cpaCySymPerformOp(
    cyInstHandle,
    (void *)this,
    &pOpData,
    pBufferList,
    pBufferList,
    NULL);

  if (status != CPA_STATUS_SUCCESS) {
    auto ec = boost::system::error_code{status, boost::system::system_category()};

    boost::asio::dispatch(ex, [this, &ec](){
      completion_handler(ec);
    });
  }

  return init.result.get();
}

/*
 * Callback function
 */
// static void symDpCallback(CpaCySymDpOpData *pOpData,
//                         CpaStatus status,
//                         CpaBoolean verifyResult)
// {
//   if (nullptr != pOpData->pCallbackTag)
//   {
//     if (static_cast<QatCrypto*>(pOpData->pCallbackTag)->complete()) {
//       static_cast<QatCrypto*>(pOpData->pCallbackTag)->completion_handler(boost::system::error_code{});
//     }
//   }
// }

static void symCallback(void *pCallbackTag,
                        CpaStatus status,
                        const CpaCySymOp operationType,
                        void *pOpData,
                        CpaBufferList *pDstBuffer,
                        CpaBoolean verifyResult)
{
    if (NULL != pCallbackTag)
    {
        /* indicate that the function has been called */
        static_cast<QatCipher*>(pCallbackTag)->completion_handler(boost::system::error_code{});
    }
}


static std::mutex qcc_alloc_mutex;
static std::mutex qcc_eng_mutex;
static std::atomic<bool> init_called = { false };

template <typename CompletionToken>
auto QccCrypto::async_get_instance(CompletionToken&& token) {
  using boost::asio::async_completion;
  using Signature = void(int);
  async_completion<CompletionToken, Signature> init(token);

  auto ex = boost::asio::get_associated_executor(init.completion_handler);

  boost::asio::post(my_context, [this, ex, handler = std::move(init.completion_handler)]()mutable{
    auto handler1 = std::move(handler);
    if (!open_instances.empty()) {
      int avail_inst = open_instances.front();
      open_instances.pop();
      boost::asio::post(ex, std::bind(handler1, avail_inst));
    } else {
      instance_completions.push([this, ex, handler2 = std::move(handler1)](int inst)mutable{
        boost::asio::post(ex, std::bind(handler2, inst));
      });
    }
  });
  return init.result.get();
}

void QccCrypto::QccFreeInstance(int entry) {
  boost::asio::post(my_context, [this, entry]()mutable{
    if (!instance_completions.empty()) {
      instance_completions.front()(entry);
      instance_completions.pop();
    } else {
      open_instances.push(entry);
    }
  });
}

void QccCrypto::cleanup() {
  icp_sal_userStop();
  qaeMemDestroy();
  is_init = false;
  init_called = false;
  derr << "Failure during QAT init sequence. Quitting" << dendl;
}

void QccCrypto::poll_instances(void) {
  while (!thread_stop) {
    for (int iter = 0; iter < qcc_inst->num_instances; iter++) {
      if (qcc_inst->is_polled[iter] == CPA_TRUE) {
        icp_sal_CyPollDpInstance(qcc_inst->cy_inst_handles[iter], 0);
      }
    }
    std::this_thread::yield();
  }
}

/*
 * We initialize QAT instance and everything that is common for all ops
*/
bool QccCrypto::init(const size_t chunk_size) {

  std::lock_guard<std::mutex> l(qcc_eng_mutex);
  CpaStatus stat = CPA_STATUS_SUCCESS;
  this->chunk_size = chunk_size;

  if (init_called) {
    dout(10) << "Init sequence already called. Skipping duplicate call" << dendl;
    return true;
  }

  // First call to init
  dout(15) << "First init for QAT" << dendl;
  init_called = true;

  // Find if the usermode memory driver is available. We need to this to
  // create contiguous memory needed by QAT.
  stat = qaeMemInit();
  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to load memory driver" << dendl;
    this->cleanup();
    return false;
  }

  stat = icp_sal_userStart("CEPH");
  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to start qat device" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_inst, sizeof(QCCINST));
  if (qcc_inst == NULL) {
    derr << "Unable to alloc mem for instance struct" << dendl;
    this->cleanup();
    return false;
  }

  // Initialize contents of qcc_inst
  qcc_inst->num_instances = 0;
  qcc_inst->cy_inst_handles = NULL;

  stat = cpaCyGetNumInstances(&(qcc_inst->num_instances));
  if ((stat != CPA_STATUS_SUCCESS) || (qcc_inst->num_instances <= 0)) {
    derr << "Unable to find available instances" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_inst->cy_inst_handles,
      ((int)qcc_inst->num_instances * sizeof(CpaInstanceHandle)));
  if (qcc_inst->cy_inst_handles == NULL) {
    derr << "Unable to allocate instances array memory" << dendl;
    this->cleanup();
    return false;
  }

  stat = cpaCyGetInstances(qcc_inst->num_instances, qcc_inst->cy_inst_handles);
  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to get instances" << dendl;
    this->cleanup();
    return false;
  }
  dout(1) << "Get instances num: " << qcc_inst->num_instances << dendl;

  int iter = 0;
  //Start Instances
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    stat = cpaCyStartInstance(qcc_inst->cy_inst_handles[iter]);
    if (stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to start instance" << dendl;
      this->cleanup();
      return false;
    }
  }

  qcc_os_mem_alloc((void **)&qcc_inst->is_polled,
      ((int)qcc_inst->num_instances * sizeof(CpaBoolean)));
  CpaInstanceInfo2 info;
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    qcc_inst->is_polled[iter] = cpaCyInstanceGetInfo2(qcc_inst->cy_inst_handles[iter],
        &info) == CPA_STATUS_SUCCESS ? info.isPolled : CPA_FALSE;
  }

  // Allocate memory structures for all instances
  qcc_os_mem_alloc((void **)&qcc_sess,
      ((int)qcc_inst->num_instances * sizeof(QCCSESS)));
  if (qcc_sess == NULL) {
    derr << "Unable to allocate memory for session struct" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_op_mem,
      ((int)qcc_inst->num_instances * sizeof(QCCOPMEM)));
  if (qcc_sess == NULL) {
    derr << "Unable to allocate memory for opmem struct" << dendl;
    this->cleanup();
    return false;
  }

  //At this point we are only doing an user-space version.
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    stat = cpaCySetAddressTranslation(qcc_inst->cy_inst_handles[iter],
                                       qaeVirtToPhysNUMA);
    if (stat == CPA_STATUS_SUCCESS) {
      open_instances.push(iter);
      qcc_op_mem[iter].is_mem_alloc = false;

      // stat = cpaCySymDpRegCbFunc(qcc_inst->cy_inst_handles[iter], symDpCallback);
      // if (stat != CPA_STATUS_SUCCESS) {
      //   dout(1) << "Unable to register callback function for instance " << iter << " with status = " << stat << dendl;
      //   return false;
      // }
    } else {
      dout(1) << "Unable to find address translations of instance " << iter << dendl;
      this->cleanup();
      return false;
    }
  }

  qat_poll_thread = make_named_thread("qat_poll", &QccCrypto::poll_instances, this);
  
  work_guard = std::make_unique<work_guard_type>(my_context.get_executor());
  qat_context_thread = make_named_thread("qat_context", &QccCrypto::my_context_run, this);
  is_init = true;
  dout(10) << "Init complete" << dendl;
  return true;
}

void QccCrypto::my_context_run() {
  my_context.run();
}

bool QccCrypto::destroy() {
  if((!is_init) || (!init_called)) {
    dout(15) << "QAT not initialized here. Nothing to do" << dendl;
    return false;
  }

  thread_stop = true;
  if (qat_poll_thread.joinable()) {
    qat_poll_thread.join();
  }
  my_context.stop();
  if (qat_context_thread.joinable()) {
    qat_context_thread.join();
  }

  dout(10) << "Destroying QAT crypto & related memory" << dendl;
  int iter = 0;

  // Free up op related memory
  // for (iter =0; iter < qcc_inst->num_instances; iter++) {
  //   for (size_t i = 0; i < MAX_NUM_SYM_REQ_BATCH; i++) {
  //     qcc_contig_mem_free((void **)&(qcc_op_mem[iter].src_buff[i]));
  //     qcc_contig_mem_free((void **)&(qcc_op_mem[iter].iv_buff[i]));
  //     qcc_contig_mem_free((void **)&(qcc_op_mem[iter].sym_op_data[i]));
  //   }
  // }

  // Free up Session memory
  // for (iter = 0; iter < qcc_inst->num_instances; iter++) {
  //   cpaCySymDpRemoveSession(qcc_inst->cy_inst_handles[iter], qcc_sess[iter].sess_ctx);
  //   qcc_contig_mem_free((void **)&(qcc_sess[iter].sess_ctx));
  // }

  // Stop QAT Instances
  for (iter = 0; iter < qcc_inst->num_instances; iter++) {
    cpaCyStopInstance(qcc_inst->cy_inst_handles[iter]);
  }

  // Free up the base structures we use
  qcc_os_mem_free((void **)&qcc_op_mem);
  qcc_os_mem_free((void **)&qcc_sess);
  qcc_os_mem_free((void **)&(qcc_inst->cy_inst_handles));
  qcc_os_mem_free((void **)&(qcc_inst->is_polled));
  qcc_os_mem_free((void **)&qcc_inst);

  //Un-init memory driver and QAT HW
  icp_sal_userStop();
  qaeMemDestroy();
  init_called = false;
  is_init = false;
  return true;
}

bool QccCrypto::perform_op_batch(unsigned char* out, const unsigned char* in, size_t size,
    Cpa8U *iv,
    Cpa8U *key,
    CpaCySymCipherDirection op_type,
    optional_yield y)
{
  if (!init_called) {
    dout(10) << "QAT not intialized yet. Initializing now..." << dendl;
    if (!QccCrypto::init(chunk_size)) {
      derr << "QAT init failed" << dendl;
      return false;
    }
  }

  if (!is_init)
  {
    dout(10) << "QAT not initialized in this instance or init failed" << dendl;
    return is_init;
  }
  CpaStatus status = CPA_STATUS_SUCCESS;
  int avail_inst = -1;

  yield_context yield = y.get_yield_context();
  avail_inst = async_get_instance(yield);

  dout(1) << "Using dp_batch inst " << avail_inst << dendl;

  auto sg = make_scope_guard([this, avail_inst] {
      //free up the instance irrespective of the op status
      dout(15) << "Completed task under " << avail_inst << dendl;
      qcc_op_mem[avail_inst].op_complete = false;
      QccCrypto::QccFreeInstance(avail_inst);
      });

  /*
   * Allocate buffers for this version of the instance if not already done.
   * Hold onto to most of them until destructor is called.
  */
  // if (qcc_op_mem[avail_inst].is_mem_alloc == false) {
  //   for (size_t i = 0; i < MAX_NUM_SYM_REQ_BATCH; i++) {
  //     // Allocate IV memory
  //     status = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].iv_buff[i]), AES_256_IV_LEN, 8);
  //     if (status != CPA_STATUS_SUCCESS) {
  //       derr << "Unable to allocate iv_buff memory" << dendl;
  //       return false;
  //     }

  //     // Allocate src memory
  //     status = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].src_buff[i]), chunk_size, 8);
  //     if (status != CPA_STATUS_SUCCESS) {
  //       derr << "Unable to allocate src_buff memory" << dendl;
  //       return false;
  //     }

  //     //Setup OpData
  //     status = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].sym_op_data[i]),
  //         sizeof(CpaCySymDpOpData), 8);
  //     if (status != CPA_STATUS_SUCCESS) {
  //       derr << "Unable to allocate opdata memory" << dendl;
  //       return false;
  //     }
  //   }

  //   // Set memalloc flag so that we don't go through this exercise again.
  //   qcc_op_mem[avail_inst].is_mem_alloc = true;
  //   qcc_sess[avail_inst].sess_ctx = nullptr;
  //   status = initSession(qcc_inst->cy_inst_handles[avail_inst],
  //                            &(qcc_sess[avail_inst].sess_ctx),
  //                            (Cpa8U *)key,
  //                            op_type);
  // } else {
  //   do {
  //     status = updateSession(qcc_sess[avail_inst].sess_ctx,
  //                               (Cpa8U *)key,
  //                               op_type);
  //     if (status == CPA_STATUS_RETRY) {
  //       cpaCySymDpRemoveSession(qcc_inst->cy_inst_handles[avail_inst], qcc_sess[avail_inst].sess_ctx);
  //       status = initSession(qcc_inst->cy_inst_handles[avail_inst],
  //                            &(qcc_sess[avail_inst].sess_ctx),
  //                            (Cpa8U *)key,
  //                            op_type);
  //     }
  //   } while (status == CPA_STATUS_RETRY);
  // }
  // if (unlikely(status != CPA_STATUS_SUCCESS)) {
  //   derr << "Unable to init session with status =" << status << dendl;
  //   return false;
  // }

  yield_context yield = y.get_yield_context();
  boost::asio::io_context& context = y.get_io_context();
  QatCipher cipher(cyInstHandle, op_type, key, context, yield);

  return symPerformOp(avail_inst,
                      qcc_sess[avail_inst].sess_ctx,
                      in,
                      out,
                      size,
                      reinterpret_cast<Cpa8U*>(iv),
                      AES_256_IV_LEN, y);
}

/*
 * Perform session update
 */
CpaStatus QccCrypto::updateSession(CpaCySymSessionCtx sessionCtx,
                               Cpa8U *pCipherKey,
                               CpaCySymCipherDirection cipherDirection) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  CpaCySymSessionUpdateData sessionUpdateData = {0};

  sessionUpdateData.flags = CPA_CY_SYM_SESUPD_CIPHER_KEY;
  sessionUpdateData.flags |= CPA_CY_SYM_SESUPD_CIPHER_DIR;
  sessionUpdateData.pCipherKey = pCipherKey;
  sessionUpdateData.cipherDirection = cipherDirection;

  status = cpaCySymUpdateSession(sessionCtx, &sessionUpdateData);

  if (unlikely(status != CPA_STATUS_SUCCESS)) {
    dout(1) << "cpaCySymUpdateSession failed with status = " << status << dendl;
  }

  return status;
}

CpaStatus QccCrypto::initSession(CpaInstanceHandle cyInstHandle,
                             CpaCySymSessionCtx *sessionCtx,
                             Cpa8U *pCipherKey,
                             CpaCySymCipherDirection cipherDirection) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa32U sessionCtxSize = 0;
  CpaCySymSessionSetupData sessionSetupData;
  memset(&sessionSetupData, 0, sizeof(sessionSetupData));

  sessionSetupData.sessionPriority = CPA_CY_PRIORITY_NORMAL;
  sessionSetupData.symOperation = CPA_CY_SYM_OP_CIPHER;
  sessionSetupData.cipherSetupData.cipherAlgorithm = CPA_CY_SYM_CIPHER_AES_CBC;
  sessionSetupData.cipherSetupData.cipherKeyLenInBytes = AES_256_KEY_SIZE;
  sessionSetupData.cipherSetupData.pCipherKey = pCipherKey;
  sessionSetupData.cipherSetupData.cipherDirection = cipherDirection;

  if (nullptr == *sessionCtx) {
    status = cpaCySymDpSessionCtxGetSize(cyInstHandle, &sessionSetupData, &sessionCtxSize);
    if (likely(CPA_STATUS_SUCCESS == status)) {
      status = qcc_contig_mem_alloc((void **)(sessionCtx), sessionCtxSize);
    } else {
      dout(1) << "cpaCySymDpSessionCtxGetSize failed with status = " << status << dendl;
    }
  }
  if (likely(CPA_STATUS_SUCCESS == status)) {
    status = cpaCySymDpInitSession(cyInstHandle,
                                   &sessionSetupData,
                                   *sessionCtx);
    if (unlikely(status != CPA_STATUS_SUCCESS)) {
      dout(1) << "cpaCySymDpInitSession failed with status = " << status << dendl;
    }
  } else {
    dout(1) << "Session alloc failed with status = " << status << dendl;
  }
  return status;
}

template <typename CompletionToken>
auto QatCrypto::async_perform_op(int avail_inst, std::vector<CpaCySymDpOpData*>& pOpDataVec, CompletionToken&& token) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  using boost::asio::async_completion;
  using Signature = void(boost::system::error_code);
  async_completion<CompletionToken, Signature> init(token);
  auto ex = boost::asio::get_associated_executor(init.completion_handler);
  completion_handler = [this, ex, handler = init.completion_handler](boost::system::error_code ec){
    boost::asio::post(ex, std::bind(handler, ec));
  };

  status = cpaCySymDpEnqueueOpBatch(pOpDataVec.size(), &pOpDataVec[0], CPA_TRUE);

  if (status != CPA_STATUS_SUCCESS) {
    auto ec = boost::system::error_code{status, boost::system::system_category()};

    boost::asio::dispatch(context, [this, &ec](){
      completion_handler(ec);
    });
  }
  return init.result.get();
}

bool QccCrypto::symPerformOp(int avail_inst,
                              CpaCySymSessionCtx sessionCtx,
                              const Cpa8U *pSrc,
                              Cpa8U *pDst,
                              Cpa32U size,
                              Cpa8U *pIv,
                              Cpa32U ivLen,
                              optional_yield y) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa32U one_batch_size = chunk_size * MAX_NUM_SYM_REQ_BATCH;
  Cpa32U iv_index = 0;
  for (Cpa32U off = 0; off < size; off += one_batch_size) {
    QatCrypto helper(y.get_io_context(), y.get_yield_context(), this);
    std::vector<CpaCySymDpOpData*> pOpDataVec;
    for (Cpa32U offset = off, i = 0; offset < size && i < MAX_NUM_SYM_REQ_BATCH; offset += chunk_size, i++) {
      CpaCySymDpOpData *pOpData = qcc_op_mem[avail_inst].sym_op_data[i];
      Cpa8U *pSrcBuffer = qcc_op_mem[avail_inst].src_buff[i];
      Cpa8U *pIvBuffer = qcc_op_mem[avail_inst].iv_buff[i];
      Cpa32U process_size = offset + chunk_size <= size ? chunk_size : size - offset;
      if (CPA_STATUS_SUCCESS == status) {
        // copy source into buffer
        memcpy(pSrcBuffer, pSrc + offset, process_size);
        // copy IV into buffer
        memcpy(pIvBuffer, &pIv[iv_index * ivLen], ivLen);
        iv_index++;

        helper.count++;

        //pOpData assignment
        pOpData->thisPhys = qaeVirtToPhysNUMA(pOpData);
        pOpData->instanceHandle = qcc_inst->cy_inst_handles[avail_inst];
        pOpData->sessionCtx = sessionCtx;
        pOpData->pCallbackTag = &helper;
        pOpData->cryptoStartSrcOffsetInBytes = 0;
        pOpData->messageLenToCipherInBytes = process_size;
        pOpData->iv = qaeVirtToPhysNUMA(pIvBuffer);
        pOpData->pIv = pIvBuffer;
        pOpData->ivLenInBytes = ivLen;
        pOpData->srcBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
        pOpData->srcBufferLen = process_size;
        pOpData->dstBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
        pOpData->dstBufferLen = process_size;

        pOpDataVec.push_back(pOpData);

      }
    }

    boost::system::error_code ec;
    yield_context yield = y.get_yield_context();
    ceph_assert(!helper.completion_handler);

    do {
      helper.async_perform_op(avail_inst, pOpDataVec, yield[ec]);
    } while (!ec && ec.value() == CPA_STATUS_RETRY);

    if (likely(CPA_STATUS_SUCCESS == status)) {
      for (Cpa32U offset = off, i = 0; offset < size && i < MAX_NUM_SYM_REQ_BATCH; offset += chunk_size, i++) {
        Cpa8U *pSrcBuffer = qcc_op_mem[avail_inst].src_buff[i];
        Cpa32U process_size = offset + chunk_size <= size ? chunk_size : size - offset;
        memcpy(pDst + offset, pSrcBuffer, process_size);
      }
    }
  }

  Cpa32U max_used_buffer_num = iv_index > MAX_NUM_SYM_REQ_BATCH ? MAX_NUM_SYM_REQ_BATCH : iv_index;
  for (Cpa32U i = 0; i < max_used_buffer_num; i++) {
    memset(qcc_op_mem[avail_inst].src_buff[i], 0, chunk_size);
    memset(qcc_op_mem[avail_inst].iv_buff[i], 0, ivLen);
  }
  return (status == CPA_STATUS_SUCCESS);
}


bool QccCrypto::symPerformOp_single(int avail_inst,
                              CpaCySymSessionCtx sessionCtx,
                              const Cpa8U *pSrc,
                              Cpa8U *pDst,
                              Cpa32U size,
                              Cpa8U *pIv,
                              Cpa32U ivLen,
                              optional_yield y) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa32U one_batch_size = chunk_size * MAX_NUM_SYM_REQ_BATCH;
  Cpa32U iv_index = 0;

  for (Cpa32U off = 0; off < size; off += one_batch_size) {
    QatCrypto helper(y.get_io_context(), y.get_yield_context(), this);
    std::vector<CpaCySymDpOpData*> pOpDataVec;
    for (Cpa32U offset = off, i = 0; offset < size && i < MAX_NUM_SYM_REQ_BATCH; offset += chunk_size, i++) {
      CpaCySymDpOpData *pOpData = qcc_op_mem[avail_inst].sym_op_data[i];
      Cpa8U *pSrcBuffer = qcc_op_mem[avail_inst].src_buff[i];
      Cpa8U *pIvBuffer = qcc_op_mem[avail_inst].iv_buff[i];
      Cpa32U process_size = offset + chunk_size <= size ? chunk_size : size - offset;
      if (CPA_STATUS_SUCCESS == status) {
        // copy source into buffer
        memcpy(pSrcBuffer, pSrc + offset, process_size);
        // copy IV into buffer
        memcpy(pIvBuffer, &pIv[iv_index * ivLen], ivLen);
        iv_index++;

        helper.count++;

        //pOpData assignment
        pOpData->thisPhys = qaeVirtToPhysNUMA(pOpData);
        pOpData->instanceHandle = qcc_inst->cy_inst_handles[avail_inst];
        pOpData->sessionCtx = sessionCtx;
        pOpData->pCallbackTag = &helper;
        pOpData->cryptoStartSrcOffsetInBytes = 0;
        pOpData->messageLenToCipherInBytes = process_size;
        pOpData->iv = qaeVirtToPhysNUMA(pIvBuffer);
        pOpData->pIv = pIvBuffer;
        pOpData->ivLenInBytes = ivLen;
        pOpData->srcBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
        pOpData->srcBufferLen = process_size;
        pOpData->dstBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
        pOpData->dstBufferLen = process_size;

        pOpDataVec.push_back(pOpData);

      }
    }

    boost::system::error_code ec;
    yield_context yield = y.get_yield_context();
    ceph_assert(!helper.completion_handler);

    do {
      helper.async_perform_op(avail_inst, pOpDataVec, yield[ec]);
    } while (!ec && ec.value() == CPA_STATUS_RETRY);

    if (likely(CPA_STATUS_SUCCESS == status)) {
      for (Cpa32U offset = off, i = 0; offset < size && i < MAX_NUM_SYM_REQ_BATCH; offset += chunk_size, i++) {
        Cpa8U *pSrcBuffer = qcc_op_mem[avail_inst].src_buff[i];
        Cpa32U process_size = offset + chunk_size <= size ? chunk_size : size - offset;
        memcpy(pDst + offset, pSrcBuffer, process_size);
      }
    }
  }

  Cpa32U max_used_buffer_num = iv_index > MAX_NUM_SYM_REQ_BATCH ? MAX_NUM_SYM_REQ_BATCH : iv_index;
  for (Cpa32U i = 0; i < max_used_buffer_num; i++) {
    memset(qcc_op_mem[avail_inst].src_buff[i], 0, chunk_size);
    memset(qcc_op_mem[avail_inst].iv_buff[i], 0, ivLen);
  }
  return (status == CPA_STATUS_SUCCESS);
}





QatCipher::QatCipher(CpaInstanceHandle cyInstHandle, const CpaCySymCipherDirection op_type, Cpa8U *key,
                     boost::asio::io_context& context, yield_context yield)
  : cyInstHandle(cyInstHandle), context(context), yield(yield) {
  sessionSetupData = new CpaCySymSessionSetupData;
  memset(sessionSetupData, 0, sizeof(CpaCySymSessionSetupData));
  sessionSetupData->sessionPriority = CPA_CY_PRIORITY_NORMAL;
  sessionSetupData->symOperation = CPA_CY_SYM_OP_CIPHER;
  sessionSetupData->cipherSetupData.cipherAlgorithm = CPA_CY_SYM_CIPHER_AES_CBC;
  sessionSetupData->cipherSetupData.cipherKeyLenInBytes = 32;
  sessionSetupData->cipherSetupData.cipherDirection = op_type;
  sessionSetupData->cipherSetupData.pCipherKey = key;
}

struct CpaBufferListDeleter {
  void operator() (CpaBufferList* pBufferList) {
    void *pBufferMeta = nullptr;
    void *pSrcBuffer = nullptr;
    void *pIvBuffer = nullptr;

    if (pBufferList != nullptr) {
      pBufferMeta   = pBufferList->pPrivateMetaData;
      pIvBuffer = pBufferList->pUserData;
      pSrcBuffer    = pBufferList->pBuffers->pData;
      QatCipher::qcc_contig_mem_free(&pSrcBuffer);
      free(pBufferList);
      QatCipher::qcc_contig_mem_free(&pBufferMeta);
      QatCipher::qcc_contig_mem_free((void**)&pIvBuffer);
    }
  }
};

using buffer_ptr = std::unique_ptr<CpaBufferList, CpaBufferListDeleter>;
static std::vector<buffer_ptr> buffers;
static std::mutex buffers_mutex;

QatCipher::~QatCipher() {
  if (pBufferList != nullptr) {
    std::scoped_lock lock{buffers_mutex};
    buffers.emplace_back(pBufferList);
  }

  if (sessionCtx != nullptr) {
    CpaBoolean sessionInUse = CPA_FALSE;
    do {
      cpaCySymSessionInUse(sessionCtx, &sessionInUse);
    } while (sessionInUse);

    cpaCySymRemoveSession(cyInstHandle, sessionCtx);
    qcc_contig_mem_free((void**)&sessionCtx);
  }

  delete sessionSetupData;
}

void QatCipher::Restart() {
  CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa32U sessionCtxSize;
  if (sessionCtx == nullptr) {
    // first need to alloc session memory
    status = cpaCySymSessionCtxGetSize(cyInstHandle, sessionSetupData, &sessionCtxSize);
    if (status != CPA_STATUS_SUCCESS) {
      std::cout <<  "cpaCySymSessionCtxGetSize failed, stat = " << status << std::endl;
      throw "cpaCySymSessionCtxGetSize failed";
    }
    status = qcc_contig_mem_alloc((void**)&sessionCtx, sessionCtxSize);
    if (status != CPA_STATUS_SUCCESS) {
      std::cout << "Failed to alloc contiguous memory for SessionCtx, stat = " << status << std::endl;
      throw "Failed to alloc contiguous memory for SessionCtx";
    }
  }

  status = cpaCySymInitSession(cyInstHandle, symCallback, sessionSetupData, sessionCtx);
  if (status != CPA_STATUS_SUCCESS) {
    std::cout << "cpaCySymInitSession failed, stat = " << status << std::endl;;
    throw "cpaCySymInitSession failed";
  }
}

/*******************************************************************************************
 **   -----------------|---------------------------|
 **                    |-------numBuffers----------|
 **    CpaBufferList   |-------pBuffers------------|-----
 **                    |-------pUserData-----------|    |
 **                    |-------pPrivateMetaData----|    |
 **   -----------------|---------------------------|<----
 **                    |-------dataLenInByte-------|      -----------------------------
 **    CpaFlatBuffer   |-------pData---------------|---->|contiguous memory| pSrcBuffer
 **   -----------------|---------------------------|      -----------------------------
*********************************************************************************************/
static CpaBufferList* getCpaBufferList(CpaInstanceHandle cyInstHandle) {

  CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa8U *pBufferMeta = nullptr;
  Cpa32U bufferMetaSize = 0;
  CpaBufferList *pBufferList = nullptr;
  CpaFlatBuffer *pFlatBuffer = nullptr;
  Cpa32U bufferSize = 4096;
  Cpa32U numBuffers = 1;
  Cpa32U bufferListMemSize = sizeof(CpaBufferList) + (numBuffers * sizeof(CpaFlatBuffer));
  Cpa8U *pSrcBuffer = nullptr;
  Cpa8U *pIvBuffer = nullptr;

  {
    std::scoped_lock lock{buffers_mutex};
    if (!buffers.empty()) {
      pBufferList = buffers.back().release();
      buffers.pop_back();
      return pBufferList;
    }
  }

  status = cpaCyBufferListGetMetaSize(cyInstHandle, numBuffers, &bufferMetaSize);

  if (CPA_STATUS_SUCCESS == status)
  {
    status = QatCipher::qcc_contig_mem_alloc((void**)&pBufferMeta, bufferMetaSize);
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    pBufferList = (CpaBufferList *)malloc(bufferListMemSize);
    if (pBufferList == nullptr) return nullptr;
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    status = QatCipher::qcc_contig_mem_alloc((void**)&pSrcBuffer, bufferSize);
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    status = QatCipher::qcc_contig_mem_alloc((void**)&pIvBuffer, 16);
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    /* increment by sizeof(CpaBufferList) to get at the
     * array of flatbuffers */
    pFlatBuffer = (CpaFlatBuffer *)(pBufferList + 1);

    pBufferList->numBuffers = 1;
    pBufferList->pBuffers = pFlatBuffer;
    pBufferList->pUserData = pIvBuffer;
    pBufferList->pPrivateMetaData = pBufferMeta;

    pFlatBuffer->dataLenInBytes = bufferSize;
    pFlatBuffer->pData = pSrcBuffer;
  }

  return pBufferList;
}

void QatCipher::Final(unsigned char* out, const unsigned char* in, size_t size, Cpa8U *iv) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  if (pBufferList == nullptr) {
    pBufferList = getCpaBufferList(cyInstHandle);

    if (pBufferList == nullptr) {
      std::cout << "cannot get CpaBufferList" << std::endl;
      return;
    }
  }

  memcpy(pBufferList->pBuffers->pData, in, size);
  memcpy(pBufferList->pUserData, iv, 16);

  CpaCySymOpData pOpData = {0};

  pBufferList->pBuffers->dataLenInBytes = size;

  pOpData.sessionCtx = sessionCtx;
  pOpData.packetType = CPA_CY_SYM_PACKET_TYPE_FULL;
  pOpData.pIv = (Cpa8U *)pBufferList->pUserData;
  pOpData.ivLenInBytes = 16;
  pOpData.cryptoStartSrcOffsetInBytes = 0;
  pOpData.messageLenToCipherInBytes = pBufferList->pBuffers->dataLenInBytes;

  ceph_assert(!completion_handler);
  boost::system::error_code ec;
  do {
    async_perform_op(yield[ec], pOpData);
  } while (!ec && ec.value() == CPA_STATUS_RETRY);

  if (CPA_STATUS_SUCCESS != status) {
    std::cout << "Final cpaCySymPerformOp failed. (status = " << status << std::endl;
  }

  memcpy(out, pBufferList->pBuffers->pData, size);
}