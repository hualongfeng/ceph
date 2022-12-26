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
#include <chrono>
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

static std::mutex qcc_alloc_mutex;
static std::mutex qcc_eng_mutex;
static std::condition_variable alloc_cv;
static std::atomic<bool> init_called = { false };

void QccCrypto::cleanup() {
  icp_sal_userStop();
  qaeMemDestroy();
  is_init = false;
  init_called = false;
  derr << "Failure during QAT init sequence. Quitting" << dendl;
}

void QccCrypto::poll_instances(void) {
  while (!thread_stop) {
    for (auto instance: cyInstances) {
        icp_sal_CyPollDpInstance(instance, 0);
    }
    std::this_thread::yield();
    // std::this_thread::sleep_for(std::chrono::milliseconds(1000));
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
    dout(1) << "Failed to initialize memory driver" << dendl;
    this->cleanup();
    return false;
  }

  stat = icp_sal_userStartMultiProcess("CEPH", CPA_FALSE);
  if (stat != CPA_STATUS_SUCCESS) {
    dout(1) << "Failed to start user process CEPH" << dendl;
    this->cleanup();
    return false;
  }

  stat = cpaCyGetNumInstances(&numInstances);
  if ((stat != CPA_STATUS_SUCCESS) || (numInstances <= 0)) {
    dout(1) << "No instances found for 'CEPH'" << dendl;
    this->cleanup();
    return false;
  }

  cyInstances.resize(numInstances);

  stat = cpaCyGetInstances(numInstances, &cyInstances[0]);
  if (stat != CPA_STATUS_SUCCESS) {
    dout(1) << "Unable to get instances" << dendl;
    this->cleanup();
    return false;
  }
  dout(1) << "Get instances num: " << numInstances << dendl;

  for (auto &instance : cyInstances) {
    stat = cpaCyStartInstance(instance);
    if (stat != CPA_STATUS_SUCCESS) {
      dout(1) << "Unable to start instance" << dendl;
      this->cleanup();
      return false;
    }
    stat = cpaCySetAddressTranslation(instance, qaeVirtToPhysNUMA);
    if (stat != CPA_STATUS_SUCCESS) {
      dout(1) << "Unable to find address translations of instance " << dendl;
      this->cleanup();
      return false;
    }
    CpaInstanceInfo2 info2;
    stat = cpaCyInstanceGetInfo2(instance, &info2);
    if ((stat == CPA_STATUS_SUCCESS) && (info2.isPolled == CPA_TRUE)) {
      stat = cpaCySymDpRegCbFunc(instance, symDpCallback);
      if (stat != CPA_STATUS_SUCCESS) {
        this->cleanup();
        dout(1) << "Unable to register callback function for instance " << " with status = " << stat << dendl;
        return false;
      }
    } else {
      dout(1) << "One instance cannot poll, check the config" << dendl;
      this->cleanup();
      return false;
    }
  }

  qat_poll_thread = make_named_thread("qat_poll", &QccCrypto::poll_instances, this);
  is_init = true;
  dout(10) << "Init complete" << dendl;
  return true;
}

QccCrypto::OPMEM::OPMEM(size_t chunk_size) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  status = qcc_contig_mem_alloc((void **)&iv_buff, AES_256_IV_LEN, 8);
  if (status != CPA_STATUS_SUCCESS) {
    dout(1) << "Unable to allocate iv_buff memory" << dendl;
    throw "Unable to allocate iv_buff memory";
  }

  // Allocate src memory
  status = qcc_contig_mem_alloc((void **)&src_buff, chunk_size, 8);
  if (status != CPA_STATUS_SUCCESS) {
    dout(1) << "Unable to allocate src_buff memory" << dendl;
    throw "Unable to allocate src_buff memory";
  }

  //Setup OpData
  status = qcc_contig_mem_alloc((void **)&sym_op_data, sizeof(CpaCySymDpOpData), 8);
  if (status != CPA_STATUS_SUCCESS) {
    dout(1) << "Unable to allocate opdata memory" << dendl;
    throw "Unable to allocate opdata memory";
  }
}

QccCrypto::OPMEM::~OPMEM() {
  if (src_buff != nullptr)    qcc_contig_mem_free((void **)src_buff);
  if (iv_buff != nullptr)     qcc_contig_mem_free((void **)iv_buff);
  if (sym_op_data != nullptr) qcc_contig_mem_free((void **)sym_op_data);
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

  dout(10) << "Destroying QAT crypto & related memory" << dendl;

  // Stop QAT Instances
  for (auto &instance : cyInstances) {
    cpaCyStopInstance(instance);
  }

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

  CpaInstanceHandle instance = cyInstances[index++ % numInstances];


  return symPerformOp(instance,
                      in,
                      out,
                      size,
                      reinterpret_cast<Cpa8U*>(iv),
                      AES_256_IV_LEN,
                      key, op_type,
                      y);
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

  do {
    status = cpaCySymUpdateSession(sessionCtx, &sessionUpdateData);
  } while (status == CPA_STATUS_RETRY);

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

  status = cpaCySymDpSessionCtxGetSize(cyInstHandle, &sessionSetupData, &sessionCtxSize);
  if (likely(CPA_STATUS_SUCCESS == status)) {
    status = qcc_contig_mem_alloc((void **)(sessionCtx), sessionCtxSize);
  } else {
    dout(1) << "cpaCySymDpSessionCtxGetSize failed with status = " << status << dendl;
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

bool QccCrypto::symPerformOp(CpaInstanceHandle instance,
                              const Cpa8U *pSrc,
                              Cpa8U *pDst,
                              Cpa32U size,
                              Cpa8U *pIv,
                              Cpa32U ivLen,
                              Cpa8U *key,
                              CpaCySymCipherDirection op_type,
                              optional_yield y) {
  QatCrypto crypto(y.get_io_context(), y.get_yield_context(), instance, chunk_size, this, key, op_type);
  return crypto.performOp(pSrc, pDst, size, pIv, ivLen);
}

void QccCrypto::symDpCallback(CpaCySymDpOpData *pOpData,
                          CpaStatus status,
                          CpaBoolean verifyResult)
{
  if (nullptr != pOpData->pCallbackTag)
  {
    ceph::async::post(std::move(static_cast<QatCrypto*>(pOpData->pCallbackTag)->completion),
                   boost::system::error_code{});
  }
}

template <typename CompletionToken>
auto QatCrypto::async_perform_op(CompletionToken&& token)
{
  CpaStatus status = CPA_STATUS_SUCCESS;
  using boost::asio::async_completion;
  using Signature = void(boost::system::error_code);
  async_completion<CompletionToken, Signature> init(token);
  completion = Completion::create(context.get_executor(),
                      std::move(init.completion_handler));
  do {
    status = cpaCySymDpEnqueueOpBatch(pOpDataVec.size(), &pOpDataVec[0], CPA_TRUE);
  } while (status == CPA_STATUS_RETRY);

  dout(1) << "async_perform_op: " << status << "=?" << CPA_STATUS_SUCCESS << dendl;

  if (status != CPA_STATUS_SUCCESS) {
    auto ec = boost::system::error_code{status, boost::system::system_category()};
    ceph::async::dispatch(std::move(completion), ec);
  }

  return init.result.get();
}

template <typename CompletionToken>
auto QatCrypto::async_get_op_mem(CompletionToken&& token) {
  using boost::asio::async_completion;
  using Signature = void(boost::system::error_code);
  async_completion<CompletionToken, Signature> init(token);
  op_mem_completion = Completion::create(context.get_executor(),
                      std::move(init.completion_handler));
  boost::asio::post(this->crypto->opmem_strand, [this]()mutable{
    if (crypto->op_mem_pool.empty()) {
      opmem = std::move(QccCrypto::OPMEM(chunk_size));
      crypto->op_mem_capacity++;
      dout(10) << "op_mem_capacity: " << crypto->op_mem_capacity << dendl;
    } else {
      opmem = std::move(crypto->op_mem_pool.back());
      crypto->op_mem_pool.pop_back();
    }
    ceph::async::post(std::move(op_mem_completion), boost::system::error_code{});
  });
  return init.result.get();
}

template <typename CompletionToken>
auto QatCrypto::async_get_session(CompletionToken&& token) {
  using boost::asio::async_completion;
  using Signature = void(boost::system::error_code);
  async_completion<CompletionToken, Signature> init(token);
  session_completion = Completion::create(context.get_executor(),
                      std::move(init.completion_handler));
  boost::asio::post(this->crypto->session_strand, [this]()mutable{
    CpaStatus status = CPA_STATUS_SUCCESS;
    dout(10) << "async_get_session" << dendl;
    if (crypto->session_pool[cyInstHandle].empty()) {
      status = crypto->initSession(cyInstHandle, &sessionCtx,
                          (Cpa8U *)key,
                          op_type);
      crypto->session_capacity++;
      dout(10) << "session_capacity: " << crypto->session_capacity << dendl;
    } else {
      sessionCtx = crypto->session_pool[cyInstHandle].back();
      crypto->session_pool[cyInstHandle].pop_back();
      status = crypto->updateSession(sessionCtx, (Cpa8U *)key, op_type);
    }
    ceph::async::post(std::move(session_completion), boost::system::error_code{status, boost::system::generic_category()});
  });
  return init.result.get();
}

bool QatCrypto::performOp(const Cpa8U *pSrc,
                   Cpa8U *pDst,
                   Cpa32U size,
                   Cpa8U *pIv,
                   Cpa32U ivLen) {
  // CpaStatus status = CPA_STATUS_SUCCESS;
  Cpa32U iv_index = 0;
  Cpa32U offset = 0;

  dout(10) << "start async get session" << dendl;
  boost::system::error_code ec;
  async_get_session(yield[ec]);
  dout(10) << "end async get session: " << ec.value() << dendl;

  do {
    dout(10) << "start async_get_op_mem" << dendl;
    boost::system::error_code ec;
    async_get_op_mem(yield[ec]);
    dout(10) << "end async_get_op_mem" << dendl;


    CpaCySymDpOpData *pOpData = opmem.sym_op_data;
    Cpa8U *pSrcBuffer = opmem.src_buff;
    Cpa8U *pIvBuffer = opmem.iv_buff;

    ceph_assert(pOpData != nullptr && pSrcBuffer != nullptr && pIvBuffer != nullptr);

    Cpa32U process_size = offset + chunk_size <= size ? chunk_size : size - offset;

    memcpy(pSrcBuffer, pSrc + offset, process_size);
    memcpy(pIvBuffer, &pIv[iv_index * ivLen], ivLen);

    //pOpData assignment
    pOpData->thisPhys = qaeVirtToPhysNUMA(pOpData);
    pOpData->instanceHandle = cyInstHandle;
    pOpData->sessionCtx = sessionCtx;
    pOpData->pCallbackTag = nullptr;
    pOpData->cryptoStartSrcOffsetInBytes = 0;
    pOpData->messageLenToCipherInBytes = process_size;
    pOpData->iv = qaeVirtToPhysNUMA(pIvBuffer);
    pOpData->pIv = pIvBuffer;
    pOpData->ivLenInBytes = ivLen;
    pOpData->srcBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
    pOpData->srcBufferLen = process_size;
    pOpData->dstBuffer = qaeVirtToPhysNUMA(pSrcBuffer);
    pOpData->dstBufferLen = process_size;

    offset += chunk_size;
    iv_index++;
    pOpDataVec.push_back(pOpData);
    op_mem_used.push_back(std::move(opmem));
  } while (offset < size);

  pOpDataVec.back()->pCallbackTag = this;

  dout(10) << "start async_perform_op" << dendl;

  // boost::system::error_code ec;
  async_perform_op(yield[ec]);

  dout(10) << "end async_perform_op" << dendl;

  for(size_t i = 0, off = 0; off < size; off += chunk_size) {
    Cpa32U process_size = off + chunk_size <= size ? chunk_size : size - off;
    memcpy(pDst + off, op_mem_used[i].src_buff, process_size);
    memset(op_mem_used[i].src_buff, 0, chunk_size);
    memset(op_mem_used[i].iv_buff, 0, ivLen);
  }


  boost::asio::post(crypto->opmem_strand, [crypto = this->crypto, mem_used = std::move(op_mem_used)]()mutable{
    dout(10) << "recycle op memory" << dendl;
    auto mem = std::move(mem_used);
    while(!mem.empty()) {
      crypto->op_mem_pool.push_back(std::move(mem.back()));
      mem.pop_back();
    }
    dout(10) << "end recycle op memory" << dendl;
  });

  boost::asio::post(crypto->session_strand, [cyInstHandle = this->cyInstHandle, crypto = this->crypto, sessionCtx = this->sessionCtx]()mutable{
    dout(10) << "recycle session" << dendl;
    crypto->session_pool[cyInstHandle].push_back(sessionCtx);

    dout(10) << "end recycle sessiony" << dendl;
  });

  return true;
}


