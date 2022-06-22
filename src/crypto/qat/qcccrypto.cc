#include "qcccrypto.h"
#include  <iostream>
#include "string.h"
#include <pthread.h>
#include "common/debug.h"
#include "include/scope_guard.h"
#include "common/dout.h"
#include "common/errno.h"
#include "semaphore.h"

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

#define TIMEOUT_MS 500 /* 5 seconds*/

/**
 *  *******************************************************************************
 *   * @ingroup sampleUtils
 *    *      Completion definitions
 *     *
 *      ******************************************************************************/
struct completion_struct
{
  sem_t semaphore;
};
/* Use semaphores to signal completion of events */
#define COMPLETION_STRUCT completion_struct

#define COMPLETION_INIT(s) sem_init(&((s)->semaphore), 0, 0);

#define COMPLETION_WAIT(s, timeout) (sem_wait(&((s)->semaphore)) == 0)

#define COMPLETE(s) sem_post(&((s)->semaphore))

#define COMPLETION_DESTROY(s) sem_destroy(&((s)->semaphore))


/*
 * Callback function
 * 
 * This function is "called back" (invoked by the implementation of
 * the API) when the asynchronous operation has completed.  The
 * context in which it is invoked depends on the implementation, but
 * as described in the API it should not sleep (since it may be called
 * in a context which does not permit sleeping, e.g. a Linux bottom
 * half).
 * 
 * This function can perform whatever processing is appropriate to the
 * application.  For example, it may free memory, continue processing
 * of a decrypted packet, etc.  In this example, the function checks
 * the verifyResult returned and sets the complete variable to indicate
 * it has been called.
 */
static void symDpCallback(CpaCySymDpOpData *pOpData,
                        CpaStatus status,
                        CpaBoolean verifyResult)
{
  //PRINT_DBG("Callback called with status = %d.\n", status);
  dout(20) << "fenghl Callback called with status = " << status << dendl;

  //if (CPA_FALSE == verifyResult)
  //{
  //  PRINT_ERR("Callback verify result error\n");
 // }

  if (NULL != pOpData->pCallbackTag)
  {
    /** indicate that the function has been called */
    COMPLETE((struct COMPLETION_STRUCT *)pOpData->pCallbackTag);
  }
}

/**
 *******************************************************************************
 * @ingroup sampleUtils
 *      This function returns the physical address for a given virtual address.
 *      In case of error 0 is returned.
 *
 * @param[in] virtAddr     Virtual address
 *
 * @retval CpaPhysicalAddr Physical address or 0 in case of error
 *
 ******************************************************************************/

static __inline CpaPhysicalAddr sampleVirtToPhys(void *virtAddr)
{
  return (CpaPhysicalAddr)qaeVirtToPhysNUMA(virtAddr);
}

static const size_t QCC_MAX_RETRIES = 50000;

/*
 * Poller thread & functions
*/
static std::mutex qcc_alloc_mutex;
static std::mutex qcc_eng_mutex;
static std::condition_variable alloc_cv;
static std::atomic<bool> init_called = { false };

void QccCrypto::QccFreeInstance(int entry) {
  std::lock_guard<std::mutex> freeinst(qcc_alloc_mutex);
  open_instances.push(entry);
  alloc_cv.notify_one();
}

int QccCrypto::QccGetFreeInstance() {
  int ret = -1;
  std::unique_lock getinst(qcc_alloc_mutex);
  alloc_cv.wait(getinst, [this](){return !open_instances.empty();});
  if (!open_instances.empty()) {
    ret = open_instances.front();
    open_instances.pop();
  }
  return ret;
}

void QccCrypto::cleanup() {
  icp_sal_userStop();
  qaeMemDestroy();
  is_init = false;
  init_stat = stat;
  init_called = false;
  derr << "Failure during QAT init sequence. Quitting" << dendl;
}

void QccCrypto::poll_instances(void) {
  while (!thread_stop) {
    for (int iter = 0; iter < qcc_inst->num_instances; iter++) {
      if (qcc_inst->is_polled[iter] == CPA_TRUE) {
        //&& qcc_op_mem[iter].op_complete
        stat = icp_sal_CyPollDpInstance(qcc_inst->cy_inst_handles[iter], 0);
        // if (stat != CPA_STATUS_SUCCESS) {
        //   derr << "poll instance " << iter << " failed" << dendl;
        // }
      }
    }
  }
}

/*
 * We initialize QAT instance and everything that is common for all ops
*/
bool QccCrypto::init() {

  std::lock_guard<std::mutex> l(qcc_eng_mutex);

  if(init_called) {
    dout(10) << "Init sequence already called. Skipping duplicate call" << dendl;
    return true;
  }

  // First call to init
  dout(15) << "First init for QAT" << dendl;
  init_called = true;

  // Find if the usermode memory driver is available. We need to this to
  // create contiguous memory needed by QAT.
  stat = qaeMemInit();
  if(stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to load memory driver" << dendl;
    this->cleanup();
    return false;
  }

  stat = icp_sal_userStart("CEPH");
  if(stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to start qat device" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_inst, sizeof(QCCINST));
  if(qcc_inst == NULL) {
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
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    stat = cpaCyStartInstance(qcc_inst->cy_inst_handles[iter]);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to start instance" << dendl;
      this->cleanup();
      return false;
    }
  }

  qcc_os_mem_alloc((void **)&qcc_inst->is_polled,
      ((int)qcc_inst->num_instances * sizeof(CpaBoolean)));
  CpaInstanceInfo2 info;
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    qcc_inst->is_polled[iter] = cpaCyInstanceGetInfo2(qcc_inst->cy_inst_handles[iter],
        &info) == CPA_STATUS_SUCCESS ? info.isPolled : CPA_FALSE;
  }

  // Allocate memory structures for all instances
  qcc_os_mem_alloc((void **)&qcc_sess,
      ((int)qcc_inst->num_instances * sizeof(QCCSESS)));
  if(qcc_sess == NULL) {
    derr << "Unable to allocate memory for session struct" << dendl;
    this->cleanup();
    return false;
  }

  qcc_os_mem_alloc((void **)&qcc_op_mem,
      ((int)qcc_inst->num_instances * sizeof(QCCOPMEM)));
  if(qcc_sess == NULL) {
    derr << "Unable to allocate memory for opmem struct" << dendl;
    this->cleanup();
    return false;
  }

  //At this point we are only doing an user-space version.
  //To-Do: Maybe a kernel based one
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    stat = cpaCySetAddressTranslation(qcc_inst->cy_inst_handles[iter],
                                       qaeVirtToPhysNUMA);
    if(stat == CPA_STATUS_SUCCESS) {
      open_instances.push(iter);
      qcc_op_mem[iter].is_mem_alloc = false;

      stat = cpaCySymDpRegCbFunc(qcc_inst->cy_inst_handles[iter], symDpCallback);
    } else {
      derr << "Unable to find address translations of instance " << iter << dendl;
      this->cleanup();
      return false;
    }
  }
  _reactor_thread = std::make_unique<std::thread>([this]{
    this->poll_instances();
    dout(10) << "End with handle events " << dendl;
  });
  is_init = true;
  dout(10) << "Init complete" << dendl;
  return true;
}

bool QccCrypto::destroy() {
  if((!is_init) || (!init_called)) {
    dout(15) << "QAT not initialized here. Nothing to do" << dendl;
    return false;
  }

  unsigned int retry = 0;
  while(retry <= QCC_MAX_RETRIES) {
    if(open_instances.size() == qcc_inst->num_instances) {
      break;
    } else {
      retry++;
    }
    dout(5) << "QAT is still busy and cannot free resources yet" << dendl;
    return false;
  }

  thread_stop = true;
  _reactor_thread->join();
  

  dout(10) << "Destroying QAT crypto & related memory" << dendl;
  int iter = 0;

  // Free up op related memory
  for (iter =0; iter < qcc_inst->num_instances; iter++) {
    qcc_contig_mem_free((void **)&(qcc_op_mem[iter].src_buff));
    qcc_contig_mem_free((void **)&(qcc_op_mem[iter].iv_buff));
    qcc_contig_mem_free((void **)&(qcc_op_mem[iter].sym_op_data));
  }

  // Free up Session memory
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    cpaCySymDpRemoveSession(qcc_inst->cy_inst_handles[iter], qcc_sess[iter].sess_ctx);
    qcc_contig_mem_free((void **)&(qcc_sess[iter].sess_ctx));
  }

  // Stop QAT Instances
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    cpaCyStopInstance(qcc_inst->cy_inst_handles[iter]);
  }

  // Free up the base structures we use
  qcc_os_mem_free((void **)&qcc_op_mem);//1
  qcc_os_mem_free((void **)&qcc_sess);//1
  qcc_os_mem_free((void **)&(qcc_inst->cy_inst_handles));//1
  qcc_os_mem_free((void **)&(qcc_inst->is_polled));//1
  qcc_os_mem_free((void **)&qcc_inst);//1

  //Un-init memory driver and QAT HW
  icp_sal_userStop();
  qaeMemDestroy();
  init_called = false;
  is_init = false;
  return true;
}

bool QccCrypto::perform_op(unsigned char* out, const unsigned char* in,
    size_t size, uint8_t *iv, uint8_t *key, CpaCySymCipherDirection op_type)
{
  if (!init_called) {
    dout(10) << "QAT not intialized yet. Initializing now..." << dendl;
    if(!QccCrypto::init()) {
      derr << "QAT init failed" << dendl;
      return false;
    }
  }

  if(!is_init)
  {
    dout(10) << "QAT not initialized in this instance or init failed with possible error " << (int)init_stat << dendl;
    return is_init;
  }

  int avail_inst = -1;
  avail_inst = QccGetFreeInstance();

  dout(15) << "Using inst " << avail_inst << dendl;


  auto sg = make_scope_guard([=] {
      //free up the instance irrespective of the op status
      dout(15) << "Completed task under " << avail_inst << dendl;
      qcc_op_mem[avail_inst].op_complete = false;
      QccCrypto::QccFreeInstance(avail_inst);
      });

  /*
   * Allocate buffers for this version of the instance if not already done.
   * Hold onto to most of them until destructor is called.
  */
  if (qcc_op_mem[avail_inst].is_mem_alloc == false) {
    // Allocate IV memory
    stat = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].iv_buff), AES_256_IV_LEN, 8);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate bufferlist memory" << dendl;
      return false;
    }

    // Allocate src memory
    stat = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].src_buff), size, 8);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate bufferlist memory" << dendl;
      return false;
    }

    //Setup OpData
    stat = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].sym_op_data),
        sizeof(CpaCySymDpOpData), 8);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate opdata memory" << dendl;
      return false;
    }


    // // Set memalloc flag so that we don't go through this exercise again.
    qcc_op_mem[avail_inst].is_mem_alloc = true;
    // dout(15) << "Instantiation complete for " << avail_inst << dendl;

    stat = initSession(qcc_inst->cy_inst_handles[avail_inst],
                             &(qcc_sess[avail_inst].sess_ctx),
                             (Cpa8U *)key,
                             op_type);
  } else {
    stat = updateSession(qcc_sess[avail_inst].sess_ctx,
                               (Cpa8U *)key,
                               op_type);
  }

  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to init session" << dendl;
    return false;
  }

  // stat = symPerformOp(qcc_inst->cy_inst_handles[avail_inst],
  stat = symPerformOp(avail_inst,
                      qcc_sess[avail_inst].sess_ctx,
                      in,
                      out,
                      size,
                      iv,
                      AES_256_IV_LEN);

  return true;
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
  if (status != CPA_STATUS_SUCCESS) {
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
  CpaCySymSessionSetupData sessionSetupData;// = {0};
  memset(&sessionSetupData, 0, sizeof(sessionSetupData));

  sessionSetupData.sessionPriority = CPA_CY_PRIORITY_NORMAL;
  sessionSetupData.symOperation = CPA_CY_SYM_OP_CIPHER;
  sessionSetupData.cipherSetupData.cipherAlgorithm = CPA_CY_SYM_CIPHER_AES_CBC;
  sessionSetupData.cipherSetupData.cipherKeyLenInBytes = AES_256_KEY_SIZE;
  sessionSetupData.cipherSetupData.pCipherKey = pCipherKey;
  sessionSetupData.cipherSetupData.cipherDirection = cipherDirection;

  status = cpaCySymDpSessionCtxGetSize(cyInstHandle, &sessionSetupData, &sessionCtxSize);
  if (CPA_STATUS_SUCCESS == status) {
    status = qcc_contig_mem_alloc((void **)(sessionCtx), sessionCtxSize);   //<<<<<<<<<<<<
  }
  if (CPA_STATUS_SUCCESS == status) {
    status = cpaCySymDpInitSession(cyInstHandle,
                                 &sessionSetupData,
                                 *sessionCtx);
  }
  return status;
}

// CpaStatus QccCrypto::symPerformOp(CpaInstanceHandle cyInstHandle,
CpaStatus QccCrypto::symPerformOp(int avail_inst,
                              CpaCySymSessionCtx sessionCtx,
                              const Cpa8U *pSrc,
                              Cpa8U *pDst,
                              Cpa32U len,
                              const Cpa8U *pIv,
                              Cpa32U ivLen) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  CpaCySymDpOpData *pOpData = qcc_op_mem[avail_inst].sym_op_data;
  Cpa8U *pSrcBuffer = qcc_op_mem[avail_inst].src_buff;
  Cpa8U *pIvBuffer = qcc_op_mem[avail_inst].iv_buff;

  // if (CPA_STATUS_SUCCESS == status) {
  //   status = PHYS_CONTIG_ALLOC(&pSrcBuffer, len);
  // }
  // if (CPA_STATUS_SUCCESS == status) {
  //   status = PHYS_CONTIG_ALLOC(&pIvBuffer, ivLen);
  // }
  // if (CPA_STATUS_SUCCESS == status) {
  //   status = PHYS_CONTIG_ALLOC_ALIGNED(&pOpData, sizeof(CpaCySymDpOpData), 8);
  // }
  if (CPA_STATUS_SUCCESS == status) {
    // copy source into buffer
    memcpy(pSrcBuffer, pSrc, len);
    // copy IV into buffer
    memcpy(pIvBuffer, pIv, ivLen);
  }

  struct COMPLETION_STRUCT complete;
  COMPLETION_INIT(&complete);
  if (CPA_STATUS_SUCCESS == status) {

    //pOpData assignment
    pOpData->thisPhys = sampleVirtToPhys(pOpData);
    pOpData->instanceHandle = qcc_inst->cy_inst_handles[avail_inst];
    pOpData->sessionCtx = sessionCtx;
    pOpData->pCallbackTag = (void *)&complete;
    pOpData->cryptoStartSrcOffsetInBytes = 0;
    pOpData->messageLenToCipherInBytes = len;
    pOpData->iv = sampleVirtToPhys(pIvBuffer);
    pOpData->pIv = pIvBuffer;
    pOpData->ivLenInBytes = ivLen;
    pOpData->srcBuffer = sampleVirtToPhys(pSrcBuffer);
    pOpData->srcBufferLen = len;
    pOpData->dstBuffer = sampleVirtToPhys(pSrcBuffer);
    pOpData->dstBufferLen = len;
  }

  if (CPA_STATUS_SUCCESS == status) {
    status = cpaCySymDpEnqueueOp(pOpData, CPA_TRUE);
    // status = cpaCySymDpEnqueueOp(pOpData, CPA_FALSE);
    // if (CPA_STATUS_SUCCESS != status) {
    //   dout(1) << "cpaCySymDpEnqueueOp failed. (status = " << status << ")" << dendl;
    // } else {
    //   status = cpaCySymDpPerformOpNow(cyInstHandle);
    //   if (CPA_STATUS_SUCCESS != status) {
    //     dout(1) << "cpaCySymDpPerformOpNow failed. (status = " << status << ")" << dendl;
    //   } else {
    //     dout(1) << "cpaCySymDpPerformOpNow succeed " << dendl;
    //   }
    // }
  }

  if (CPA_STATUS_SUCCESS == status) {
    if (!COMPLETION_WAIT(&complete, TIMEOUT_MS)) {
      dout(1) << "timeout or interruption in cpaCySymPerformOp" << dendl;
    }
    // dout(1) << "callback OK" << dendl;
  }
  //Copy data back to pDst buffer
  memcpy(pDst, pSrcBuffer, len);

  COMPLETION_DESTROY(&complete);

  // PHYS_CONTIG_FREE(pSrcBuffer);
  // PHYS_CONTIG_FREE(pIvBuffer);
  // PHYS_CONTIG_FREE(pOpData);

  return status;
}