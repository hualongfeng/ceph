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
static void symCallback(void *pCallbackTag,
                        CpaStatus status,
                        const CpaCySymOp operationType,
                        void *pOpData,
                        CpaBufferList *pDstBuffer,
                        CpaBoolean verifyResult)
{
  //PRINT_DBG("Callback called with status = %d.\n", status);
  derr << "fenghl Callback called with status = " << status << dendl;

  //if (CPA_FALSE == verifyResult)
  //{
  //  PRINT_ERR("Callback verify result error\n");
 // }

  if (NULL != pCallbackTag)
  {
    /** indicate that the function has been called */
    COMPLETE((struct COMPLETION_STRUCT *)pCallbackTag);
  }
}


/*
 * Poller thread & functions
*/
static std::mutex qcc_alloc_mutex;
static std::mutex qcc_eng_mutex;
static std::condition_variable alloc_cv;
static std::atomic<bool> init_called = { false };

void* QccCrypto::crypt_thread(void *args) {
  struct qcc_thread_args *thread_args = (struct qcc_thread_args *)args;
  thread_args->qccinstance->do_crypt(thread_args);
  return thread_args;
}

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
      if (qcc_inst->is_polled[iter] == CPA_TRUE && qcc_op_mem[iter].op_complete) {
        stat = icp_sal_CyPollInstance(qcc_inst->cy_inst_handles[iter], 0);
        if (stat != CPA_STATUS_SUCCESS) {
          derr << "poll instance " << iter << " failed" << dendl;
        }
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

  qcc_os_mem_alloc((void **)&cypollthreads,
      ((int)qcc_inst->num_instances * sizeof(pthread_t)));
  if(cypollthreads == NULL) {
    derr << "Unable to allocate memory for pthreads" << dendl;
    this->cleanup();
    return false;
  }

  //At this point we are only doing an user-space version.
  //To-Do: Maybe a kernel based one
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    stat = cpaCySetAddressTranslation(qcc_inst->cy_inst_handles[iter],
                                       qaeVirtToPhysNUMA);
    if(stat == CPA_STATUS_SUCCESS) {
      // Start HW Polling Thread
      // To-Do: Enable epoll & interrupt based later?
      // QccCyStartPoll(iter);
      // Setup the session structures for crypto operation and populate
      // whatever we can now. Rest will be filled in when crypto operation
      // happens.
      qcc_sess[iter].sess_ctx_sz = 0;
      qcc_sess[iter].sess_ctx = NULL;
      qcc_sess[iter].sess_stp_data.sessionPriority = CPA_CY_PRIORITY_NORMAL;
      qcc_sess[iter].sess_stp_data.symOperation = CPA_CY_SYM_OP_CIPHER;
      open_instances.push(iter);
      qcc_op_mem[iter].is_mem_alloc = false;
      qcc_op_mem[iter].op_complete = false;
      qcc_op_mem[iter].op_result = CPA_STATUS_SUCCESS;
      qcc_op_mem[iter].sym_op_data = NULL;
      qcc_op_mem[iter].buff_meta_size = qcc_op_mem[iter].buff_size = 0;
      qcc_op_mem[iter].src_buff_meta = qcc_op_mem[iter].src_buff
        = qcc_op_mem[iter].iv_buff = NULL;
      qcc_op_mem[iter].src_buff_list = NULL;
      qcc_op_mem[iter].src_buff_flat = NULL;
      qcc_op_mem[iter].num_buffers = 1;
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
    qcc_os_mem_free((void **)&(qcc_op_mem[iter].src_buff_list));
    qcc_os_mem_free((void **)&(qcc_op_mem[iter].src_buff_flat));
    qcc_contig_mem_free((void **)&(qcc_op_mem[iter].sym_op_data));
  }

  // Free up Session memory
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    cpaCySymRemoveSession(qcc_inst->cy_inst_handles[iter], qcc_sess[iter].sess_ctx);
    qcc_contig_mem_free((void **)&(qcc_sess[iter].sess_ctx));
  }

  // Stop QAT Instances
  for(iter = 0; iter < qcc_inst->num_instances; iter++) {
    cpaCyStopInstance(qcc_inst->cy_inst_handles[iter]);
  }

  // Free up the base structures we use
  qcc_os_mem_free((void **)&qcc_op_mem);
  qcc_os_mem_free((void **)&qcc_sess);
  qcc_os_mem_free((void **)&(qcc_inst->cy_inst_handles));
  qcc_os_mem_free((void **)&(qcc_inst->is_polled));
  qcc_os_mem_free((void **)&cypollthreads);
  qcc_os_mem_free((void **)&qcc_inst);

  //Un-init memory driver and QAT HW
  icp_sal_userStop();
  qaeMemDestroy();
  init_called = false;
  is_init = false;
  return true;
}

void QccCrypto::do_crypt(qcc_thread_args *thread_args) {
  auto entry = thread_args->entry;
  qcc_op_mem[entry].op_result = cpaCySymPerformOp(qcc_inst->cy_inst_handles[entry],
      NULL,
      qcc_op_mem[entry].sym_op_data,
      qcc_op_mem[entry].src_buff_list,
      qcc_op_mem[entry].src_buff_list,
      NULL);
  qcc_op_mem[entry].op_complete = true;
  free(thread_args);
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
  unsigned int retrycount = 0;
  while(retrycount <= QCC_MAX_RETRIES) {
    avail_inst = QccGetFreeInstance();
    if(avail_inst != -1) {
      break;
    } else {
      retrycount++;
      usleep(qcc_sleep_duration);
    }
  }

  if(avail_inst == -1) {
    derr << "Unable to get an QAT instance. Failing request" << dendl;
    return false;
  }

  dout(15) << "Using inst " << avail_inst << dendl;
  // Start polling threads for this instance
  //QccCyStartPoll(avail_inst);

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

    qcc_sess[avail_inst].sess_stp_data.cipherSetupData.cipherAlgorithm =
                                                     CPA_CY_SYM_CIPHER_AES_CBC;
    qcc_sess[avail_inst].sess_stp_data.cipherSetupData.cipherKeyLenInBytes =
                                                              AES_256_KEY_SIZE;

    // Allocate contig memory for buffers that are independent of the
    // input/output
    stat = cpaCyBufferListGetMetaSize(qcc_inst->cy_inst_handles[avail_inst],
        qcc_op_mem[avail_inst].num_buffers, &(qcc_op_mem[avail_inst].buff_meta_size));
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to get buff meta size" << dendl;
      return false;
    }

    // Allocate Buffer List Private metadata
    stat = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].src_buff_meta),
        qcc_op_mem[avail_inst].buff_meta_size, 1);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate private metadata memory" << dendl;
      return false;
    }

    // Allocate Buffer List Memory
    qcc_os_mem_alloc((void **)&(qcc_op_mem[avail_inst].src_buff_list), sizeof(CpaBufferList));
    qcc_os_mem_alloc((void **)&(qcc_op_mem[avail_inst].src_buff_flat),
              (qcc_op_mem[avail_inst].num_buffers * sizeof(CpaFlatBuffer)));
    if(qcc_op_mem[avail_inst].src_buff_list == NULL || qcc_op_mem[avail_inst].src_buff_flat == NULL) {
      derr << "Unable to allocate bufferlist memory" << dendl;
      return false;
    }

    // Allocate IV memory
    stat = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].iv_buff), AES_256_IV_LEN);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate bufferlist memory" << dendl;
      return false;
    }

    //Assign src stuff for the operation
    (qcc_op_mem[avail_inst].src_buff_list)->pBuffers = qcc_op_mem[avail_inst].src_buff_flat;
    (qcc_op_mem[avail_inst].src_buff_list)->numBuffers = qcc_op_mem[avail_inst].num_buffers;
    (qcc_op_mem[avail_inst].src_buff_list)->pPrivateMetaData = qcc_op_mem[avail_inst].src_buff_meta;

    //Setup OpData
    stat = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].sym_op_data),
        sizeof(CpaCySymOpData));
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate opdata memory" << dendl;
      return false;
    }

    // Assuming op to be encryption for initiation. This will be reset when we
    // exit this block
    qcc_sess[avail_inst].sess_stp_data.cipherSetupData.cipherDirection =
                                            CPA_CY_SYM_CIPHER_DIRECTION_ENCRYPT;
    // Allocate Session memory
    stat = cpaCySymSessionCtxGetSize(qcc_inst->cy_inst_handles[avail_inst],
        &(qcc_sess[avail_inst].sess_stp_data), &(qcc_sess[avail_inst].sess_ctx_sz));
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to find session size" << dendl;
      return false;
    }

    stat = qcc_contig_mem_alloc((void **)&(qcc_sess[avail_inst].sess_ctx),
        qcc_sess[avail_inst].sess_ctx_sz);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate contig memory" << dendl;
      return false;
    }

    // Set memalloc flag so that we don't go through this exercise again.
    qcc_op_mem[avail_inst].is_mem_alloc = true;
    dout(15) << "Instantiation complete for " << avail_inst << dendl;
  }

  // Section that runs on every call
  // Identify the operation and assign to session
  qcc_sess[avail_inst].sess_stp_data.cipherSetupData.cipherDirection = op_type;
  qcc_sess[avail_inst].sess_stp_data.cipherSetupData.pCipherKey = (Cpa8U *)key;

  stat = cpaCySymInitSession(qcc_inst->cy_inst_handles[avail_inst],
      symCallback,
      &(qcc_sess[avail_inst].sess_stp_data),
      qcc_sess[avail_inst].sess_ctx);
  if (stat != CPA_STATUS_SUCCESS) {
    derr << "Unable to init session" << dendl;
    return false;
  }

  // Allocate actual buffers that will hold data
  if (qcc_op_mem[avail_inst].buff_size != (Cpa32U)size) {
    qcc_contig_mem_free((void **)&(qcc_op_mem[avail_inst].src_buff));
    qcc_op_mem[avail_inst].buff_size = (Cpa32U)size;
    stat = qcc_contig_mem_alloc((void **)&(qcc_op_mem[avail_inst].src_buff),
              qcc_op_mem[avail_inst].buff_size);
    if(stat != CPA_STATUS_SUCCESS) {
      derr << "Unable to allocate contig memory" << dendl;
      return false;
    }
  }

  // Copy src & iv into the respective buffers
  memcpy(qcc_op_mem[avail_inst].src_buff, in, size);
  memcpy(qcc_op_mem[avail_inst].iv_buff, iv, AES_256_IV_LEN);

  //Assign the reminder of the stuff
  qcc_op_mem[avail_inst].src_buff_flat->dataLenInBytes = qcc_op_mem[avail_inst].buff_size;
  qcc_op_mem[avail_inst].src_buff_flat->pData = qcc_op_mem[avail_inst].src_buff;

  //OpData assignment
  qcc_op_mem[avail_inst].sym_op_data->sessionCtx = qcc_sess[avail_inst].sess_ctx;
  qcc_op_mem[avail_inst].sym_op_data->packetType = CPA_CY_SYM_PACKET_TYPE_FULL;
  qcc_op_mem[avail_inst].sym_op_data->pIv = qcc_op_mem[avail_inst].iv_buff;
  qcc_op_mem[avail_inst].sym_op_data->ivLenInBytes = AES_256_IV_LEN;
  qcc_op_mem[avail_inst].sym_op_data->cryptoStartSrcOffsetInBytes = 0;
  qcc_op_mem[avail_inst].sym_op_data->messageLenToCipherInBytes = qcc_op_mem[avail_inst].buff_size;

  struct COMPLETION_STRUCT complete;
  auto entry = avail_inst;
  COMPLETION_INIT(&complete);
  qcc_op_mem[entry].op_result = cpaCySymPerformOp(qcc_inst->cy_inst_handles[entry],
      (void *)&complete,
      qcc_op_mem[entry].sym_op_data,
      qcc_op_mem[entry].src_buff_list,
      qcc_op_mem[entry].src_buff_list,
      NULL);
  qcc_op_mem[entry].op_complete = true;

  if(qcc_op_mem[avail_inst].op_result != CPA_STATUS_SUCCESS) {
    derr << "Unable to perform crypt operation" << dendl;
    return false;
  }


//  derr << "fenghl starting to wait" << dendl;
  if(qcc_op_mem[avail_inst].op_result == CPA_STATUS_SUCCESS) {
    if (!COMPLETION_WAIT(&complete, TIMEOUT_MS)) {
      derr << "timeout or interruption in cpaCySymPerformOp" << dendl;
      //return false;
    }
  }
//  derr << "fenghl ending to wait" << dendl;

  //Copy data back to out buffer
  memcpy(out, qcc_op_mem[avail_inst].src_buff, size);
  //Always cleanup memory holding user-data at the end
  memset(qcc_op_mem[avail_inst].iv_buff, 0, AES_256_IV_LEN);
  memset(qcc_op_mem[avail_inst].src_buff, 0, qcc_op_mem[avail_inst].buff_size);

  COMPLETION_DESTROY(&complete);

  return true;
}
