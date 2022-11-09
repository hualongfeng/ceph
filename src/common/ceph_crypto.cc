// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2010-2011 Dreamhost
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <vector>

#include "common/ceph_context.h"
#include "common/ceph_mutex.h"
#include "common/config.h"
#include "common/Thread.h"
#include "ceph_crypto.h"

#include <openssl/evp.h>

#if OPENSSL_VERSION_NUMBER < 0x10100000L
#  include <openssl/conf.h>
#  include <openssl/engine.h>
#  include <openssl/err.h>
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

#include <iostream>
#include <semaphore.h>
#include <string.h>
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>

extern "C" {
#include "cpa.h"
#include "lac/cpa_cy_sym.h"
#include "lac/cpa_cy_im.h"
#include "qae_mem.h"
#include "icp_sal_user.h"
#include "icp_sal_poll.h"
#include "qae_mem_utils.h"
}

#define BUFFER_SIZE 131072

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

static __inline CpaStatus msSleep(Cpa32U ms)
{
  int ret = 0;
  struct timespec resTime, remTime;
  resTime.tv_sec = ms / 1000;
  resTime.tv_nsec = (ms % 1000) * 1000000;
  do {
    ret = nanosleep(&resTime, &remTime);
    resTime = remTime;
  } while ((ret != 0) && (errno == EINTR));

  if (ret != 0) {
    std::cout << "nanoSleep failed with code "<< ret << std::endl;
    return CPA_STATUS_FAIL;
  } else {
    return CPA_STATUS_SUCCESS;
  }
}

namespace TOPNSPC::crypto::ssl {

#if OPENSSL_VERSION_NUMBER < 0x10100000L
static std::atomic_uint32_t crypto_refs;


static auto ssl_mutexes = ceph::make_lock_container<ceph::shared_mutex>(
  static_cast<size_t>(std::max(CRYPTO_num_locks(), 0)),
  [](const size_t i) {
    return ceph::make_shared_mutex(
      std::string("ssl-mutex-") + std::to_string(i));
  });

static struct {
  // we could use e.g. unordered_set instead at the price of providing
  // std::hash<...> specialization. However, we can live with duplicates
  // quite well while the benefit is not worth the effort.
  std::vector<CRYPTO_THREADID> tids;
  ceph::mutex lock = ceph::make_mutex("crypto::ssl::init_records::lock");;
} init_records;

static void
ssl_locking_callback(
  const int mode,
  const int mutex_num,
  [[maybe_unused]] const char *file,
  [[maybe_unused]] const int line)
{
  if (mutex_num < 0 || static_cast<size_t>(mutex_num) >= ssl_mutexes.size()) {
    ceph_assert_always("openssl passed wrong mutex index" == nullptr);
  }

  if (mode & CRYPTO_READ) {
    if (mode & CRYPTO_LOCK) {
      ssl_mutexes[mutex_num].lock_shared();
    } else if (mode & CRYPTO_UNLOCK) {
      ssl_mutexes[mutex_num].unlock_shared();
    }
  } else if (mode & CRYPTO_WRITE) {
    if (mode & CRYPTO_LOCK) {
      ssl_mutexes[mutex_num].lock();
    } else if (mode & CRYPTO_UNLOCK) {
      ssl_mutexes[mutex_num].unlock();
    }
  }
}

static unsigned long
ssl_get_thread_id(void)
{
  static_assert(sizeof(unsigned long) >= sizeof(pthread_t));
  /* pthread_t may be any data type, so a simple cast to unsigned long
   * can rise a warning/error, depending on the platform.
   * Here memcpy is used as an anything-to-anything cast. */
  unsigned long ret = 0;
  pthread_t t = pthread_self();
  memcpy(&ret, &t, sizeof(pthread_t));
  return ret;
}
#endif /* not OPENSSL_VERSION_NUMBER < 0x10100000L */

static void init() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  if (++crypto_refs == 1) {
    // according to
    // https://wiki.openssl.org/index.php/Library_Initialization#libcrypto_Initialization
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();

    // initialize locking callbacks, needed for thread safety.
    // http://www.openssl.org/support/faq.html#PROG1
    CRYPTO_set_locking_callback(&ssl_locking_callback);
    CRYPTO_set_id_callback(&ssl_get_thread_id);

    OPENSSL_config(nullptr);
  }

  // we need to record IDs of all threads calling the initialization in
  // order to *manually* free per-thread memory OpenSSL *automagically*
  // allocated in ERR_get_state().
  // XXX: this solution/nasty hack is IMPERFECT. A leak will appear when
  // a client init()ializes the crypto subsystem with one thread and then
  // uses it from another one in a way that results in ERR_get_state().
  // XXX: for discussion about another approaches please refer to:
  // https://www.mail-archive.com/openssl-users@openssl.org/msg59070.html
  {
    std::lock_guard l(init_records.lock);
    CRYPTO_THREADID tmp;
    CRYPTO_THREADID_current(&tmp);
    init_records.tids.emplace_back(std::move(tmp));
  }
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
}

static void shutdown() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  if (--crypto_refs != 0) {
    return;
  }

  // drop error queue for each thread that called the init() function to
  // satisfy valgrind.
  {
    std::lock_guard l(init_records.lock);

    // NOTE: in OpenSSL 1.0.2g the signature is:
    //    void ERR_remove_thread_state(const CRYPTO_THREADID *tid);
    // but in 1.1.0j it has been changed to
    //    void ERR_remove_thread_state(void *);
    // We're basing on the OPENSSL_VERSION_NUMBER check to preserve
    // const-correctness without failing builds on modern envs.
    for (const auto& tid : init_records.tids) {
      ERR_remove_thread_state(&tid);
    }
  }

  // Shutdown according to
  // https://wiki.openssl.org/index.php/Library_Initialization#Cleanup
  // http://stackoverflow.com/questions/29845527/how-to-properly-uninitialize-openssl
  //
  // The call to CONF_modules_free() has been introduced after a valgring run.
  CRYPTO_set_locking_callback(nullptr);
  CRYPTO_set_id_callback(nullptr);
  ENGINE_cleanup();
  CONF_modules_free();
  CONF_modules_unload(1);
  ERR_free_strings();
  EVP_cleanup();
  CRYPTO_cleanup_all_ex_data();

  // NOTE: don't clear ssl_mutexes as we should be ready for init-deinit-init
  // sequence.
#endif /* OPENSSL_VERSION_NUMBER < 0x10100000L */
}

void zeroize_for_security(void* const s, const size_t n) {
  OPENSSL_cleanse(s, n);
}

} // namespace TOPNSPC::crypto::openssl


namespace TOPNSPC::crypto {
void init() {
  ssl::init();
}

void shutdown([[maybe_unused]] const bool shared) {
  ssl::shutdown();
}

void zeroize_for_security(void* const s, const size_t n) {
  ssl::zeroize_for_security(s, n);
}

ssl::OpenSSLDigest::OpenSSLDigest(const EVP_MD * _type)
  : mpContext(EVP_MD_CTX_create())
  , mpType(_type) {
  this->Restart();
}

ssl::OpenSSLDigest::~OpenSSLDigest() {
  EVP_MD_CTX_destroy(mpContext);
}

void ssl::OpenSSLDigest::Restart() {
  EVP_DigestInit_ex(mpContext, mpType, NULL);
}

void ssl::OpenSSLDigest::SetFlags(int flags) {
  EVP_MD_CTX_set_flags(mpContext, flags);
  this->Restart();
}

void ssl::OpenSSLDigest::Update(const unsigned char *input, size_t length) {
  if (length) {
    EVP_DigestUpdate(mpContext, const_cast<void *>(reinterpret_cast<const void *>(input)), length);
  }
}

void ssl::OpenSSLDigest::Final(unsigned char *digest) {
  unsigned int s;
  EVP_DigestFinal_ex(mpContext, digest, &s);
}

}

namespace TOPNSPC::crypto::qat {
class QatInstancesManager {
  std::vector<CpaInstanceHandle> cyInstances;
  std::mutex lock;
  std::atomic<size_t> index{0};
  std::thread poll_thread;
  volatile bool gPollingCy{false};
  void init();
  bool inited{false};
 public:
  void poll_instance();
  QatInstancesManager();
  ~QatInstancesManager();
  CpaInstanceHandle getInstance();
};

QatInstancesManager::QatInstancesManager() : index(0) {}


void QatInstancesManager::init() {
  CpaStatus stat = CPA_STATUS_SUCCESS;
  Cpa16U numInstances = 0;
//  std::cout << "QatInstancesManager constructor" << std::endl;

  stat = qaeMemInit();
  if (CPA_STATUS_SUCCESS != stat)
  {
    std::cout << "Failed to initialize memory driver" << std::endl;
    throw "Failed to initialize memory driver";
  }

  stat = icp_sal_userStartMultiProcess("SHIM", CPA_FALSE);
  if (CPA_STATUS_SUCCESS != stat)
  {
    std::cout << "Failed to start user process SHIM" << std::endl;
    qaeMemDestroy();
    throw "Failed to start user process SHIM";
  }

  stat = cpaCyGetNumInstances(&numInstances);
  if ((stat == CPA_STATUS_SUCCESS) && (numInstances > 0)) {
    cyInstances.resize(numInstances);
    stat = cpaCyGetInstances(numInstances, &cyInstances[0]);
  }

  if ((stat != CPA_STATUS_SUCCESS) || (numInstances == 0)) {
    icp_sal_userStop();
    qaeMemDestroy();

    std::cout << "No instances found for 'SHIM'" << std::endl;
    throw "No instances found for 'SHIM'";
  }

  //std::cout << "cpaCyStartInstance: " << numInstances << std::endl;
  for (auto &instance : cyInstances) {
    cpaCyStartInstance(instance);
    cpaCySetAddressTranslation(instance, qaeVirtToPhysNUMA);
    CpaInstanceInfo2 info2;
    stat = cpaCyInstanceGetInfo2(instance, &info2);
    if ((stat == CPA_STATUS_SUCCESS) && (info2.isPolled == CPA_TRUE)) {
      /*do nothing*/
    } else {
      icp_sal_userStop();
      qaeMemDestroy();

      std::cout << "One instance cannot poll, check the config" << std::endl;
      throw "One instance cannot poll, check the config";
    }
  }
  //std::cout << "start poll instance" << std::endl;
  //poll_thread = std::thread(&QatInstancesManager::poll_instance, this);
  poll_thread = make_named_thread("qat_hash_poll", &QatInstancesManager::poll_instance, this);
}

QatInstancesManager::~QatInstancesManager() {
//  std::cout << "QatInstancesManager destructor" << std::endl;
  gPollingCy = false;
  if (poll_thread.joinable()) {
    poll_thread.join();
  }
  for (auto &instance : cyInstances) {
    cpaCyStopInstance(instance);
  }
  icp_sal_userStop();
  qaeMemDestroy();
}

CpaInstanceHandle QatInstancesManager::getInstance() {
  std::lock_guard<std::mutex> l{lock};
  if (!inited) {
    init();
    inited = true;
    //poll_thread = std::thread(&QatInstancesManager::poll_instance, this);
  }
  CpaInstanceHandle instance = cyInstances[index++];
  if (index >= cyInstances.size()) index = 0;
  return instance;
}

void QatInstancesManager::poll_instance() {
  //std::cout << "start poll instance" << std::endl;
  gPollingCy = true;
  while (gPollingCy) {
    Cpa64U requestsCount = 0;
    int cnt = 0;
    for (auto &instance : cyInstances) {
      CpaCySymStats64 stat{0};
      cpaCySymQueryStats64(instance, &stat);
    //  std::cout << "numSymOpRequests = " << stat.numSymOpRequests
    //            << ", numSymOpCompleted = " << stat.numSymOpCompleted
    //            << ", numSessionsInitialized = " << stat.numSessionsInitialized
    //            << ", numSessionsRemoved = " << stat.numSessionsRemoved
    //	          << ", num = " << cnt++
    //            << std::endl;

      requestsCount += (stat.numSymOpRequests - stat.numSymOpCompleted);
      if (stat.numSymOpRequests != stat.numSymOpCompleted) {
        icp_sal_CyPollInstance(instance, 0);
      }
    }
    if (requestsCount == 0) {
      msSleep(1);
    }
  }
}

static inline void qcc_contig_mem_free(void **ptr) {
  if (*ptr) {
    qaeMemFreeNUMA(ptr);
    *ptr = nullptr;
  }
}

static inline CpaStatus qcc_contig_mem_alloc(void **ptr, Cpa32U size, Cpa32U alignment = 1) {
  *ptr = qaeMemAllocNUMA(size, 0, alignment);
  if (nullptr == *ptr) {
    return CPA_STATUS_RESOURCE;
  }
  return CPA_STATUS_SUCCESS;
}

static void symCallback(void *pCallbackTag,
                        CpaStatus status,
                        const CpaCySymOp operationType,
                        void *pOpData,
                        CpaBufferList *pDstBuffer,
                        CpaBoolean verifyResult)
{
  if (nullptr != pCallbackTag)
  {
    /** indicate that the function has been called*/
    COMPLETE((struct COMPLETION_STRUCT *)pCallbackTag);
  }
}


static Cpa32U digest_size(CpaCySymHashAlgorithm _type) {
  switch(_type) {
    case CPA_CY_SYM_HASH_MD5:
      return CEPH_CRYPTO_MD5_DIGESTSIZE;
    case CPA_CY_SYM_HASH_SHA1:
      return CEPH_CRYPTO_SHA1_DIGESTSIZE;
    case CPA_CY_SYM_HASH_SHA256:
      return CEPH_CRYPTO_SHA256_DIGESTSIZE;
    case CPA_CY_SYM_HASH_SHA512:
      return CEPH_CRYPTO_SHA512_DIGESTSIZE;
  }
  return -1;
}

static Cpa32U block_size(CpaCySymHashAlgorithm _type) {
  switch(_type) {
    case CPA_CY_SYM_HASH_MD5:
      return LAC_HASH_MD5_BLOCK_SIZE;
    case CPA_CY_SYM_HASH_SHA1:
      return LAC_HASH_SHA1_BLOCK_SIZE;
    case CPA_CY_SYM_HASH_SHA256:
      return LAC_HASH_SHA256_BLOCK_SIZE;
    case CPA_CY_SYM_HASH_SHA512:
      return LAC_HASH_SHA512_BLOCK_SIZE;
  }
  return -1;
}

static QatInstancesManager qat_instance_manager;

QatHashCommon::QatHashCommon(const CpaCySymHashAlgorithm _type)
	: cyInstHandle(qat_instance_manager.getInstance()), mpType(_type) {
  CpaStatus status = CPA_STATUS_SUCCESS;
  digest_length = digest_size(mpType);
  block_length = block_size(mpType);
  align_left = new unsigned char[LAC_HASH_MAX_BLOCK_SIZE];
  sessionSetupData = new CpaCySymSessionSetupData;
  memset(sessionSetupData, 0, sizeof(CpaCySymSessionSetupData));
  sessionSetupData->sessionPriority = CPA_CY_PRIORITY_NORMAL;
  sessionSetupData->symOperation = CPA_CY_SYM_OP_HASH;
  sessionSetupData->hashSetupData.hashAlgorithm = mpType;
  sessionSetupData->hashSetupData.hashMode = CPA_CY_SYM_HASH_MODE_PLAIN;
  sessionSetupData->hashSetupData.digestResultLenInBytes = digest_length;
  sessionSetupData->digestIsAppended = CPA_FALSE;
  sessionSetupData->verifyDigest = CPA_FALSE;
}


  QatHashCommon::QatHashCommon(QatHashCommon &&qat) {
    cyInstHandle     = qat.cyInstHandle;
    mpType           = qat.mpType;
    sessionCtx       = qat.sessionCtx;
    pBufferList      = qat.pBufferList;
    digest_length    = qat.digest_length;
    block_length     = qat.block_length;
    align_left       = qat.align_left;
    align_left_len   = qat.align_left_len;
    sessionSetupData = qat.sessionSetupData;
    partial          = qat.partial;

    qat.cyInstHandle = nullptr;
    qat.mpType = CPA_CY_SYM_HASH_NONE;
    qat.sessionCtx = nullptr;
    qat.pBufferList = nullptr;
    qat.digest_length = 0;
    qat.block_length = 0;
    qat.align_left = nullptr;
    qat.align_left_len = 0;
    qat.sessionSetupData = nullptr;
    qat.partial = false;
  }

struct CpaBufferListDeleter {
  void operator() (CpaBufferList* pBufferList) {
    void *pBufferMeta = nullptr;
    void *pSrcBuffer = nullptr;
    void *pDigestBuffer = nullptr;

    if (pBufferList != nullptr) {
      pBufferMeta   = pBufferList->pPrivateMetaData;
      pDigestBuffer = pBufferList->pUserData;
      pSrcBuffer    = pBufferList->pBuffers->pData;
      qcc_contig_mem_free(&pSrcBuffer);
      free(pBufferList);
      qcc_contig_mem_free(&pBufferMeta);
      qcc_contig_mem_free((void**)&pDigestBuffer);
    }
  }
};

using buffer_ptr = std::unique_ptr<CpaBufferList, CpaBufferListDeleter>;
static std::vector<buffer_ptr> buffers;
static std::mutex mutex;

QatHashCommon::~QatHashCommon() {
  if (pBufferList != nullptr) {
    std::scoped_lock lock{mutex};
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

  delete[] align_left;
  delete sessionSetupData;
}

void QatHashCommon::Restart() {
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
  Cpa32U bufferSize = BUFFER_SIZE;
  Cpa32U numBuffers = 1;
  Cpa32U bufferListMemSize = sizeof(CpaBufferList) + (numBuffers * sizeof(CpaFlatBuffer));
  Cpa8U *pSrcBuffer = nullptr;
  Cpa8U *pDigestBuffer = nullptr;

  {
    std::scoped_lock lock{mutex};
    if (!buffers.empty()) {
      pBufferList = buffers.back().release();
      buffers.pop_back();
      return pBufferList;
    }
  }

  status = cpaCyBufferListGetMetaSize(cyInstHandle, numBuffers, &bufferMetaSize);

  if (CPA_STATUS_SUCCESS == status)
  {
    status = qcc_contig_mem_alloc((void**)&pBufferMeta, bufferMetaSize);
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    pBufferList = (CpaBufferList *)malloc(bufferListMemSize);
    if (pBufferList == nullptr) return nullptr;
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    status = qcc_contig_mem_alloc((void**)&pSrcBuffer, bufferSize);
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    status = qcc_contig_mem_alloc((void**)&pDigestBuffer, CEPH_CRYPTO_MAX_DIGESTSIZE);
  }

  if (CPA_STATUS_SUCCESS == status)
  {
    /* increment by sizeof(CpaBufferList) to get at the
     * array of flatbuffers */
    pFlatBuffer = (CpaFlatBuffer *)(pBufferList + 1);

    pBufferList->numBuffers = 1;
    pBufferList->pBuffers = pFlatBuffer;
    pBufferList->pUserData = pDigestBuffer;
    pBufferList->pPrivateMetaData = pBufferMeta;

    pFlatBuffer->dataLenInBytes = bufferSize;
    pFlatBuffer->pData = pSrcBuffer;

  }

  return pBufferList;
}

void QatHashCommon::Update(const unsigned char *input, size_t length) {
//  std::cout << "Update input length: " << length
//            << "align_left_len: " << align_left_len << std::endl;
  CpaStatus status = CPA_STATUS_SUCCESS;
  CpaCySymOpData pOpData = {0};

  size_t left_length = length;
  size_t current_length = 0;
  size_t new_write_length = 0;
  size_t current_align_left_len = 0;

  if (pBufferList == nullptr) {
    pBufferList = getCpaBufferList(cyInstHandle);

    if (pBufferList == nullptr) {
      std::cout << "cannot get CpaBufferList" << std::endl;
      return;
    }
  }

  struct COMPLETION_STRUCT complete;

  COMPLETION_INIT((&complete));

  do {
    new_write_length = left_length > (BUFFER_SIZE - align_left_len) ? \
	    (BUFFER_SIZE - align_left_len) : left_length;
    current_length = new_write_length + align_left_len;
    left_length -= new_write_length;

    if (current_length % block_length == 0) {
      memcpy(pBufferList->pBuffers->pData, align_left, align_left_len);
      memcpy(pBufferList->pBuffers->pData + align_left_len, input, new_write_length);
      align_left_len = 0;
    } else if (current_length / block_length > 0){
      memcpy(pBufferList->pBuffers->pData, align_left, align_left_len);
      current_align_left_len = current_length % block_length;
      memcpy(pBufferList->pBuffers->pData + align_left_len, input, new_write_length - current_align_left_len);
      memcpy(align_left, input + new_write_length - current_align_left_len, current_align_left_len);
      align_left_len = current_align_left_len;
      current_length -= align_left_len;
    } else {
      memcpy(align_left + align_left_len, input, new_write_length);
      align_left_len += new_write_length;
      current_length = 0;
//      std::cout << "Update " << "align_left_len: " << align_left_len << std::endl;
      break ;
    }

    input += new_write_length;

//    std::cout << "Update current_length: " << current_length
//	      << ", align_left_len: " << align_left_len
//	      << ", new_write_length" << new_write_length
//	      << std::endl;

    pBufferList->pBuffers->dataLenInBytes = current_length;
    pOpData.packetType = CPA_CY_SYM_PACKET_TYPE_PARTIAL;
    pOpData.sessionCtx = sessionCtx;
    pOpData.hashStartSrcOffsetInBytes = 0;
    pOpData.messageLenToHashInBytes = current_length;
    pOpData.pDigestResult = (Cpa8U *)pBufferList->pUserData;
    do {
      status = cpaCySymPerformOp(
        cyInstHandle,
        (void *)&complete,
        &pOpData,
        pBufferList,
        pBufferList,
        NULL);
    } while (status == CPA_STATUS_RETRY);
    partial = true;
    if (CPA_STATUS_SUCCESS != status) {
      std::cout << "Update cpaCySymPerformOp failed. (status = " << status << std::endl;
    }

    if (CPA_STATUS_SUCCESS == status) {
      if (!COMPLETION_WAIT((&complete), TIMEOUT_MS)) {
        std::cout << "timeout or interruption in cpaCySymPerformOp" << std::endl;
      }
    }
  } while (left_length > 0);
  COMPLETION_DESTROY(&complete);
}

void QatHashCommon::Final(unsigned char *digest) {
//  std::cout << "Final start" << std::endl;
  CpaStatus status = CPA_STATUS_SUCCESS;
  if (pBufferList == nullptr) {
    pBufferList = getCpaBufferList(cyInstHandle);

    if (pBufferList == nullptr) {
      std::cout << "cannot get CpaBufferList" << std::endl;
      return;
    }
  }

  CpaCySymOpData pOpData = {0};

  struct COMPLETION_STRUCT complete;

  COMPLETION_INIT((&complete));

  pBufferList->pBuffers->dataLenInBytes = 0;
  if (align_left_len > 0) {

//    std::cout << "align_left_len: " << align_left_len << std::endl;
    memcpy(pBufferList->pBuffers->pData, align_left, align_left_len);
    pBufferList->pBuffers->dataLenInBytes = align_left_len;
    align_left_len = 0;
  }

  if (partial) {
    pOpData.packetType = CPA_CY_SYM_PACKET_TYPE_LAST_PARTIAL;
  } else {
    pOpData.packetType = CPA_CY_SYM_PACKET_TYPE_FULL;
  }
  pOpData.sessionCtx = sessionCtx;
  pOpData.hashStartSrcOffsetInBytes = 0;
  pOpData.messageLenToHashInBytes = pBufferList->pBuffers->dataLenInBytes;
  pOpData.pDigestResult = (Cpa8U *)pBufferList->pUserData;

  do {
    status = cpaCySymPerformOp(
      cyInstHandle,
      (void *)&complete,
      &pOpData,
      pBufferList,
      pBufferList,
      NULL);

  } while (status == CPA_STATUS_RETRY);
  if (CPA_STATUS_SUCCESS != status) {
    std::cout << "Final cpaCySymPerformOp failed. (status = " << status << std::endl;
  }

  if (CPA_STATUS_SUCCESS == status) {
    if (!COMPLETION_WAIT((&complete), TIMEOUT_MS)) {
      std::cout << "timeout or interruption in cpaCySymPerformOp" << std::endl;
    }
  }
  COMPLETION_DESTROY(&complete);

  memcpy(digest, pOpData.pDigestResult, digest_length);
//  std::cout << "Final end" << std::endl;
}
}

#pragma clang diagnostic pop
#pragma GCC diagnostic pop
