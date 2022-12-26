#ifndef QCCCRYPTO_H
#define QCCCRYPTO_H

#include <atomic>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <thread>
#include <mutex>
#include <queue>
#include <memory>
#include "common/async/yield_context.h"
#include "common/async/completion.h"
#include "include/rados/librados_fwd.hpp"
#include <memory>
#include "common/ceph_mutex.h"
#include <vector>
#include <map>
extern "C" {
#include "cpa.h"
#include "cpa_cy_sym_dp.h"
#include "cpa_cy_im.h"
#include "lac/cpa_cy_sym.h"
#include "lac/cpa_cy_im.h"
#include "qae_mem.h"
#include "icp_sal_user.h"
#include "icp_sal_poll.h"
#include "qae_mem_utils.h"
}

class QccCrypto {
  friend class QatCrypto;
  static const size_t AES_256_IV_LEN = 16;
  static const size_t AES_256_KEY_SIZE = 32;
  static const size_t MAX_NUM_SYM_REQ_BATCH = 32;

  size_t chunk_size;
  Cpa16U numInstances{0};
  std::vector<CpaInstanceHandle> cyInstances;
  std::atomic<size_t> index{0};

  std::thread qat_poll_thread;
  bool thread_stop{false};

  boost::asio::io_context::strand opmem_strand;
  boost::asio::io_context::strand session_strand;
public:
  struct OPMEM {
    CpaCySymDpOpData *sym_op_data{nullptr};
    Cpa8U *src_buff{nullptr};
    Cpa8U *iv_buff{nullptr};
   public:
    OPMEM() {};
    OPMEM(size_t chunk_size);
    OPMEM(const OPMEM &opmem) = delete;
    OPMEM(OPMEM &&opmem)
      : sym_op_data(opmem.sym_op_data),
        src_buff(opmem.src_buff),
        iv_buff(opmem.iv_buff) {
      opmem.sym_op_data = nullptr;
      opmem.src_buff = nullptr;
      opmem.iv_buff = nullptr;
    }
    void operator=(const OPMEM &opmem) = delete;
    void operator=(OPMEM &&opmem){
      sym_op_data = opmem.sym_op_data;
      src_buff = opmem.src_buff;
      iv_buff = opmem.iv_buff;
      opmem.sym_op_data = nullptr;
      opmem.src_buff = nullptr;
      opmem.iv_buff = nullptr;
    }
    ~OPMEM();
    bool alloc_ok() {return (sym_op_data != nullptr) && (src_buff != nullptr) && (iv_buff != nullptr);}
  };
  std::vector<OPMEM> op_mem_pool;
  size_t op_mem_capacity{0};
  std::vector<CpaCySymSessionCtx> session_pool;
  size_t session_capacity{0};

  public:
    CpaCySymCipherDirection qcc_op_type;

    QccCrypto(boost::asio::io_context& context): opmem_strand(context), session_strand(context) {};
    ~QccCrypto() {};

    bool init(const size_t chunk_size);
    bool destroy();
    bool perform_op_batch(unsigned char* out, const unsigned char* in, size_t size,
                          Cpa8U *iv,
                          Cpa8U *key,
                          CpaCySymCipherDirection op_type,
                          optional_yield y);

  private:
    /*
     * Contiguous Memory Allocator and de-allocator. We are using the usdm
     * driver that comes along with QAT to get us direct memory access using
     * hugepages.
     * To-Do: A kernel based one.
     */
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

    /*
     * Malloc & free calls masked to maintain consistency and future kernel
     * alloc support.
     */
    static inline void qcc_os_mem_free(void **ptr) {
      if (*ptr) {
        free(*ptr);
        *ptr = NULL;
      }
    }

    static inline CpaStatus qcc_os_mem_alloc(void **ptr, Cpa32U size) {
      *ptr = malloc(size);
      if (*ptr == NULL)
      {
        return CPA_STATUS_RESOURCE;
      }
      return CPA_STATUS_SUCCESS;
    }

    std::atomic<bool> is_init = { false };

    /*
     * Function to cleanup memory if constructor fails
     */
    void cleanup();

    /*
     * Crypto Polling Function
     * This helps to retrieve data from the QAT rings and dispatching the
     * associated callbacks. For synchronous operation (like this one), QAT
     * library creates an internal callback for the operation.
     */
    void poll_instances(void);

    bool symPerformOp(CpaInstanceHandle instance,
                      const Cpa8U *pSrc,
                      Cpa8U *pDst,
                      Cpa32U size,
                      Cpa8U *pIv,
                      Cpa32U ivLen,
                      Cpa8U *key,
                      CpaCySymCipherDirection op_type,
                      optional_yield y);

    CpaStatus initSession(CpaInstanceHandle cyInstHandle,
                          CpaCySymSessionCtx *sessionCtx,
                          Cpa8U *pCipherKey,
                          CpaCySymCipherDirection cipherDirection);

    CpaStatus updateSession(CpaCySymSessionCtx sessionCtx,
                            Cpa8U *pCipherKey,
                            CpaCySymCipherDirection cipherDirection);

    static void symDpCallback(CpaCySymDpOpData *pOpData,
                              CpaStatus status,
                              CpaBoolean verifyResult);
};

class QatCrypto {
  boost::asio::io_context& context;
  yield_context yield;
  struct Handler;
  using Completion = ceph::async::Completion<void(boost::system::error_code)>;

  template <typename CompletionToken>
  auto async_perform_op(CompletionToken&& token);

  template <typename CompletionToken>
  auto async_get_op_mem(CompletionToken&& token);

  template <typename CompletionToken>
  auto async_get_session(CompletionToken&& token);

  CpaInstanceHandle cyInstHandle{nullptr};
  CpaCySymSessionCtx sessionCtx{nullptr};
  std::vector<CpaCySymDpOpData*> pOpDataVec;
  size_t chunk_size;
  QccCrypto *crypto;
  Cpa8U *key;
  CpaCySymCipherDirection op_type;

  QccCrypto::OPMEM opmem;
  std::vector<QccCrypto::OPMEM> op_mem_used;

 public:
  std::unique_ptr<Completion> completion;
  std::unique_ptr<Completion> op_mem_completion;
  std::unique_ptr<Completion> session_completion;
  QatCrypto (boost::asio::io_context& context,
             yield_context yield,
             CpaInstanceHandle cyInstHandle,
             size_t chunk_size,
             QccCrypto *crypto,
             Cpa8U *key,
             CpaCySymCipherDirection op_type
             ) : context(context), yield(yield),
                 cyInstHandle(cyInstHandle),
                 chunk_size(chunk_size), crypto(crypto),
                 key(key), op_type(op_type) {}
  QatCrypto (const QatCrypto &qat) = delete;
  // ~QatCrypto ();
  bool performOp(const Cpa8U *pSrc,
                 Cpa8U *pDst,
                 Cpa32U size,
                 Cpa8U *pIv,
                 Cpa32U ivLen);
};

#endif //QCCCRYPTO_H
