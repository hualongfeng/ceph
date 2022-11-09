// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#ifndef CEPH_CRYPTO_H
#define CEPH_CRYPTO_H

#include "acconfig.h"
#include <stdexcept>

#include "include/common_fwd.h"
#include "include/buffer.h"
#include "include/types.h"

extern "C" {
#include "cpa.h"
#include "lac/cpa_cy_sym.h"
}

//quickassist/lookaside/access_layer/src/common/crypto/sym/include/lac_sym_hash_defs.h
#define LAC_HASH_MD5_BLOCK_SIZE    64
#define LAC_HASH_SHA1_BLOCK_SIZE   64
#define LAC_HASH_SHA256_BLOCK_SIZE 64
#define LAC_HASH_SHA512_BLOCK_SIZE 128
#define LAC_HASH_MAX_BLOCK_SIZE LAC_HASH_SHA512_BLOCK_SIZE


#define CEPH_CRYPTO_MD5_DIGESTSIZE 16
#define CEPH_CRYPTO_HMACSHA1_DIGESTSIZE 20
#define CEPH_CRYPTO_SHA1_DIGESTSIZE 20
#define CEPH_CRYPTO_HMACSHA256_DIGESTSIZE 32
#define CEPH_CRYPTO_SHA256_DIGESTSIZE 32
#define CEPH_CRYPTO_SHA512_DIGESTSIZE 64
#define CEPH_CRYPTO_MAX_DIGESTSIZE CEPH_CRYPTO_SHA512_DIGESTSIZE

#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <openssl/hmac.h>

#include "include/ceph_assert.h"

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

extern "C" {
  const EVP_MD *EVP_md5(void);
  const EVP_MD *EVP_sha1(void);
  const EVP_MD *EVP_sha256(void);
  const EVP_MD *EVP_sha512(void);
}

namespace TOPNSPC::crypto {
  void assert_init();
  void init();
  void shutdown(bool shared=true);

  void zeroize_for_security(void *s, size_t n);

  class DigestException : public std::runtime_error
  {
    public:
      DigestException(const char* what_arg) : runtime_error(what_arg)
	{}
  };

  namespace ssl {
    class OpenSSLDigest {
      private:
	EVP_MD_CTX *mpContext;
	const EVP_MD *mpType;
      public:
	OpenSSLDigest (const EVP_MD *_type);
	~OpenSSLDigest ();
	void Restart();
	void SetFlags(int flags);
	void Update (const unsigned char *input, size_t length);
	void Final (unsigned char *digest);
    };

    class MD5 : public OpenSSLDigest {
      public:
	static constexpr size_t digest_size = CEPH_CRYPTO_MD5_DIGESTSIZE;
	MD5 () : OpenSSLDigest(EVP_md5()) { }
    };

    class SHA1 : public OpenSSLDigest {
      public:
        static constexpr size_t digest_size = CEPH_CRYPTO_SHA1_DIGESTSIZE;
        SHA1 () : OpenSSLDigest(EVP_sha1()) { }
    };

    class SHA256 : public OpenSSLDigest {
      public:
        static constexpr size_t digest_size = CEPH_CRYPTO_SHA256_DIGESTSIZE;
        SHA256 () : OpenSSLDigest(EVP_sha256()) { }
    };

    class SHA512 : public OpenSSLDigest {
      public:
        static constexpr size_t digest_size = CEPH_CRYPTO_SHA512_DIGESTSIZE;
        SHA512 () : OpenSSLDigest(EVP_sha512()) { }
    };


# if OPENSSL_VERSION_NUMBER < 0x10100000L
  class HMAC {
  private:
    HMAC_CTX mContext;
    const EVP_MD *mpType;

  public:
    HMAC (const EVP_MD *type, const unsigned char *key, size_t length)
      : mpType(type) {
      // the strict FIPS zeroization doesn't seem to be necessary here.
      // just in the case.
      ::TOPNSPC::crypto::zeroize_for_security(&mContext, sizeof(mContext));
      const auto r = HMAC_Init_ex(&mContext, key, length, mpType, nullptr);
      if (r != 1) {
	  throw DigestException("HMAC_Init_ex() failed");
      }
    }
    ~HMAC () {
      HMAC_CTX_cleanup(&mContext);
    }

    void Restart () {
      const auto r = HMAC_Init_ex(&mContext, nullptr, 0, mpType, nullptr);
      if (r != 1) {
	throw DigestException("HMAC_Init_ex() failed");
      }
    }
    void Update (const unsigned char *input, size_t length) {
      if (length) {
        const auto r = HMAC_Update(&mContext, input, length);
	if (r != 1) {
	  throw DigestException("HMAC_Update() failed");
	}
      }
    }
    void Final (unsigned char *digest) {
      unsigned int s;
      const auto r = HMAC_Final(&mContext, digest, &s);
      if (r != 1) {
	throw DigestException("HMAC_Final() failed");
      }
    }
  };
# else
  class HMAC {
  private:
    HMAC_CTX *mpContext;

  public:
    HMAC (const EVP_MD *type, const unsigned char *key, size_t length)
      : mpContext(HMAC_CTX_new()) {
      const auto r = HMAC_Init_ex(mpContext, key, length, type, nullptr);
      if (r != 1) {
	throw DigestException("HMAC_Init_ex() failed");
      }
    }
    ~HMAC () {
      HMAC_CTX_free(mpContext);
    }

    void Restart () {
      const EVP_MD * const type = HMAC_CTX_get_md(mpContext);
      const auto r = HMAC_Init_ex(mpContext, nullptr, 0, type, nullptr);
      if (r != 1) {
	throw DigestException("HMAC_Init_ex() failed");
      }
    }
    void Update (const unsigned char *input, size_t length) {
      if (length) {
        const auto r = HMAC_Update(mpContext, input, length);
	if (r != 1) {
	  throw DigestException("HMAC_Update() failed");
	}
      }
    }
    void Final (unsigned char *digest) {
      unsigned int s;
      const auto r = HMAC_Final(mpContext, digest, &s);
      if (r != 1) {
	throw DigestException("HMAC_Final() failed");
      }
    }
  };
# endif // OPENSSL_VERSION_NUMBER < 0x10100000L

  struct HMACSHA1 : public HMAC {
    HMACSHA1 (const unsigned char *key, size_t length)
      : HMAC(EVP_sha1(), key, length) {
    }
  };

  struct HMACSHA256 : public HMAC {
    HMACSHA256 (const unsigned char *key, size_t length)
      : HMAC(EVP_sha256(), key, length) {
    }
  };
}


  namespace qat {
  class QatHashCommon {
   private:
    CpaInstanceHandle cyInstHandle{nullptr};
    CpaCySymSessionCtx sessionCtx{nullptr};
    CpaBufferList *pBufferList{nullptr};
    size_t digest_length{0};
    // partial packet need to align block length
    size_t block_length{0};
    unsigned char* align_left;
    size_t align_left_len{0};
    bool partial{false};
   protected:
    CpaCySymSessionSetupData* sessionSetupData;
    CpaCySymHashAlgorithm mpType{CPA_CY_SYM_HASH_NONE};
   public:
    QatHashCommon (const CpaCySymHashAlgorithm mpType);
    QatHashCommon (const QatHashCommon &qat) = delete;
    QatHashCommon (QatHashCommon &&qat);
    ~QatHashCommon ();
    void Restart();
    void SetFlags(int flags){}
    void Update(const unsigned char *input, size_t length);
    void Final(unsigned char *digest);
  };


  class QatDigest : public QatHashCommon {
   public:
    QatDigest (const CpaCySymHashAlgorithm mpType) :
	  QatHashCommon(mpType) {
      this->Restart();
    }
    QatDigest (const QatDigest& qat_digest) = delete;

    QatDigest (QatDigest&& qat_digest) : QatHashCommon(std::move(qat_digest)){};
  };

  class MD5 : public QatDigest {
   public:
    static constexpr size_t digest_size = CEPH_CRYPTO_MD5_DIGESTSIZE;
    MD5() : QatDigest(CPA_CY_SYM_HASH_MD5) {}
  };

  class SHA1 : public QatDigest {
   public:
    static constexpr size_t digest_size = CEPH_CRYPTO_SHA1_DIGESTSIZE;
    SHA1() : QatDigest(CPA_CY_SYM_HASH_SHA1) {}
  };

  class SHA256 : public QatDigest {
   public:
    static constexpr size_t digest_size = CEPH_CRYPTO_SHA256_DIGESTSIZE;
    SHA256() : QatDigest(CPA_CY_SYM_HASH_SHA256) {}
  };

  class SHA512 : public QatDigest {
   public:
    static constexpr size_t digest_size = CEPH_CRYPTO_SHA512_DIGESTSIZE;
    SHA512() : QatDigest(CPA_CY_SYM_HASH_SHA512) {}
  };

  class QatHMAC : public QatHashCommon {
   public:
    QatHMAC (const CpaCySymHashAlgorithm mpType,
	   const unsigned char *key, size_t length) :
	  QatHashCommon(mpType) {
      sessionSetupData->hashSetupData.hashMode = CPA_CY_SYM_HASH_MODE_AUTH;
      sessionSetupData->hashSetupData.authModeSetupData.authKey = const_cast<Cpa8U*>(key);
      sessionSetupData->hashSetupData.authModeSetupData.authKeyLenInBytes = length;
      this->Restart();
    }
    QatHMAC (const QatHMAC& qat_hmac) = delete;
    QatHMAC (QatHMAC&& qat_hmac) :
	  QatHashCommon(std::move(qat_hmac)) {
    }

  };

  class HMACSHA1 : public QatHMAC {
   public:
    static constexpr size_t digest_size = CEPH_CRYPTO_HMACSHA1_DIGESTSIZE;
    HMACSHA1(const unsigned char *key, size_t length) :
	  QatHMAC(CPA_CY_SYM_HASH_SHA1, key, length){}
  };

  class HMACSHA256 : public QatHMAC {
   public:
    static constexpr size_t digest_size = CEPH_CRYPTO_HMACSHA256_DIGESTSIZE;
    HMACSHA256(const unsigned char *key, size_t length) :
	  QatHMAC(CPA_CY_SYM_HASH_SHA256, key, length){}
  };
}

//  using ssl::SHA256;
  using ssl::MD5;
//  using ssl::SHA1;
//  using ssl::SHA512;

//  using ssl::HMACSHA256;
//  using ssl::HMACSHA1;

  using qat::SHA256;
//  using qat::MD5;
  using qat::SHA1;
  using qat::SHA512;

  using qat::HMACSHA256;
  using qat::HMACSHA1;



template<class Digest>
auto digest(const ceph::buffer::list& bl)
{
  unsigned char fingerprint[Digest::digest_size];
  Digest gen;
  for (auto& p : bl.buffers()) {
    gen.Update((const unsigned char *)p.c_str(), p.length());
  }
  gen.Final(fingerprint);
  return sha_digest_t<Digest::digest_size>{fingerprint};
}
}

#pragma clang diagnostic pop
#pragma GCC diagnostic pop

#endif
