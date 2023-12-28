/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Intel Corporation
 *
 * Author: Qiaowei Ren <qiaowei.ren@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include <qatzip.h>

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"
#include "ZstdCompressor.h"

extern "C" struct QzSession_S; // typedef struct QzSession_S QzSession_T;

struct ZstdQzSessionDeleter {
  void operator() (struct QzSession_S *session);
};

class ZstdQatAccel {
 public:
  using session_ptr = std::unique_ptr<struct QzSession_S, QzSessionDeleter>;
  ZstdQatAccel();
  ~ZstdQatAccel();

  bool init(const std::string &alg);

  int compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message);
  int decompress(const bufferlist &in, bufferlist &out, std::optional<int32_t> compressor_message);
  int decompress(bufferlist::const_iterator &p, size_t compressed_len, bufferlist &dst, std::optional<int32_t> compressor_message);

 private:
  // get a session from the pool or create a new one. returns null if session init fails
  session_ptr get_session();

  friend struct cached_session_t;
  std::vector<session_ptr> sessions;
  std::mutex mutex;
  std::string alg_name;
};


/* macros for lz4 */
#define QZ_LZ4_MAGIC         0x184D2204U
#define QZ_LZ4_MAGIC_SKIPPABLE 0x184D2A50U
#define QZ_LZ4_VERSION       0x1
#define QZ_LZ4_BLK_INDEP     0x0
#define QZ_LZ4_BLK_CKS_FLAG  0x0
#define QZ_LZ4_DICT_ID_FLAG  0x0
#define QZ_LZ4_CNT_SIZE_FLAG 0x1
#define QZ_LZ4_CNT_CKS_FLAG  0x1
#define QZ_LZ4_ENDMARK       0x0
#define QZ_LZ4_MAX_BLK_SIZE  0x4

#define QZ_LZ4_MAGIC_SIZE      4                     //lz4 magic number length
#define QZ_LZ4_FD_SIZE         11                    //lz4 frame descriptor length
#define QZ_LZ4_HEADER_SIZE     (QZ_LZ4_MAGIC_SIZE + \
                                QZ_LZ4_FD_SIZE)      //lz4 frame header length
#define QZ_LZ4_CHECKSUM_SIZE   4                     //lz4 checksum length
#define QZ_LZ4_ENDMARK_SIZE    4                     //lz4 endmark length
#define QZ_LZ4_FOOTER_SIZE     (QZ_LZ4_CHECKSUM_SIZE + \
                                QZ_LZ4_ENDMARK_SIZE) //lz4 frame footer length
#define QZ_LZ4_BLK_HEADER_SIZE 4                     //lz4 block header length
#define QZ_LZ4_STOREDBLOCK_FLAG 0x80000000U
#define QZ_LZ4_STORED_HEADER_SIZE 4

#pragma pack(push, 1)
/* lz4 frame header */
typedef struct QzLZ4H_S {
    uint32_t magic; /* LZ4 magic number */
    uint8_t flag_desc;
    uint8_t block_desc;
    uint64_t cnt_size;
    uint8_t hdr_cksum; /* header checksum */
} QzLZ4H_T;

/* lz4 frame footer */
typedef struct QzLZ4F_S {
    uint32_t end_mark;  /* LZ4 end mark */
    uint32_t cnt_cksum; /* content checksum */
} QzLZ4F_T;
#pragma pack(pop)



// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "ZstdQatAccel: ";
}
// -----------------------------------------------------------------------------
// default window size for Zlib 1.2.8, negated for raw deflate
#define ZLIB_DEFAULT_WIN_SIZE -15

// app return status
#define QZSTD_OK 0
#define QZSTD_ERROR 1

// Error type for log capture.
#define QZSTD_ERROR_TYPE    "[QZSTD_ERROR]"
#define QZ_ERROR_TYPE       "[QZ_LIB_ERROR]"
#define ZSTD_ERROR_TYPE     "[ZSTD_LIB_ERROR]"

#define KB                  (1024)
#define MB                  (KB * KB)

#define ZSTD_SEQUENCES_SIZE (1024 * 32)

#define ML_BITS 4
#define ML_MASK ((1U << ML_BITS) - 1)
#define RUN_BITS (8 - ML_BITS)
#define RUN_MASK ((1U << RUN_BITS) - 1)

typedef uint8_t BYTE;
typedef uint16_t U16;
typedef int16_t S16;
typedef uint32_t U32;
typedef int32_t S32;
typedef uint64_t U64;
typedef int64_t S64;

#define QATZIP_MAX_HW_SZ (512 * KB)
#define ZSRC_BUFF_LEN (512 * MB)
#define MAX_BLOCK_SIZE (128 * KB)

/*
QzSessionParamsLZ4S_T sess_params_zstd_default = {
    .common_params.direction         = QZ_DIRECTION_DEFAULT,
    .common_params.comp_lvl          = QZ_COMP_LEVEL_DEFAULT,
    .common_params.comp_algorithm    = QZ_LZ4s,
    .common_params.max_forks         = QZ_MAX_FORK_DEFAULT,
    .common_params.sw_backup         = QZ_SW_BACKUP_DEFAULT,
    .common_params.hw_buff_sz        = QZ_HW_BUFF_SZ,
    .common_params.strm_buff_sz      = QZ_STRM_BUFF_SZ_DEFAULT,
    .common_params.input_sz_thrshold = QZ_COMP_THRESHOLD_DEFAULT,
    .common_params.req_cnt_thrshold  = 32,
    .common_params.wait_cnt_thrshold = QZ_WAIT_CNT_THRESHOLD_DEFAULT,
    .common_params.polling_mode   = QZ_PERIODICAL_POLLING,
    .lz4s_mini_match   = 3,
    .qzCallback        = zstdCallBack,
    .qzCallback_external = NULL
};
*/

static unsigned int LZ4MINMATCH = 2;

unsigned qzstd_isLittleEndian(void)
{
    const union {
        U32 u;
        BYTE c[4];
    } one = {1}; /* don't use static : performance detrimental */
    return one.c[0];
}

static U16 qzstd_read16(const void *memPtr)
{
    U16 val;
    memcpy(&val, memPtr, sizeof(val));
    return val;
}

U16 qzstd_readLE16(const void *memPtr)
{
    if (qzstd_isLittleEndian()) {
        return qzstd_read16(memPtr);
    } else {
        const BYTE *p = (const BYTE *)memPtr;
        return (U16)((U16)p[0] + (p[1] << 8));
    }
}

void decLz4Block(unsigned char *lz4s, int lz4sSize, ZSTD_Sequence *zstdSeqs,
                 unsigned int *seq_offset)
{
    unsigned char *ip = lz4s;
    unsigned char *endip = lz4s + lz4sSize;

    while (ip < endip && lz4sSize > 0) {
        size_t length = 0;
        size_t offset = 0;
        unsigned int literalLen = 0, matchlen = 0;
        /* get literal length */
        unsigned const token = *ip++;
        if ((length = (token >> ML_BITS)) == RUN_MASK) {
            unsigned s;
            do {
                s = *ip++;
                length += s;
            } while (s == 255);
        }
        literalLen = (unsigned short)length;
        ip += length;
        if (ip == endip) {
            // Meet the end of the LZ4 sequence
            /* update ZSTD_Sequence */
            zstdSeqs[*seq_offset].litLength += literalLen;
            continue;
        }

        /* get matchPos */
        offset = qzstd_readLE16(ip);
        ip += 2;

        /* get match length */
        length = token & ML_MASK;
        if (length == ML_MASK) {
            unsigned s;
            do {
                s = *ip++;
                length += s;
            } while (s == 255);
        }
        if (length != 0) {
            length += LZ4MINMATCH;
            matchlen = (unsigned short)length;

            /* update ZSTD_Sequence */
            zstdSeqs[*seq_offset].offset = offset;
            zstdSeqs[*seq_offset].litLength += literalLen;
            zstdSeqs[*seq_offset].matchLength = matchlen;

            ++(*seq_offset);

            assert(*seq_offset <= ZSTD_SEQUENCES_SIZE);
        } else {
            if (literalLen > 0) {
                /* When match length is 0, the literalLen needs to be temporarily stored
                and processed together with the next data block. If also ip == endip, need
                to convert sequences to seqStore.*/
                zstdSeqs[*seq_offset].litLength += literalLen;
            }
        }
    }
    assert(ip == endip);
}

inline int getLz4FrameHeaderSz()
{
    return QZ_LZ4_HEADER_SIZE;
}

inline int getLz4BlkHeaderSz()
{
    return QZ_LZ4_BLK_HEADER_SIZE;
}

inline int getLZ4FooterSz()
{
    return QZ_LZ4_FOOTER_SIZE;
}

int getContentSize(unsigned char *const ptr)
{
    QzLZ4H_T *hdr = NULL;
    hdr = (QzLZ4H_T *)ptr;
    assert(hdr->magic == QZ_LZ4_MAGIC);
    return hdr->cnt_size;
}

unsigned int getBlockSize(unsigned char *const ptr)
{
    unsigned int blk_sz = *(unsigned int *)ptr;
    return blk_sz;
}

int zstdCallBack(void *external, const unsigned char *src,
                 unsigned int *src_len, unsigned char *dest,
                 unsigned int *dest_len, int *ExtStatus)
{
    int ret = QZ_OK;
    //copied data is used to decode
    //original data will be overwrote by ZSTD_compressSequences
    unsigned char *dest_data = (unsigned char *)malloc(*dest_len);
    assert(dest_data != NULL);
    memcpy(dest_data, dest, *dest_len);
    unsigned char *cur = dest_data;
    unsigned char *end = dest_data + *dest_len;

    ZSTD_Sequence *zstd_seqs = (ZSTD_Sequence *)calloc(ZSTD_SEQUENCES_SIZE,
                               sizeof(ZSTD_Sequence));
    assert(zstd_seqs != NULL);

    ZSTD_CCtx *zc = (ZSTD_CCtx *)external;

    unsigned int produced = 0;
    unsigned int consumed = 0;
    unsigned int cnt_sz = 0, blk_sz = 0;    //content size and block size
    unsigned int dec_offset = 0;
    while (cur < end && *dest_len > 0) {
        //decode block header and get block size
        blk_sz = getBlockSize(cur);
        cur += getLz4BlkHeaderSz();

        //decode lz4s sequences into zstd sequences
        decLz4Block(cur, blk_sz, zstd_seqs, &dec_offset);
        cur += blk_sz;

        cnt_sz = 0;
        for (unsigned int i = 0; i < dec_offset + 1 ; i++) {
            cnt_sz += (zstd_seqs[i].litLength + zstd_seqs[i].matchLength) ;
        }
        assert(cnt_sz <= MAX_BLOCK_SIZE);

        // compress sequence to zstd frame
        int compressed_sz = ZSTD_compressSequences(zc,
                            dest + produced,
                            ZSTD_compressBound(cnt_sz),
                            zstd_seqs,
                            dec_offset + 1,
                            src + consumed,
                            cnt_sz);

        if (compressed_sz < 0) {
            ret = QZ_POST_PROCESS_ERROR;
            *ExtStatus = compressed_sz;
//            QZ_ERROR("%s : ZSTD API ZSTD_compressSequences failed with error code, %d, %s\n",
//                     ZSTD_ERROR_TYPE, *ExtStatus, DECODE_ZSTD_ERROR_CODE(*ExtStatus));
            goto done;
        }
        //reuse zstd_seqs
        memset(zstd_seqs, 0, ZSTD_SEQUENCES_SIZE * sizeof(ZSTD_Sequence));
        dec_offset = 0;
        produced += compressed_sz;
        consumed += cnt_sz;
    }

    *dest_len = produced;

done:
    free(dest_data);
    free(zstd_seqs);
    return ret;
}

/* Estimate data expansion after decompression */
static const unsigned int expansion_ratio[] = {5, 20, 50, 100, 200, 1000, 10000};

void ZstdQzSessionDeleter::operator() (struct QzSession_S *session) {
  qzTeardownSession(session);
  delete session;
}

static bool setup_session(const std::string &alg, ZstdQatAccel::session_ptr &session) {
  int rc;
  rc = qzInit(session.get(), QZ_SW_BACKUP_DEFAULT);
  if (rc != QZ_OK && rc != QZ_DUPLICATE)
    return false;
  if (alg == "zstd") {
    QzSessionParamsLZ4S_T params;
    rc = qzGetDefaultsLZ4S(&params);
    if (rc != QZ_OK)
      return false;
    params.common_params.direction         = QZ_DIR_BOTH;
    params.common_params.comp_lvl          = g_ceph_context->_conf->compressor_zlib_level;
    params.common_params.comp_algorithm    = QZ_LZ4s;
    params.common_params.max_forks         = QZ_MAX_FORK_DEFAULT;
    params.common_params.sw_backup         = QZ_SW_BACKUP_DEFAULT;
    params.common_params.hw_buff_sz        = QZ_HW_BUFF_SZ;
    params.common_params.strm_buff_sz      = QZ_STRM_BUFF_SZ_DEFAULT;
    params.common_params.input_sz_thrshold = QZ_COMP_THRESHOLD_DEFAULT;
    params.common_params.req_cnt_thrshold  = 32;
    params.common_params.wait_cnt_thrshold = QZ_WAIT_CNT_THRESHOLD_DEFAULT;
    params.common_params.polling_mode   = QZ_PERIODICAL_POLLING;
    params.lz4s_mini_match   = 3;
    params.qzCallback        = zstdCallBack;
    params.qzCallback_external = NULL;

    //initial zstd context
    ZSTD_CCtx *zc = ZSTD_createCCtx();
    if (zc == NULL) {
        dout(1) << "ZSTD_createCCtx failed: " << dendl;
        return QZSTD_ERROR;
    }
    ZSTD_CCtx_setParameter(zc, ZSTD_c_blockDelimiters,
                           ZSTD_sf_explicitBlockDelimiters);
    params.qzCallback_external = (void *)zc;

    /* Different mini_match would use different LZ4MINMATCH to decode
    * lz4s sequence. note that when it is mini_match is 4, the LZ4MINMATCH
    * should be 3. if mini match is 3, then LZ4MINMATCH should be 2*/
    LZ4MINMATCH = params.lz4s_mini_match == 4 ? 3 : 2;

    /* Align zstd minmatch to the QAT minmatch */
    ZSTD_CCtx_setParameter(
        zc, ZSTD_c_minMatch,
        params.lz4s_mini_match >= 4 ? 4 : 3
    );

    rc = qzSetupSessionLZ4S(session.get(), &params);
    if (rc != QZ_OK)
      return false;
  }
  else {
    // later, there also has lz4.
    return false;
  }
  return true;
}

// put the session back to the session pool in a RAII manner
struct cached_session_t {
  cached_session_t(ZstdQatAccel* accel, ZstdQatAccel::session_ptr&& sess)
    : accel{accel}, session{std::move(sess)} {}

  ~cached_session_t() {
    std::scoped_lock lock{accel->mutex};
    // if the cache size is still under its upper bound, the current session is put into
    // accel->sessions. otherwise it's released right
    uint64_t sessions_num = g_ceph_context->_conf.get_val<uint64_t>("qat_compressor_session_max_number");
    if (accel->sessions.size() < sessions_num) {
      accel->sessions.push_back(std::move(session));
    }
  }

  struct QzSession_S* get() {
    assert(static_cast<bool>(session));
    return session.get();
  }

  ZstdQatAccel* accel;
  ZstdQatAccel::session_ptr session;
};

ZstdQatAccel::session_ptr ZstdQatAccel::get_session() {
  {
    std::scoped_lock lock{mutex};
    if (!sessions.empty()) {
      auto session = std::move(sessions.back());
      sessions.pop_back();
      return session;
    }
  }

  // If there are no available session to use, we try allocate a new
  // session.
  session_ptr session(new struct QzSession_S());
  memset(session.get(), 0, sizeof(struct QzSession_S));
  if (setup_session(alg_name, session)) {
    return session;
  } else {
    return nullptr;
  }
}

ZstdQatAccel::ZstdQatAccel() {
  init("zstd");
}

ZstdQatAccel::~ZstdQatAccel() {
  // First, we should uninitialize all QATzip session that disconnects all session
  // from a hardware instance and deallocates buffers.
  sessions.clear();
  // Then we close the connection with QAT.
  // where the value of the parameter passed to qzClose() does not matter. as long as
  // it is not nullptr.
  qzClose((QzSession_T*)1);
}

bool ZstdQatAccel::init(const std::string &alg) {
  std::scoped_lock lock(mutex);
  if (!alg_name.empty()) {
    return true;
  }

  dout(15) << "First use for QAT compressor" << dendl;
  if (alg != "zstd") {
    return false;
  }

  alg_name = alg;
  return true;
}

int ZstdQatAccel::compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message) {
  int rc;
  uint64_t callback_error_code = 0;
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction

  // prefix with decompressed length
  ceph::encode((uint32_t)in.length(), out);

  for (auto &i : in.buffers()) {
    const unsigned char* c_in = (unsigned char*) i.c_str();
    unsigned int len = i.length();
    unsigned int out_len = qzMaxCompressedLength(len, session.get());
    out_len = ZSTD_compressBound(out_len);

    bufferptr ptr = buffer::create_small_page_aligned(out_len);
    unsigned char* c_out = (unsigned char*)ptr.c_str();
//    if (len < QZ_COMP_THRESHOLD_DEFAULT) {
      //initial zstd context
//    ZSTD_CCtx *zc = ZSTD_createCCtx();
//    if (zc == NULL) {
//        dout(1) << "ZSTD_createCCtx failed: " << dendl;
//        return QZSTD_ERROR;
//    }
//    ZSTD_CCtx_setParameter(zc, ZSTD_c_blockDelimiters,
//                           ZSTD_sf_explicitBlockDelimiters);
//
//    // Align zstd minmatch to the QAT minmatch
//    ZSTD_CCtx_setParameter(
//        zc, ZSTD_c_minMatch,
//        3 >= 4 ? 4 : 3
//    );
//    out_len = ZSTD_compressCCtx(zc, c_out, out_len, c_in, len, 1);
//    if (out_len <= 0) {
//        dout(1) << "ZSTD API ZSTD_compressCCtx failed. c_in len=" << len << dendl;
//        return QZSTD_ERROR;
//    }
//    if (zc != NULL) {
//      ZSTD_freeCCtx(zc);
//    }
//    } else {
        rc = qzCompressExt(session.get(), c_in, &len, c_out, &out_len, 1, &callback_error_code);
//    }
      if (rc != QZ_OK) {
        dout(1) << "ZSTD API ZSTD_compressSequences failed with error code " << callback_error_code << dendl;
        return -1;
      }
    out.append(ptr, 0, out_len);

  }
  return 0;
}



#include "ZstdCompressor.h"


static ZstdQatAccel zstd_qat_accel;

int ZstdCompressor::compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) {

#ifdef HAVE_QATZIP
  if (zstd_qat_enabled && src.length() >= 32768)
    return zstd_qat_accel.compress(src, dst, compressor_message);
#endif

    ZSTD_CStream *s = ZSTD_createCStream();
    ZSTD_CCtx_reset(s, ZSTD_reset_session_only);
    ZSTD_CCtx_refCDict(s, NULL); // clear the dictionary (if any)
    ZSTD_CCtx_setParameter(s, ZSTD_c_compressionLevel, cct->_conf->compressor_zstd_level);
    ZSTD_CCtx_setPledgedSrcSize(s, src.length());
    //ZSTD_initCStream_srcSize(s, cct->_conf->compressor_zstd_level, src.length());
    auto p = src.begin();
    size_t left = src.length();

    size_t const out_max = ZSTD_compressBound(left);
    ceph::buffer::ptr outptr = ceph::buffer::create_small_page_aligned(out_max);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = outptr.c_str();
    outbuf.size = outptr.length();
    outbuf.pos = 0;

    while (left) {
      ceph_assert(!p.end());
      struct ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(left, (const char**)&inbuf.src);
      left -= inbuf.size;
      ZSTD_EndDirective const zed = (left==0) ? ZSTD_e_end : ZSTD_e_continue;
      size_t r = ZSTD_compressStream2(s, &outbuf, &inbuf, zed);
      if (ZSTD_isError(r)) {
	return -EINVAL;
      }
    }
    ceph_assert(p.end());

    ZSTD_freeCStream(s);

    // prefix with decompressed length
    ceph::encode((uint32_t)src.length(), dst);
    dst.append(outptr, 0, outbuf.pos);
    return 0;
  }

int ZstdCompressor::decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> compressor_message) {
    auto i = std::cbegin(src);
    return decompress(i, src.length(), dst, compressor_message);
  }

  int ZstdCompressor::decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 std::optional<int32_t> compressor_message) {
    if (compressed_len < 4) {
      return -1;
    }
    compressed_len -= 4;
    uint32_t dst_len;
    ceph::decode(dst_len, p);

    ceph::buffer::ptr dstptr(dst_len);
    ZSTD_outBuffer_s outbuf;
    outbuf.dst = dstptr.c_str();
    outbuf.size = dstptr.length();
    outbuf.pos = 0;
    ZSTD_DStream *s = ZSTD_createDStream();
    ZSTD_initDStream(s);
    while (compressed_len > 0) {
      if (p.end()) {
	return -1;
      }
      ZSTD_inBuffer_s inbuf;
      inbuf.pos = 0;
      inbuf.size = p.get_ptr_and_advance(compressed_len,
					 (const char**)&inbuf.src);
      ZSTD_decompressStream(s, &outbuf, &inbuf);
      compressed_len -= inbuf.size;
    }
    ZSTD_freeDStream(s);

    dst.append(dstptr, 0, outbuf.pos);
    return 0;
  }

