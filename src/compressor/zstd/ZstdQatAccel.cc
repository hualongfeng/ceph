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
  if (alg == "zlib") {
    QzSessionParamsDeflate_T params;
    rc = qzGetDefaultsDeflate(&params);
    if (rc != QZ_OK)
      return false;
    params.data_fmt = QZ_DEFLATE_RAW;
    params.common_params.comp_algorithm = QZ_DEFLATE;
    params.common_params.comp_lvl = g_ceph_context->_conf->compressor_zlib_level;
    params.common_params.direction = QZ_DIR_BOTH;
    rc = qzSetupSessionDeflate(session.get(), &params);
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

ZstdQatAccel::ZstdQatAccel() {}

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
  if (alg != "zlib") {
    return false;
  }

  alg_name = alg;
  return true;
}

int ZstdQatAccel::compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message) {
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction
  compressor_message = ZLIB_DEFAULT_WIN_SIZE;
  int begin = 1;
  for (auto &i : in.buffers()) {
    const unsigned char* c_in = (unsigned char*) i.c_str();
    unsigned int len = i.length();
    unsigned int out_len = qzMaxCompressedLength(len, session.get()) + begin;

    bufferptr ptr = buffer::create_small_page_aligned(out_len);
    unsigned char* c_out = (unsigned char*)ptr.c_str() + begin;
    int rc = qzCompress(session.get(), c_in, &len, c_out, &out_len, 1);
    if (rc != QZ_OK)
      return -1;
    if (begin) {
      // put a compressor variation mark in front of compressed stream, not used at the moment
      ptr.c_str()[0] = 0;
      out_len += begin;
      begin = 0;
    }
    out.append(ptr, 0, out_len);

  }

  return 0;
}

int ZstdQatAccel::decompress(const bufferlist &in, bufferlist &out, std::optional<int32_t> compressor_message) {
  auto i = in.begin();
  return decompress(i, in.length(), out, compressor_message);
}

int ZstdQatAccel::decompress(bufferlist::const_iterator &p,
		 size_t compressed_len,
		 bufferlist &dst,
		 std::optional<int32_t> compressor_message) {
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction
  int begin = 1;

  int rc = 0;
  bufferlist tmp;
  size_t remaining = std::min<size_t>(p.get_remaining(), compressed_len);

  while (remaining) {
    unsigned int ratio_idx = 0;
    const char* c_in = nullptr;
    unsigned int len = p.get_ptr_and_advance(remaining, &c_in);
    remaining -= len;
    len -= begin;
    c_in += begin;
    begin = 0;
    unsigned int out_len = QZ_HW_BUFF_SZ;

    bufferptr ptr;
    do {
      while (out_len <= len * expansion_ratio[ratio_idx]) {
        out_len *= 2;
      }

      ptr = buffer::create_small_page_aligned(out_len);
      rc = qzDecompress(session.get(), (const unsigned char*)c_in, &len, (unsigned char*)ptr.c_str(), &out_len);
      ratio_idx++;
    } while (rc == QZ_BUF_ERROR && ratio_idx < std::size(expansion_ratio));

    if (rc == QZ_OK) {
      dst.append(ptr, 0, out_len);
    } else if (rc == QZ_DATA_ERROR) {
      dout(1) << "QAT compressor DATA ERROR" << dendl;
      return -1;
    } else if (rc == QZ_BUF_ERROR) {
      dout(1) << "QAT compressor BUF ERROR" << dendl;
      return -1;
    } else if (rc != QZ_OK) {
      dout(1) << "QAT compressor NOT OK" << dendl;
      return -1;
    }
  }

  return 0;
}
