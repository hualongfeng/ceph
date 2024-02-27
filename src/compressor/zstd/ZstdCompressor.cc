// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Haomai Wang <haomaiwang@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ZstdCompressor.h"

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd/lib/zstd.h"

#include "include/buffer.h"
#include "include/encoding.h"
#include "compressor/Compressor.h"
#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"


#include "qatseqprod.h"


// -----------------------------------------------------------------------------
#define dout_context cct
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix _prefix(_dout)
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

using std::ostream;

using ceph::bufferlist;
using ceph::bufferptr;

static ostream&
_prefix(std::ostream* _dout)
{
  return *_dout << "ZstdCompressor: ";
}
// -----------------------------------------------------------------------------


int ZstdCompressor::compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) {
  void *sequenceProducerState{nullptr};
  ZSTD_CStream *s = ZSTD_createCStream();

  bool qat_enabled=true;
  if (qat_enabled) {

    dout(15) << "Start to use QAT" << dendl;
    QZSTD_startQatDevice();
    sequenceProducerState = QZSTD_createSeqProdState();
    ZSTD_registerSequenceProducer(
          	  s,
          	  sequenceProducerState,
          	  qatSequenceProducer
          	  );
//  int res = ZSTD_CCtx_setParameter(s, ZSTD_c_enableSeqProducerFallback, 1);
//  if (res <= 0) {
//    dout(1) << "Failed to set fallback: res=" << res << dendl;
//    return -1;
//  }
    int res = ZSTD_CCtx_setParameter(s, ZSTD_c_searchForExternalRepcodes, ZSTD_ps_disable);
    if (res <= 0) {
      dout(1) << "Failed to set searchForExternalRepcodes: res=" << res << ZSTD_getErrorName(res) << dendl;
      return -1;
    }
  }

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

  if (qat_enabled && sequenceProducerState != nullptr) {
    QZSTD_freeSeqProdState(sequenceProducerState);
  }

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
