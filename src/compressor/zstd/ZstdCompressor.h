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

#ifndef CEPH_ZSTDCOMPRESSOR_H
#define CEPH_ZSTDCOMPRESSOR_H

#define ZSTD_STATIC_LINKING_ONLY
#include "zstd/lib/zstd.h"

#include "include/buffer.h"
#include "include/encoding.h"
#include "compressor/Compressor.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <vector>

#include "include/buffer.h"

class ZstdCompressor : public Compressor {
 public:

//#ifdef HAVE_QATZIP
//  bool zstd_qat_enabled;
//#endif

 public:
  ZstdCompressor(CephContext *cct) : Compressor(COMP_ALG_ZSTD, "zstd"), cct(cct) {
#ifdef HAVE_QATZIP
    if (cct->_conf->qat_compressor_enabled)
      zstd_qat_enabled = true;
    else
      zstd_qat_enabled = false;
#endif
  }

  int compress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> &compressor_message) override;

  int decompress(const ceph::buffer::list &src, ceph::buffer::list &dst, std::optional<int32_t> compressor_message) override;

  int decompress(ceph::buffer::list::const_iterator &p,
		 size_t compressed_len,
		 ceph::buffer::list &dst,
		 std::optional<int32_t> compressor_message) override;
 private:
  CephContext *const cct;
};

#endif

