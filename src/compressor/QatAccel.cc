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
#include "QatAccel.h"

// -----------------------------------------------------------------------------
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "QatAccel: ";
}
// -----------------------------------------------------------------------------
#define MAX_LEN (CEPH_PAGE_SIZE)

void QzSessionDeleter::operator() (struct QzSession_S *session) {
  qzTeardownSession(session);
  delete session;
}

static bool get_qz_params(const std::string &alg, QzSessionParams_T &params) {
  int rc;
  rc = qzGetDefaults(&params);
  if (rc != QZ_OK)
    return false;
  params.direction = QZ_DIR_BOTH;
  params.is_busy_polling = true;
  if (alg == "zlib") {
    params.comp_algorithm = QZ_DEFLATE;
    params.data_fmt = QZ_DEFLATE_RAW;
    params.comp_lvl = g_ceph_context->_conf->compressor_zlib_level;
    params.strm_buff_sz = params.hw_buff_sz * 2;
  }
  else {
    // later, there also has lz4.
    return false;
  }

  rc = qzSetDefaults(&params);
  if (rc != QZ_OK)
      return false;
  return true;
}

static bool setup_session(QatAccel::session_ptr &session, QzSessionParams_T &params) {
  int rc;
  rc = qzInit(session.get(), QZ_SW_BACKUP_DEFAULT);
  if (rc != QZ_OK && rc != QZ_DUPLICATE)
    return false;
  rc = qzSetupSession(session.get(), &params);
  if (rc != QZ_OK) {
    return false;
  }
  return true;
}

// put the session back to the session pool in a RAII manner
struct cached_session_t {
  cached_session_t(QatAccel* accel, QatAccel::session_ptr&& sess)
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

  QatAccel* accel;
  QatAccel::session_ptr session;
};

QatAccel::session_ptr QatAccel::get_session() {
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
  QzSessionParams_T params = {(QzHuffmanHdr_T)0,};
  session_ptr session(new struct QzSession_S());
  memset(session.get(), 0, sizeof(struct QzSession_S));
  if (get_qz_params(alg_name, params) && setup_session(session, params)) {
    return session;
  } else {
    return nullptr;
  }
}

QatAccel::QatAccel() {}

QatAccel::~QatAccel() {
  // First, we should uninitialize all QATzip session that disconnects all session
  // from a hardware instance and deallocates buffers.
  sessions.clear();
  // Then we close the connection with QAT.
  // where the value of the parameter passed to qzClose() does not matter. as long as
  // it is not nullptr.
  qzClose((QzSession_T*)1);
}

bool QatAccel::init(const std::string &alg) {
  std::scoped_lock lock(mutex);
  if (!alg_name.empty()) {
    return true;
  }

  QzSessionParams_T params = {(QzHuffmanHdr_T)0,};
  qzGetDefaults(&params);
  slice_sz = params.hw_buff_sz / 4;

  dout(15) << "First use for QAT compressor" << dendl;
  if (alg != "zlib") {
    return false;
  }

  alg_name = alg;
  return true;
}

int QatAccel::compress(const bufferlist &in, bufferlist &out, boost::optional<int32_t> &compressor_message) {
  dout(10) << "in compress" << dendl;
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction
  compressor_message = g_ceph_context->_conf->compressor_zlib_winsize;

  int rc = 0;
  QzStream_T comp_strm = {0};
  unsigned int consumed = 0;
  unsigned int last = 0;
  int begin = 1;
  unsigned have;
  unsigned int input_left = 0;
  unsigned int orig_sz = 0;

  for (auto &i : in.buffers()) {
    orig_sz = input_left = i.length();
    do {
      comp_strm.in = (unsigned char*) i.c_str() + orig_sz - input_left;
      comp_strm.in_sz = (input_left > slice_sz) ? slice_sz : input_left;
      last = (((consumed + comp_strm.in_sz) == in.length()) ? 1 : 0);

      bufferptr ptr = ceph::buffer::create_page_aligned(MAX_LEN);
      comp_strm.out = (unsigned char*)ptr.c_str() + begin;
      comp_strm.out_sz = MAX_LEN - begin;

      rc = qzCompressStream(session.get(), &comp_strm, last);
      if (rc != QZ_OK) {
        derr << "Compression error: compress return qzCompressStream ERROR("
              << rc << ")" << dendl;
        qzEndStream(session.get(), &comp_strm);
        return -1;
      }
      consumed += comp_strm.in_sz;
      input_left -= comp_strm.in_sz;

      have = comp_strm.out_sz + begin;
      if (begin) {
        // put a compressor variation mark in front of compressed stream, not used at the moment
        ptr.c_str()[0] = 0;
        begin = 0;
      }
      out.append(ptr, 0, have);
    } while (!(1 == last && 0 == comp_strm.pending_in && 0 == comp_strm.pending_out));
  }
  qzEndStream(session.get(), &comp_strm);
  return 0;
}

int QatAccel::decompress(const bufferlist &in, bufferlist &out, boost::optional<int32_t> compressor_message) {
  auto i = in.begin();
  return decompress(i, in.length(), out, compressor_message);
}

int QatAccel::decompress(bufferlist::const_iterator &p,
		 size_t compressed_len,
		 bufferlist &out,
		 boost::optional<int32_t> compressor_message) {
  dout(10) << "fenghl in decompress" << dendl;
  int begin = 1;
  auto s = get_session(); // get a session from the pool
  if (!s) {
    return -1; // session initialization failed
  }
  auto session = cached_session_t{this, std::move(s)}; // returns to the session pool on destruction

  int rc = 0;
  unsigned have;
  unsigned int input_left = 0;
  unsigned int orig_sz = 0;
  unsigned int consumed = 0;
  bufferlist tmp;
  size_t remaining = std::min<size_t>(p.get_remaining(), compressed_len);

  QzStream_T comp_strm = {0};
  unsigned int last = 0;

  while (remaining) {
    const char* c_in = nullptr;
    unsigned int len = p.get_ptr_and_advance(remaining, &c_in);
    remaining -= len;
    dout(10) << "fenghl remaining" << remaining << dendl;
    orig_sz = input_left = len;
    input_left -= begin;
    begin = 0;
    do {
      bufferptr ptr = buffer::create_small_page_aligned(MAX_LEN);
      comp_strm.in = reinterpret_cast<unsigned char*>(const_cast<char*>(c_in)) + orig_sz - input_left;
      comp_strm.in_sz = (input_left > slice_sz) ? slice_sz : input_left;
      comp_strm.out = (unsigned char*)ptr.c_str();
      comp_strm.out_sz = MAX_LEN;
      last = ((remaining == 0 && input_left == 0) ? 1 : 0);


      rc = qzDecompressStream(session.get(), &comp_strm, last);
      if (rc != QZ_OK) {
        derr << "ERROR: Decompression FAILED with return value: "
            << rc << dendl;
        qzEndStream(session.get(), &comp_strm);
        return -1;
      }
      consumed += comp_strm.in_sz;
      input_left -= comp_strm.in_sz;
      have = comp_strm.out_sz;
      out.append(ptr, 0, have);
      dout(10) << "fenghl pending_in:" << comp_strm.pending_in
               << " pending_out:" << comp_strm.pending_out << dendl;
      dout(10) << "fenghl have:" << have << " last: " << last << dendl;
      dout(10) << "fenghl input_left: " << input_left << dendl;
    } while (1 == last && 0 == comp_strm.pending_in && 0 == comp_strm.pending_out);
  }

  qzEndStream(session.get(), &comp_strm);
  dout(10) << "fenghl out decompress" << dendl;
  return 0;
}
