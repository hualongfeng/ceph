#include "IaaAccel.h"
#include <vector>
#include <memory>
#include "qpl/qpl.h"

#include "common/ceph_context.h"
#include "common/common_init.h"
#include "common/debug.h"
#include "common/dout.h"
#include "common/errno.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_compressor
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static std::ostream& _prefix(std::ostream* _dout) {
  return *_dout << "IaaAceel: ";
}

#define MAX_LEN (CEPH_PAGE_SIZE)

// default window size for Zlib 1.2.8, negated for raw deflate
#define ZLIB_DEFAULT_WIN_SIZE -15

// Estimate data expansion after decompression
static const unsigned int expansion_ratio[] = {5, 20, 50, 100, 200, 1000, 10000};


int IaaAccel::compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message) {
  compressor_message = ZLIB_DEFAULT_WIN_SIZE;

  unsigned have;
  unsigned char* c_in;
  int begin = 1;

  qpl_path_t execution_path = qpl_path_software;

  std::unique_ptr<uint8_t[]> job_buffer;
  qpl_status                 status;
  uint32_t                   size = 0;

  status = qpl_get_job_size(execution_path, &size);
  if (status != QPL_STS_OK) {
    dout(1) << "An error acquired during job size getting." << dendl;
    return -1;
  }

  job_buffer = std::make_unique<uint8_t[]>(size);
  qpl_job *job = reinterpret_cast<qpl_job*>(job_buffer.get());
  status = qpl_init_job(execution_path, job);
  if (status != QPL_STS_OK) {
    dout(1) << "An error acuqired during compression job initializing." << dendl;
    return -1;
  }

  job->op = qpl_op_compress;
  job->level = qpl_default_level;
  job->flags = QPL_FLAG_FIRST | QPL_FLAG_DYNAMIC_HUFFMAN | QPL_FLAG_OMIT_VERIFY;

  for (ceph::bufferlist::buffers_t::const_iterator i = in.buffers().begin(); i != in.buffers().end();) {
    c_in = (unsigned char*) (*i).c_str();
    long unsigned int len = (*i).length();
    ++i;

    job->next_in_ptr = c_in;
    job->available_in = len;

    if (i == in.buffers().end()) {
      job->flags |= QPL_FLAG_LAST;
    }

    do {
      bufferptr ptr = ceph::buffer::create_page_aligned(MAX_LEN);
      job->next_out_ptr = (unsigned char*)ptr.c_str() + begin;
      job->available_out = MAX_LEN - begin;

      if (begin) {
        // put a compressor variation mark in front of compressed stream, not used at the moment
        ptr.c_str()[0] = 0;
        begin = 0;
      }

      status = qpl_execute_job(job);
      if (status != QPL_STS_OK) {
        dout(1) << "Error while compression occurred." << dendl;
        qpl_fini_job(job);
        return -1;
      }
      have = MAX_LEN - job->available_out;
      out.append(ptr, 0, have);
    } while (job->available_out == 0);

    if (job->available_in != 0) {
      dout(10) << "Compression error: unused input" << dendl;
      qpl_fini_job(job);
      return -1;
    }
  }

  status = qpl_fini_job(job);
  if (status != QPL_STS_OK) {
    dout(1) << "An error acquired during job finalization." << dendl;
    return -1;
  }

  return 0;
}


int IaaAccel::decompress(const bufferlist &in, bufferlist &out, std::optional<int32_t> compressor_message) {
  auto i = in.begin();
  return decompress(i, in.length(), out, compressor_message);
}


int IaaAccel::decompress(bufferlist::const_iterator &p, size_t compressed_len, bufferlist &dst, std::optional<int32_t> compressor_message) {
  const char* c_in;
  int begin = 1;

  qpl_path_t execution_path = qpl_path_auto;

  std::unique_ptr<uint8_t[]> job_buffer;
  qpl_status                 status;
  uint32_t                   size = 0;

  status = qpl_get_job_size(execution_path, &size);
  if (status != QPL_STS_OK) {
    dout(1) << "An error acquired during job size getting" << dendl;
    return -1;
  }

  job_buffer = std::make_unique<uint8_t[]>(size);
  qpl_job *job = reinterpret_cast<qpl_job*>(job_buffer.get());
  status = qpl_init_job(execution_path, job);
  if (status != QPL_STS_OK) {
    dout(1) << "An error acquired during compression job initializing" << dendl;
    return -1;
  }

  job->op = qpl_op_decompress;
  job->flags = QPL_FLAG_FIRST | QPL_FLAG_LAST;

  size_t remaining = std::min<size_t>(p.get_remaining(), compressed_len);

  while(remaining) {
    unsigned int ratio_idx = 0;
    long unsigned int len = p.get_ptr_and_advance(remaining, &c_in);
    remaining -= len;
    job->available_in = len - begin;
    job->next_in_ptr = (unsigned char*)c_in + begin;
    begin = 0;

    unsigned int out_len = 65536;

    bufferptr ptr;
    do {
      while (out_len <= len * expansion_ratio[ratio_idx]) {
        out_len *= 2;
      }

      ptr = buffer::create_small_page_aligned(out_len);
      job->next_out_ptr = reinterpret_cast<uint8_t*>(ptr.c_str());
      job->available_out = out_len;

      status = qpl_execute_job(job);

    } while (status == QPL_STS_MORE_OUTPUT_NEEDED && ratio_idx < std::size(expansion_ratio));

    if (status == QPL_STS_OK) {
      dst.append(ptr, 0, out_len);
    } else if (status == QPL_STS_MORE_INPUT_NEEDED) {
      dout(1) << "IAA need more input need" << dendl;
      qpl_fini_job(job);
      return -1;
    } else {
      dout(1) << "compressor NOT OK: " << status << dendl;
      qpl_fini_job(job);
      return -1;
    }
  }

  status = qpl_fini_job(job);
  if (status != QPL_STS_OK) {
    dout(1) << "An error acquired during job finalization." << dendl;
  }

  return 0;
}
