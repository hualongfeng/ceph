#ifndef CEPH_IAAACCEL_H
#define CEPH_IAAACCEL_H

#include <optional>

#include "include/buffer.h"

class IaaAccel {
 public:
  int compress(const bufferlist &in, bufferlist &out, std::optional<int32_t> &compressor_message);
  int decompress(const bufferlist &in, bufferlist &out, std::optional<int32_t> compressor_message);
  int decompress(bufferlist::const_iterator &p, size_t compressed_len, bufferlist &dst, std::optional<int32_t> compressor_message);
};

#endif //CEPH_IAAACCEL_H
