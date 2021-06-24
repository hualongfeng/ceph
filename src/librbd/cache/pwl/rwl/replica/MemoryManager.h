//
// Created by fenghl on 2021/5/29.
//

#ifndef RPMA_DEMO_MEMORYMANAGER_H
#define RPMA_DEMO_MEMORYMANAGER_H

#include <string>
#include <inttypes.h>

#include "common/ceph_context.h"
#include "include/rados/librados.hpp"

namespace librbd::cache::pwl::rwl::replica {

class MemoryManager {
public:
  MemoryManager(CephContext *cct, uint64_t size, std::string path);
  MemoryManager(CephContext *cct) : _cct(cct) {}
  ~MemoryManager();
  void init(uint64_t size, std::string path);
  void *get_pointer();
  uint64_t size() {return _size;}
  bool is_pmem() { return _is_pmem;}
  int close_and_remove();
private:
  void *get_memory_from_pmem(std::string &path);

  void *_data{nullptr};
  uint64_t _size;
  bool _is_pmem{false};
  std::string _path;
  CephContext *_cct;
};

} //namespace ceph::librbd::cache::pwl::rwl::replica
#endif //RPMA_DEMO_MEMORYMANAGER_H
