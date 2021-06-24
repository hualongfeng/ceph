//
// Created by fenghl on 2021/5/29.
//

#include "MemoryManager.h"
#include <unistd.h>
#include <iostream>
#include <libpmem.h>

#include "common/dout.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rwl_replica
#undef dout_prefix
#define dout_prefix *_dout << "ceph::rwl_repilca::MemoryManager " << ": " << this << " " \
                           << __func__ << ": "

namespace librbd::cache::pwl::rwl::replica {

MemoryManager::MemoryManager(CephContext *cct, uint64_t size, std::string path)
  : _size(size), _path(path), _cct(cct) {
  ldout(_cct, 10) << "path: " << _path << dendl;
  _size = size;
  _data = get_memory_from_pmem(path);
  if (_data != nullptr) {
    _is_pmem = true;
  }
}

void MemoryManager::init(uint64_t size, std::string path) {
  _path = path;
  ldout(_cct, 10) << "path: " << _path << dendl;
  _size = size;
  _data = get_memory_from_pmem(_path);
  if (_data != nullptr) {
    _is_pmem = true;
  }
}

int MemoryManager::close_and_remove() {
  ldout(_cct, 20) << dendl;
  if (_data == nullptr) {
    return 0;
  }
  if (_is_pmem) {
    pmem_unmap(_data, _size);
  } else {
    free(_data);
  }
  _data = nullptr;
  return remove(_path.c_str());
}

MemoryManager::~MemoryManager() {
  ldout(_cct, 20) << dendl;
  if (_data == nullptr) {
    return;
  }
  if (_is_pmem) {
    pmem_unmap(_data, _size);
  } else {
    free(_data);
  }
  _data = nullptr;
}

void* MemoryManager::get_pointer() {
  return _data;
}

void* MemoryManager::get_memory_from_pmem(std::string &path) {
  if (path.empty()) {
    return nullptr;
  }
  size_t len;
  int is_pmem;
  if (access(path.c_str(), F_OK) == 0) {
    _data = pmem_map_file(path.c_str(), 0, 0, 0600, &len, &is_pmem);
  }
  else {
    _data = pmem_map_file(path.c_str(), _size, PMEM_FILE_CREATE, 0600, &len, &is_pmem);
  }
  if (!is_pmem || len != _size || _data == nullptr) {
    if (_data) {
      pmem_unmap(_data, _size);
      _data = nullptr;
    }
  }
  return _data;
}

} //namespace ceph::librbd::cache::pwl::rwl::replica