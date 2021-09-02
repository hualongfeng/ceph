//
// Created by fenghl on 2021/5/29.
//

#include "MemoryManager.h"
#include <unistd.h>
#include <iostream>
#include <libpmem.h>

#include "common/dout.h"
#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "include/rbd/librbd.hpp"
#include "common/errno.h"

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

void MemoryManager::init(RwlCacheInfo&& info) {
  _info = std::move(info);
  std::string cachefile_name("rbd-pwl." + _info.pool_name + "." + _info.image_name + ".pool." + std::to_string(_info.cache_id));
  _path = _cct->_conf->rwl_replica_path + "/" + cachefile_name;
  ldout(_cct, 10) << "path: " << _path << dendl;
  _size = _info.cache_size;
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
    flush_to_osd();
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

int MemoryManager::flush_to_osd() {
  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::RBD rbd;
  librbd::Image image;

  int r = rados.init_with_context(_cct);
  if (r < 0) {
    lderr(_cct) << "replica: couldn't initialize rados!" << dendl;
    return r;
  }

  r = rados.connect();
  if (r < 0) {
    lderr(_cct) << "replica: couldn't connect to the cluster!" << dendl;
    return r;
  }

  r = rados.ioctx_create(_info.pool_name.c_str(), io_ctx);
  if (r < 0) {
    lderr(_cct) << "replica: error opening pool '" << _info.pool_name << "': "
              << cpp_strerror(r) << dendl;
  }

  r = rbd.open(io_ctx, image, _info.image_name.c_str());
  if (r < 0) {
    lderr(_cct) << "replica: error opening image " << _info.image_name << ": "
              << cpp_strerror(r) << dendl;
    return r;
  }

  r = image.flush();
  if (r < 0) {
    lderr(_cct) << "replica: failed to flush at the end: " << cpp_strerror(r)
                << dendl;
    return r;
  }

  r = image.close();
  if (r < 0) {
    lderr(_cct) << "replica: failed to close the image: " << cpp_strerror(r)
                << dendl;
    return r;
  }
  return 0;
}

} //namespace ceph::librbd::cache::pwl::rwl::replica