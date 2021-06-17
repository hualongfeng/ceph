// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_REPLICA_CLIENT_H
#define CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_REPLICA_CLIENT_H

#include <string>
#include <vector>
#include <set>

#include "include/rados/librados.hpp"
#include "Types.h"
#include "Reactor.h"
#include "EventHandler.h"
#include "EventOp.h"

namespace librbd::cache::pwl::rwl::replica {

class ReplicaClient {
  uint64_t _id;
  uint64_t _size;
  uint32_t _copies;

  epoch_t _cache_id;

  using ClientHandlerPtr = std::shared_ptr<ClientHandler>;
  using ReactorPtr = std::shared_ptr<Reactor>;
  struct DaemonInfo {
    uint64_t id;
    std::string rdma_ip;
    std::string rdma_port;
    ClientHandlerPtr client_handler;
  };
  std::vector<DaemonInfo> _daemons;
  std::set<uint64_t> _need_free_daemons;

  librados::Rados rados;
  librados::IoCtx io_ctx;
  CephContext *_cct;
 
  ReactorPtr _reactor;

public:
  ReplicaClient(CephContext *cct, uint64_t size, uint32_t copies) 
    : _size(size), _copies(copies), _cct(cct), 
      _reactor(std::make_shared<Reactor>(cct)) {}
  ~ReplicaClient(){}

  int init_rados();
  int init_ioctx();
  int cache_request();
  int cache_free();
  void disconnect();
  int set_head(void *head_ptr, uint64_t size);
  int replica_init(std::string pool_name, std::string image_name);
  int replica_close();
  int write(size_t offset, size_t len);
  int flush(size_t offset, size_t len);
  
};

}

#endif //CEPH_LIBRBD_CACHE_PWL_RWL_REPLICA_REPLICA_CLIENT_H