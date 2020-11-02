// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_IOC_DISPATCH_H
#define CEPH_RGW_IOC_DISPATCH_H

#include "tools/immutable_object_cache/CacheClient.h"
#include "tools/immutable_object_cache/Types.h"
#include "tools/immutable_object_cache/SocketCommon.h"

class IOChook {
//    using ObjectCacheReadReplyData = ceph::immutable_obj_cache::ObjectCacheReadReplyData;
//    using ObjectCacheRequest = ceph::immutable_obj_cache::ObjectCacheRequest;
//    using RBDSC_READ_REPLY = ceph::immutable_obj_cache::RBDSC_READ_REPLY;
//    using CacheClient = ceph::immutable_obj_cache::CacheClient;


public:
  IOChook(CephContext* _cct)
    : cct(_cct), m_lock(ceph::make_mutex("rgw::cache::IOChook", true, false)) {
            ceph_assert(cct != nullptr);
            auto controller_path = cct->_conf.template get_val<std::string>(
                "immutable_object_cache_sock");
            m_cache_client = new ceph::immutable_obj_cache::CacheClient(controller_path.c_str(), cct);
  }
    
  ~IOChook() { 
    delete m_cache_client;
    m_cache_client = nullptr;
  }

  void init(Context* on_finish = nullptr);

  void create_cache_session(Context *on_finish, bool is_reconnect);

  int handle_register_client(bool reg);

  bool read(std::string oid, std::string pool_nspace, uint64_t pool_id);

  void handle_read_cache(ceph::immutable_obj_cache::ObjectCacheRequest* ack);


public:
  ceph::immutable_obj_cache::CacheClient* get_cache_client() { return m_cache_client;}

private:
  CephContext *cct;

  ceph::mutex m_lock;

  ceph::immutable_obj_cache::CacheClient* m_cache_client = nullptr;
  bool m_connecting = false;
};
#endif