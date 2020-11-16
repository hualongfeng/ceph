// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_ioc_dispatch.h"
#include "rgw_common.h"
#include "algorithm"
#include "aio.h"
#include "rgw_aio.h"
#include "thread"
#include "chrono"

using namespace ceph::immutable_obj_cache;


#define lldout(level) lsubdout(g_ceph_context, rgw, level)

void IOChook::init(CephContext* _cct, Context* on_finish) {
  cct = _cct;
  ceph_assert(cct != nullptr);
  std::unique_lock locker{m_lock};
  if(m_cache_client == nullptr) {
    auto controller_path = cct->_conf.template get_val<std::string>(
      "immutable_object_cache_sock");
    m_cache_client = new ceph::immutable_obj_cache::CacheClient(controller_path.c_str(), cct);
  }
  create_cache_session(on_finish, false);
}

void IOChook::create_cache_session(Context *on_finish, bool is_reconnect) {
//	ceph_assert(ceph_mutex_is_lock_by_me(m_lock));
  if(m_connecting) {
    return;
  }

  m_connecting = true;

  Context* register_ctx = new LambdaContext([this, on_finish](int ret) {
    if(ret < 0) {
      lldout(1) << "rgw_hook: IOC hook fail to register client." << dendl;
    }
    handle_register_client(ret < 0 ? false : true);

    ceph_assert(m_connecting);
    m_connecting = false;

    if(on_finish != nullptr) {
      on_finish->complete(0);
    }
  });

  Context* connect_ctx = new LambdaContext([this, register_ctx](int ret) {
    if(ret < 0) {
      lldout(1)  << "rgw_hook: IOC hook fail to connect to IOC daemon" << dendl;
      register_ctx->complete(0);
      return;
    }

    lldout(1) << "rgw_hook: IOC hook connected to IOC daemon." << dendl;

    m_cache_client->register_client(register_ctx);
  });


	if(m_cache_client != nullptr && is_reconnect) {
    delete m_cache_client;
    auto controller_path = cct->_conf.template get_val<std::string>(
        	"immutable_object_cache_sock");
    m_cache_client = new CacheClient(controller_path.c_str(), cct);
	}

	ceph_assert(m_cache_client != nullptr);

  m_cache_client->run();
  m_cache_client->connect(connect_ctx);
}

int IOChook::handle_register_client(bool reg) {
  lldout(20) << "rgw_hook: registering client" << dendl;

  if(!reg) {
    lldout(1) << "rgw_hook: IOC hook register fails." << dendl;
  }
  return 0;
}

void IOChook::read(std::string oid, std::string pool_ns, int64_t pool_id,
                   off_t read_ofs, off_t read_len, optional_yield y, 
                   rgw::Aio::OpFunc&& radosread, rgw::Aio* aio, rgw::AioResult& r  
                  ) { 

  /* if IOC daemon still don't startup, or IOC daemon crash,
     or session occur any error, try to re-connect daemon. */
  std::unique_lock locker{m_lock};
  if(!m_cache_client->is_session_work()) {
    // go to read from rados first
    lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: session didn't work, go to rados" << oid << dendl;
    std::move(radosread)(aio,r);
    // then try to conect to IOC daemon.
    lldout(5) << "rgw_hook: IOC hook try to re-connect to IOC daemon." << dendl;
    create_cache_session(nullptr, true);
    return;
  }

  CacheGenContextURef gen_ctx = make_gen_lambda_context<ObjectCacheRequest*,
                                            std::function<void(ObjectCacheRequest*)>>
            ([this, oid, read_ofs, read_len, y, radosread=std::move(radosread), aio, &r](ObjectCacheRequest* ack) mutable { 
    handle_read_cache(ack, oid, read_ofs, read_len, y, std::move(radosread), aio, r);

    lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: ending callback" << dendl;
  });

  lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: starting lookup " << dendl;
	ceph_assert(m_cache_client != nullptr);
  m_cache_client->lookup_object(pool_ns, 
                                pool_id,
                                CEPH_NOSNAP,
                                oid,
                                std::move(gen_ctx));
        
  lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: ending lookup" << dendl;
  return;
}

void IOChook::handle_read_cache(ObjectCacheRequest* ack, std::string oid,
                                off_t read_ofs, off_t read_len, optional_yield y,
                                rgw::Aio::OpFunc&& radosread, rgw::Aio* aio, rgw::AioResult& r  
                                ) {
  if(ack->type != RBDSC_READ_REPLY) {
    // go back to read rados
    lsubdout(g_ceph_context, rgw, 1) << "rgw_hook: IOC cache don't cache object, go to rados " << oid << dendl;
    std::move(radosread)(aio,r);
    return;
  }

  ceph_assert(ack->type == RBDSC_READ_REPLY);
  std::string file_path = ((ObjectCacheReadReplyData*)ack)->cache_path;
  if(file_path.empty()) {
    lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: file path is empty, go to rados" << oid << dendl;
    std::move(radosread)(aio,r);
    return;
  }
	
  lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: ensure should found oid: " << oid << dendl;
  lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: ensure file path: " << file_path << dendl;

//  std::move(radosread)(aio,r);
/*
  librados::ObjectReadOperation op;
  op.read(read_ofs, read_len, nullptr, nullptr);
  auto cache_read = rgw::Aio::cache_op(std::move(op), y, 0, read_ofs, read_len, file_path);
  std::move(cache_read)(aio, r);
*/  
//  return;

  int ret = read_object(file_path, &r.data, read_ofs, read_len);

  if(ret < 0) {
    r.data.clear();
    lsubdout(g_ceph_context, rgw, 20) << " rgw_hook: failed to read from ioc cache, go to rados" << dendl;
    std::move(radosread)(aio,r);
    return;
  }


  std::unique_lock locker{m_put_lock};
  r.result = 0;
  aio->put(r);
  return;
  
}

int IOChook::read_object(const std::string &file_path, bufferlist* read_data, uint64_t offset,
                         uint64_t length) {
  lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: file path: " << file_path
                                    << "start to read" << dendl;

  std::string error;
  int ret = read_data->pread_file(file_path.c_str(), offset, length, &error);
  lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: finish to read from ioc cache: " << ret << dendl;
  if(ret < 0) {
    lsubdout(g_ceph_context, rgw, 5) << "read from file return error: " << error
                                     << "file path = " << file_path 
                                     << dendl;
    return ret;
  }
  lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: succeed to read from ioc cache" << dendl;

  return read_data->length();
}