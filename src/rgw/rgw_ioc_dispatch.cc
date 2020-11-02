// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "rgw_ioc_dispatch.h"

using namespace ceph::immutable_obj_cache;


void IOChook::init(Context* on_finish) {
  std::unique_lock locker{m_lock};
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
//                lderr(cct) << "IOC hook fail to register client." << dendl;
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
//                lderr(cct) << "IOC hook fail to connect to IOC daemon" << dendl;
      register_ctx->complete(0);
      return;
    }

//            ldout(cct, 20) << "IOC hook connected to IOC daemon." << dendl;

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
//        ldout(cct, 20) << "registering client" << dendl;

  if(!reg) {
//            lderr(cct) << "IOC hook register fails." << dendl;
  }
  return 0;
}

bool IOChook::read(std::string oid, std::string pool_nspace, uint64_t pool_id) { 

  /* if IOC daemon still don't startup, or IOC daemon crash,
     or session occur any error, try to re-connect daemon. */
  std::unique_lock locker{m_lock};
  if(!m_cache_client->is_session_work()) {
    create_cache_session(nullptr, true);
//            ldout(cct, 5) << "IOC hook try to re-connect to IOC daemon." << dendl;
    return false;
  }

  CacheGenContextURef gen_ctx = make_gen_lambda_context<ObjectCacheRequest*,
                                            std::function<void(ObjectCacheRequest*)>>
            ([this](ObjectCacheRequest* ack) { 
    handle_read_cache(ack);
  });

	ceph_assert(m_cache_client != nullptr);
  m_cache_client->lookup_object(pool_nspace, 
                                pool_id,
                                CEPH_NOSNAP,
                                oid,
                                std::move(gen_ctx));
        
  return true;
}

void IOChook::handle_read_cache(ObjectCacheRequest* ack) {
  if(ack->type != RBDSC_READ_REPLY) {
            //go back to read rados
            // TODO: need return a result;
    return;
  }

  ceph_assert(ack->type == RBDSC_READ_REPLY);
  std::string file_path = ((ObjectCacheReadReplyData*)ack)->cache_path;
	
  lsubdout(g_ceph_context, rgw, 1) << " rgw_hook: file path: " << file_path << dendl;
       // ldout(cct, 5) << " rgw_hook: file path: " << file_path << dendl;
}