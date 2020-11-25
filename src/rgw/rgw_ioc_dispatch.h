// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_IOC_DISPATCH_H
#define CEPH_RGW_IOC_DISPATCH_H

#include "tools/immutable_object_cache/CacheClient.h"
#include "tools/immutable_object_cache/Types.h"
#include "tools/immutable_object_cache/SocketCommon.h"

#include "rgw_rados.h"

#include "rgw_aio.h"

#include "common/async/yield_context.h"

class IOChook {

public:
  IOChook()
    : m_lock(ceph::make_mutex("rgw::cache::IOChook:session", true, false)), 
    m_put_lock(ceph::make_mutex("rgw::cache::IOChook:put", true, false)),
    m_cache_client(nullptr),
    m_connecting(false) {}
    
  ~IOChook() { 
    delete m_cache_client;
    m_cache_client = nullptr;
  }

  void init(CephContext* _cct, Context* on_finish = nullptr);

  void create_cache_session(Context *on_finish, bool is_reconnect);

  int handle_register_client(bool reg);

  template <typename ExectionContext, typename CompletionToken>
  void async_read(ExectionContext& ctx, std::string oid, std::string pool_ns, int64_t pool_id,
                   off_t read_ofs, off_t read_len, 
                   rgw::Aio::OpFunc&& radosread, rgw::Aio* aio, rgw::AioResult& r,
                   CompletionToken&& token
                  );

  bool handle_read_cache(ceph::immutable_obj_cache::ObjectCacheRequest* ack, std::string oid,
                                off_t read_ofs, off_t read_len, 
                                rgw::Aio::OpFunc&& radosread, rgw::Aio* aio, rgw::AioResult& r
                        );
  
  int read_object(const std::string &file_path, bufferlist* read_data, uint64_t offset,
  uint64_t length);


public:
  ceph::immutable_obj_cache::CacheClient* get_cache_client() { return m_cache_client;}

private:
  CephContext *cct;

  ceph::mutex m_lock;
  ceph::mutex m_put_lock;

  ceph::immutable_obj_cache::CacheClient* m_cache_client = nullptr;
  bool m_connecting = false;
};


template <typename ExectionContext, typename CompletionToken>
void IOChook::async_read(ExectionContext& ctx, std::string oid, std::string pool_ns, int64_t pool_id,
                   off_t read_ofs, off_t read_len, 
                   rgw::Aio::OpFunc&& radosread, rgw::Aio* aio, rgw::AioResult& r,
                   CompletionToken&& token
                  ) { 
  using Signature = void(boost::system::error_code);
  /* if IOC daemon still don't startup, or IOC daemon crash,
     or session occur any error, try to re-connect daemon. */
  std::unique_lock locker{m_lock};
  if(!m_cache_client->is_session_work()) {
    // go to read from rados first
    lsubdout(g_ceph_context, rgw, 20) << "rgw_hook: session didn't work, go to rados" << oid << dendl;
    std::move(radosread)(aio,r);
    // then try to conect to IOC daemon.
    lsubdout(g_ceph_context, rgw, 5) << "rgw_hook: IOC hook try to re-connect to IOC daemon." << dendl;
    create_cache_session(nullptr, true);
    return;
  }
  using namespace ceph::immutable_obj_cache;
  boost::asio::async_completion<CompletionToken, Signature> init(token);
  auto work = boost::asio::make_work_guard(init.completion_handler, ctx.get_executor());
  CacheGenContextURef gen_ctx = make_gen_lambda_context<ObjectCacheRequest*,
                                            std::function<void(ObjectCacheRequest*)>>
            ([this, w=std::move(work), oid, read_ofs, read_len, 
            radosread=std::move(radosread), aio, &r, handler=std::move(init.completion_handler)](ObjectCacheRequest* ack) mutable { 
    
    bool succeed = handle_read_cache(ack, oid, read_ofs, read_len, std::move(radosread), aio, r);
    using namespace boost::asio;
    if(succeed) {
      boost::asio::post(w.get_executor(), [handler=std::move(handler)]() mutable
      {
        handler(boost::system::error_code());
      });
    }

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
  init.result.get();
  return;
}

template <class T>
class IOCRGWDataCache : public T
{

  IOChook ioc_hook;

public:
  IOCRGWDataCache() {}

  int init_rados() override {
    int ret;
    ret = T::init_rados();
    if (ret < 0)
      return ret;
    ioc_hook.init(T::cct);
    lsubdout(g_ceph_context, rgw, 4) << "rgw hook init " << dendl;
    return 0;
  }

  int get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                         off_t read_ofs, off_t len, bool is_head_obj,
                         RGWObjState *astate, void *arg) override;
};


template<class T>
int IOCRGWDataCache<T>::get_obj_iterate_cb(const rgw_raw_obj& read_obj, off_t obj_ofs,
                                 off_t read_ofs, off_t len, bool is_head_obj,
                                 RGWObjState *astate, void *arg) {

  librados::ObjectReadOperation op;
  struct get_obj_data* d = static_cast<struct get_obj_data*>(arg);
  string oid, key;

  int r = 0;

  if (is_head_obj) {
    // only when reading from the head object do we need to do the atomic test
    r = T::append_atomic_test(astate, op);
    if (r < 0)
      return r;

    if (astate && obj_ofs < astate->data.length()) {
      unsigned chunk_len = std::min((uint64_t)astate->data.length() - obj_ofs, (uint64_t)len);

      r = d->client_cb->handle_data(astate->data, obj_ofs, chunk_len);
      if (r < 0)
        return r;

      len -= chunk_len;
      d->offset += chunk_len;
      read_ofs += chunk_len;
      obj_ofs += chunk_len;
      if (!len)
        return 0;
    }
  }

  auto obj = d->store->svc.rados->obj(read_obj);
  r = obj.open();
  if (r < 0) {
    lsubdout(g_ceph_context, rgw, 4) << "failed to open rados context for " << read_obj << dendl;
    return r;
  }


  lsubdout(g_ceph_context, rgw, 20) << "rados->get_obj_iterate_cb oid=" << read_obj.oid << " obj-ofs=" << obj_ofs << " read_ofs=" << read_ofs << " len=" << len << dendl;
  op.read(read_ofs, len, nullptr, nullptr);

  const uint64_t cost = len;
  const uint64_t id = obj_ofs; // use logical object offset for sorting replies

  auto completed = d->aio->get(obj, rgw::Aio::ioc_cache_op(std::move(op), d->yield, read_ofs, len, &ioc_hook), cost, id);
  return d->flush(std::move(completed));

}

#endif