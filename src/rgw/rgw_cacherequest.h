// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef RGW_CACHEREQUEST_H
#define RGW_CACHEREQUEST_H

#include <stdlib.h>
#include <aio.h>

#include "include/rados/librados.hpp"
#include "include/Context.h"

#include "rgw_aio.h"
#include "rgw_cache.h"
#define COPY_BUF_SIZE (4 * 1024 * 1024)

class Aio;
struct AioResult;
struct DataCache;

class CacheRequest {
  public:
    std::mutex lock;
    int sequence;
    buffer::list* pbl;
    std::string oid;
    off_t ofs;
    off_t len;
    std::string key;
    off_t read_ofs;
    Context *onack;
    CephContext* cct;
    rgw::AioResult* r = nullptr;
    rgw::Aio* aio = nullptr;
    CacheRequest() : sequence(0), pbl(nullptr), ofs(0), read_ofs(0), len(0){};
    virtual ~CacheRequest(){};
    virtual void release()=0;
    virtual void cancel_io()=0;
    virtual int status()=0;
    virtual void finish()=0;
};

struct L1CacheRequest : public CacheRequest {
  int stat;
  struct aiocb* paiocb;
  L1CacheRequest() :  CacheRequest(), stat(-1), paiocb(nullptr) {}
  ~L1CacheRequest(){}

  int prepare_op(std::string obj_key, bufferlist* bl, int read_len, int ofs, int read_ofs, std::string& cache_location,
                 void(*f)(sigval_t), rgw::Aio* aio, rgw::AioResult* r) {
    this->r = r;
    this->aio = aio;
    this->pbl = bl;
    this->ofs = ofs;
    this->key = obj_key;
    this->len = read_len;
    this->stat = EINPROGRESS;
    std::string location = cache_location + obj_key;
    struct aiocb* cb = new struct aiocb;
    memset(cb, 0, sizeof(aiocb));
    cb->aio_fildes = ::open(location.c_str(), O_RDONLY);
    if (cb->aio_fildes < 0) {
      return -1;
    }

    cb->aio_buf = malloc(read_len);
    cb->aio_nbytes = read_len;
    cb->aio_offset = read_ofs;
    cb->aio_sigevent.sigev_notify = SIGEV_THREAD;
    cb->aio_sigevent.sigev_notify_function = f;
    cb->aio_sigevent.sigev_notify_attributes = NULL;
    cb->aio_sigevent.sigev_value.sival_ptr = this;
    this->paiocb = cb;
    return 0;
  }

  void release (){
    lock.lock();
    free((void*)paiocb->aio_buf);
    paiocb->aio_buf = nullptr;
    ::close(paiocb->aio_fildes);
    delete(paiocb);
    lock.unlock();
    delete this;
	}

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  int status(){
    lock.lock();
    if (stat != EINPROGRESS) {
      lock.unlock();
      if (stat == ECANCELED){
        release();
        return ECANCELED;
      }
    }
    stat = aio_error(paiocb);
    lock.unlock();
    return stat;
  }

  void finish(){
    pbl->append((char*)paiocb->aio_buf, paiocb->aio_nbytes);
    release();
  }
};

struct L2CacheRequest : public CacheRequest {
  size_t read;
  int stat;
  void *tp;
  string dest;
  L2CacheRequest() : CacheRequest(), read(0), stat(-1) {}
  ~L2CacheRequest(){}
  void release (){
    lock.lock();
    lock.unlock();
  }

  void cancel_io(){
    lock.lock();
    stat = ECANCELED;
    lock.unlock();
  }

  void finish(){
    onack->complete(0);
    release();
  }

  int status(){
    return 0;
  }
};

#endif
