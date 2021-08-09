set(ceph_rwl_replica_files
  EventOp.cc
  MemoryManager.cc
  Reactor.cc
  Types.cc
  ReplicaClient.cc
  TimePing.cc
  )

add_library(ceph_rwl_replica_lib STATIC ${ceph_rwl_replica_files})

add_executable(ceph-rwl-replica-server server.cc)
target_link_libraries(ceph-rwl-replica-server
  ceph_rwl_replica_lib
  librados
  global
  ceph-common
  pmem::pmem
  rpma::rpma
  cls_rbd_client
  libneorados
  )
install(TARGETS ceph-rwl-replica-server DESTINATION bin)

