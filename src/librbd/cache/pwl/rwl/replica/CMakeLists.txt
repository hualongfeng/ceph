set(ceph_rwl_replica_files
  EventOp.cc
  MemoryManager.cc
  Reactor.cc
  Types.cc
  ReplicaClient.cc
  TimerPing.cc
  )

add_library(ceph_rwl_replica_lib STATIC ${ceph_rwl_replica_files})

add_executable(ceph-rwl-replica main.cc)
target_link_libraries(ceph-rwl-replica
  ceph_rwl_replica_lib
  librados
  librbd
  global
  ceph-common
  pmem::pmem
  rpma::rpma
  cls_rbd_client
  libneorados
  )
install(TARGETS ceph-rwl-replica DESTINATION bin)
