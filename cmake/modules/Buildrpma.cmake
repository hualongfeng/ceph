function(build_rpma)
  include(ExternalProject)
  set(RPMA_SRC "${CMAKE_BINARY_DIR}/src/rpma/src")
  set(RPMA_INCLUDE "${RPMA_SRC}/include")

  ExternalProject_Add(rpma_ext
      GIT_REPOSITORY "https://github.com/pmem/rpma.git"
      GIT_TAG "master"
      GIT_SHALLOW TRUE
      SOURCE_DIR "${CMAKE_BINARY_DIR}/src/rpma"
      BINARY_DIR "${CMAKE_BINARY_DIR}/src/rpma"
      CMAKE_ARGS -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_DOC=OFF
      BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR> --target rpma
      BUILD_BYPRODUCTS "<BINARY_DIR>/src/librpma.so"
      INSTALL_COMMAND "true")

  set(RPMA_LIB "${RPMA_SRC}/")
  # librpma
  add_library(rpma::rpma STATIC IMPORTED)
  add_dependencies(rpma::rpma rpma_ext)
  file(MAKE_DIRECTORY ${RPMA_INCLUDE})
  set_target_properties(rpma::rpma PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${RPMA_INCLUDE}
    IMPORTED_LOCATION "${RPMA_LIB}/librpma.so"
    INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
endfunction()
