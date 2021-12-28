function(build_rpma)
  include(FindMake)

  if(EXISTS "${PROJECT_SOURCE_DIR}/src/rpma/CMakeLists.txt")
    set(source_dir_args
       SOURCE_DIR "${PROJECT_SOURCE_DIR}/src/rpma"
       )
  else()
    set(source_dir_args
	SOURCE_DIR ${CMAKE_BINARY_DIR}/src/rpma
	GIT_REPOSITORY https://github.com/pmem/rpma.git
	GIT_TAG "0.9.0"
	GIT_SHALLOW TRUE
	GIT_CONFIG advice.detachedHead=false
	)
  endif()

  include(ExternalProject)
  ExternalProject_Add(rpma_ext
      ${source_dir_args}
      CMAKE_ARGS -DBUILD_EXAMPLES=OFF -DBUILD_TESTS=OFF -DBUILD_DOC=OFF
      BUILD_COMMAND ${CMAKE_COMMAND} --build <SOURCE_DIR> --target rpma
      BUILD_BYPRODUCTS "<SOURCE_DIR>/src/librpma.so"
      INSTALL_COMMAND "true")

  ExternalProject_Get_Property(rpma_ext source_dir)
  set(RPMA_SRC "${source_dir}/src")
  set(RPMA_INCLUDE "${source_dir}/src/include")
  set(RPMA_LIB "${RPMA_SRC}/")

  # librpma
  add_library(rpma::rpma STATIC IMPORTED GLOBAL)
  add_dependencies(rpma::rpma rpma_ext)
  file(MAKE_DIRECTORY ${RPMA_INCLUDE})
  set_target_properties(rpma::rpma PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES ${RPMA_INCLUDE}
    IMPORTED_LOCATION "${RPMA_LIB}/librpma.so"
    INTERFACE_LINK_LIBRARIES ${CMAKE_THREAD_LIBS_INIT})
endfunction()
