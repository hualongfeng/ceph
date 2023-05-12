function(build_qpl)
  include(ExternalProject)

  set(source_dir ${CMAKE_BINARY_DIR}/src/qpl)
  file(MAKE_DIRECTORY ${source_dir})
# set(qpl_INSTALL_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/qpl")

  ExternalProject_Add(qpl_ext
    UPDATE_COMMAND "" # this disables rebuild on each run
    GIT_REPOSITORY "https://github.com/intel/qpl.git"
    GIT_CONFIG advice.detachedHead=false
    GIT_SHALLOW TRUE
    GIT_TAG "v1.1.0"
    SOURCE_DIR ${source_dir}
    CMAKE_ARGS
      -DCMAKE_BUILD_TYPE=Release
    BINARY_DIR ${source_dir}
    BUILD_COMMAND ${CMAKE_COMMAND} --build <BINARY_DIR>
    INSTALL_COMMAND ""
#   INSTALL_COMMAND "${CMAKE_COMMAND} --build <BINARY_DIR> --target install"
    BUILD_BYPRODUCTS ${source_dir}/sources/c_api/libqpl.a
    )

# ExternalProject_Get_Property(qpl_ext source_dir)

set(qpl_LIBRARIES "${source_dir}/sources/c_api/libqpl.a")

add_library(qpl::qpl STATIC IMPORTED)
add_dependencies(qpl::qpl qpl_ext)
file(MAKE_DIRECTORY "${source_dir}/include")
set_target_properties(qpl::qpl PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${source_dir}/include"
#   INTERFACE_LINK_LIBRARIES "qpl"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${source_dir}/sources/c_api/libqpl.a")

endfunction()
