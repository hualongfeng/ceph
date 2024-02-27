# - Find qatseqprod
# Find the qatseqprod compression library and includes
#
# qatseqprod_INCLUDE_DIR - where to find qatzip.h, etc.
# qatseqprod_LIBRARIES - List of libraries when using qatzip.
# qatseqprod_FOUND - True if qatzip found.

find_path(qatseqprod_INCLUDE_DIR NAMES qatseqprod.h)
#find_path(qatseqprod_INCLUDE_DIR NAMES qatseqprod.h HINTS /root/QAT-ZSTD-Plugin/src)
find_library(qatseqprod_LIBRARIES NAMES qatseqprod)
#find_library(qatseqprod_LIBRARIES NAMES qatseqprod HINTS /root/QAT-ZSTD-Plugin/src)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(qatseqprod DEFAULT_MSG qatseqprod_LIBRARIES qatseqprod_INCLUDE_DIR)

mark_as_advanced(
  qatseqprod_LIBRARIES
  qatseqprod_INCLUDE_DIR)

if(qatseqprod_FOUND AND NOT TARGET QAT::qatseqprod)
  add_library(QAT::qatseqprod SHARED IMPORTED)
  set_target_properties(QAT::qatseqprod PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${qatseqprod_INCLUDE_DIR}"
    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
    IMPORTED_LOCATION "${qatseqprod_LIBRARIES}")
endif()
