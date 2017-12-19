cmake_minimum_required(VERSION 3.6)
project(astroDB_cache)

set(CMAKE_CXX_STANDARD 11)

set(ACL_ROOT_PATH "${PROJECT_SOURCE_DIR}/lib/acl/")

set(ASTRO_ROOT_PATH "${PROJECT_SOURCE_DIR}/lib/astrometry.net-0.38")

set(CFITSIO_ROOT_PATH "${PROJECT_SOURCE_DIR}/lib/cfitsio")

set(WCSTOOLS_ROOT_PATH "${PROJECT_SOURCE_DIR}/lib/wcstools-3.8.5")

set(THREADPOOL_ROOT_PATH "${PROJECT_SOURCE_DIR}/lib/C-Thread-Pool-master")

# include directories
INCLUDE_DIRECTORIES(
        ${PROJECT_SOURCE_DIR}/include
        ${ACL_ROOT_PATH}/lib_acl_cpp/include
        ${ASTRO_ROOT_PATH}/include
        ${CFITSIO_ROOT_PATH}/include
        ${WCSTOOLS_ROOT_PATH}/include
        ${THREADPOOL_ROOT_PATH}
)

link_directories(${ACL_ROOT_PATH}/lib_acl_cpp/lib)

link_directories(${ACL_ROOT_PATH}/lib_protocol/lib)

link_directories(${ACL_ROOT_PATH}/lib_acl/lib)

link_directories(${ASTRO_ROOT_PATH}/lib)

link_directories(${CFITSIO_ROOT_PATH}/lib)

link_directories(${WCSTOOLS_ROOT_PATH}/lib)

link_directories(${PROJECT_BINARY_DIR}/lib)

link_directories(${THREADPOOL_ROOT_PATH})

SET(EXECUTABLE_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
SET(LIBRARY_OUTPUT_PATH ${PROJECT_BINARY_DIR}/lib)

add_subdirectory(src bin)