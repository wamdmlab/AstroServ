PROJECT(ttiger)

MESSAGE(STATUS "operation system is ${CMAKE_SYSTEM}")

aux_source_directory(./crossMatch CROSSMATCH_FILES)

aux_source_directory(./intoRedis INTOREDIS_FILES)

aux_source_directory(./storeSourceData STORESOURCE_FILES)

ADD_LIBRARY(cross STATIC ${CROSSMATCH_FILES})
target_link_libraries(cross -lcfitsio -lanutils -lwcs -lbackend)
#SET_TARGET_PROPERTIES(cross PROPERTIES OUTPUT_NAME "libcross")

ADD_LIBRARY(intoredis STATIC ${INTOREDIS_FILES})
target_link_libraries(intoredis -l_acl_cpp -l_protocol -l_acl -lthreadpool -lpthread)
#SET_TARGET_PROPERTIES(intoredis PROPERTIES OUTPUT_NAME "libintoredis")

ADD_LIBRARY(storesource STATIC ${STORESOURCE_FILES} storeSourceData/storeIntoLocalDisk.cpp)
target_link_libraries(storesource -lthreadpool -lpthread)

add_executable(astroDB_cache main.cpp ../include/randAbStar.h storeSourceData/storeIntoLocalDisk.cpp ../include/storeSourceData.h)

target_link_libraries(astroDB_cache intoredis cross storesource)