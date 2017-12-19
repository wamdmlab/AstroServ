//
// Created by zuijian Weng on 11/12/16.
//
#include <stdio.h>
#include <stdlib.h>

#include "acl_cpp/lib_acl.hpp"

static int __max_conns = 0;

static void test_redis_string(acl::redis& cmd, const char* key)
{
    acl::string val("test_value");

    // call redis-server: SET key value
    if (cmd.rpush(key, val.c_str(), NULL) == false)
    {
        printf("redis set error\r\n");
        return;
    }

    // clear the string buf space
    val.clear();

    // reset the redis command object for reusing it
    cmd.clear();

    // call redis-server: GET key
    //if (cmd.get(key, val) == false)
    //    printf("get key error\r\n");
}

static void* thread_main(void* arg)
{
    acl::redis_client_cluster* cluster = (acl::redis_client_cluster*) arg;

    acl::redis cmd;
    cmd.set_cluster(cluster, __max_conns);

    const char* key = "test_key";

    for (int i = 0; i < 30000; i++)
    {
        test_redis_string(cmd, key);
        //test_redis_key(cmd, key);
    }

    return NULL;
}

int main1(void)
{
    // init socket module for windows
	struct timeval start, end;
	gettimeofday(&start, NULL);

    acl::acl_cpp_init();

    const char* redis_addr = "192.168.0.90:7001";
    int conn_timeout = 10, rw_timeout = 10;

    // declare redis cluster ojbect
    acl::redis_client_cluster cluster;
    cluster.set(redis_addr, __max_conns, conn_timeout, rw_timeout);

    pthread_attr_t attr;
    pthread_attr_init(&attr);

    // create first thread
    pthread_t id1;
    pthread_create(&id1, &attr, thread_main, &cluster);

    // create second thread
    pthread_t id2;
    pthread_create(&id2, &attr, thread_main, &cluster);

    // create second thread
	pthread_t id3;
	pthread_create(&id3, &attr, thread_main, &cluster);

	// create second thread
	pthread_t id4;
	pthread_create(&id4, &attr, thread_main, &cluster);

	// create second thread
	pthread_t id5;
	pthread_create(&id5, &attr, thread_main, &cluster);

	// create second thread
	pthread_t id6;
	pthread_create(&id6, &attr, thread_main, &cluster);

    pthread_join(id1, NULL);
    pthread_join(id2, NULL);
    pthread_join(id3, NULL);
    pthread_join(id4, NULL);
    pthread_join(id5, NULL);
    pthread_join(id6, NULL);

    gettimeofday(&end, NULL);
	float time_use=((end.tv_sec-start.tv_sec)*1000000+
			(end.tv_usec-start.tv_usec)) / 1000000;//
	printf("total redis test time is: %fs\n", time_use);

    return 0;
}
