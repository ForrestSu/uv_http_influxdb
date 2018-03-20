#ifndef _RECV_QUOTE_H
#define _RECV_QUOTE_H

#include <cstdlib>
#include <iostream>
#include <cstdio>
#include <string>
#include "uv.h"
#include <memory>
#include <mutex>
#include <queue>
#include "recordtime.h"
#include "zmq.hpp"
#include <sys/syscall.h>
#include <sys/types.h>
#define gettid() syscall(SYS_gettid)

using namespace std;


typedef struct udp_msg_tag{
    int len;
    struct udp_msg_tag *next;
    char data[1];
}udp_msg;

//snapshot/tick msg
typedef struct marketdata_task_tag{
	int file_hdl; //file
	void* udp_hdl;
	void* udp_addr;
	udp_msg * udp_msg_ptr;
	//
	int32_t total; //total msg packs
	int32_t used_bytes;
	char data[1];
}marketdata_task;

class Consumer;
class MarketProvider{

public:
    MarketProvider(uv_loop_t *loop, const string& saddr, const string& topic, const char* filename, Consumer* pwork);
    ~MarketProvider();
    bool Start();
    bool Stop();
    static void thread_func(void *pParam);
private:
    uv_loop_t * m_loop;
    bool m_run;
    std::string m_saddr;
    std::string m_topic;
    //zmq
    zmq::context_t m_ctx;
    std::shared_ptr<zmq::socket_t> m_socket;
    uv_thread_t m_tid;
    Consumer* m_work;
    //file
    int m_filehdl;
    //udp

};

class Consumer {
public:
	Consumer(uv_loop_t* loop);
	~Consumer();
	bool InitUDP(const string& udp_addr, int port);
	//call by another thread
	int SendAsync(marketdata_task* data);
private:
	static void OnAsync(uv_async_t* handle);
private:
	uv_loop_t  * m_loop;
	uv_async_t * m_async_hdl;
	uv_mutex_t m_mtx;
	vector<marketdata_task*> m_tasks;
	//udp optional
	struct sockaddr_in *m_udp_addr;
	uv_udp_t * m_udp_hdl;
};


#endif


