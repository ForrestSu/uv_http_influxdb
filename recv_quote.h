#ifndef _RECV_QUOTE_H
#define _RECV_QUOTE_H

#include <cstdlib>
#include <iostream>
#include <cstdio>
#include <string>
#include <vector>
#include <queue>
#include <memory>
#include <mutex>
#include <queue>
#include "zmq.hpp"
#include "uv.h"
#include "recordtime.h"
#include <sys/syscall.h>
#include <sys/types.h>
#define gettid() syscall(SYS_gettid)

enum EConnStatus{
    Disconn = -1,
    Avail = 0,
    Sending = 1,
    Recving = 2,
};
typedef struct tcp_msg_tag{
    void* pself; //save TcpClient pointer
    int32_t capcity; // alloc total size
    int32_t size; // used_bytes
    struct tcp_msg_tag *next;
    char data[1];
}tcp_msg;

//snapshot/tick msg
typedef struct marketdata_task_tag{
    void* pself;
	int file_hdl; //file
	char* tcp_header;
	int tcp_header_len;
	int tcp_dollar_pos; // dollar position
	tcp_msg * udp_msg_ptr;
    //
	int32_t total_packs; //total msg packs
	int32_t used_bytes;
	char data[1];
}marketdata_task;


class Consumer;
class MarketProvider{

public:
    MarketProvider(uv_loop_t *loop, const std::string& saddr, const std::string& topic, const char* filename, Consumer* pwork);
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

class TcpClient{
public:
    TcpClient(uv_loop_t * loop, const std::string& tcp_addr, int port, int id=0);
    ~TcpClient();
    bool Reconnect();
private:
    uv_loop_t * m_loop;
    struct sockaddr_in* m_tcpaddr;
    static void on_tcp_connect(uv_connect_t* req, int status);
public:
    int m_id;
    uv_tcp_t  * m_socket;
    uv_connect_t *m_connect;
    int m_state; // -1 unavailable ;0 is using; 1 available
};


class Consumer {
public:
	Consumer(uv_loop_t* loop);
	~Consumer();
	bool InitTCP(const std::string& tcp_addr, const std::string& dbname, int port, int cnt);
	//call by another thread
	int SendAsync(marketdata_task* data);
private:
	static void OnAsync(uv_async_t* handle);
private:
	uv_loop_t  * m_loop;
	uv_mutex_t m_mtx;
	uv_async_t * m_async_hdl;
	std::vector<marketdata_task*> m_tasks;
	//tcp header
	char *m_header;
	int m_headerlen;
	int m_dollarpos;
public:
	int m_index;
	std::vector<std::shared_ptr<TcpClient>> m_tcpConn;
};




#endif


