#include "recv_quote.h"

#include <unistd.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <fcntl.h>
#include <stddef.h>

#include "../../emsproto/quote.pb.h"

static void Job(uv_work_t *req);
static void JobDone(uv_work_t *req, int status);
void del_async_cb(uv_handle_t* handle);
void on_write_done(uv_fs_t *req);
void on_udp_send(uv_udp_send_t *req, int status);
// create an udp_msg node
udp_msg * get_udp_node(const string* msgsrc);

const char format_snap[] = "market,date=%d,exch=%d,code=%s status=%d,lastprice=%.2f,prevclose=%.2f,open=%.2f,high=%.2f,low=%.2f,volume=%lld,value=%.2f,highlimited=%.2f,lowlimited=%.2f,niopv=%.4f,numtrades=%d,totalBidVol=%lld,totalAskVol=%lld";

const char format_tick[] = "tick,trade_date=%d,exch=%d,code=%s price=%.2f,nIndex=%d,volume=%lld,turnover=%.2f,nBSFlag=%d,chOrderKind=%d,chFunctionCode=%d,nAskOrder=%d,nBidOrder=%d %lld%06d\n";

const int MAX_MSGBUFF_SIZE = 32 * 1024 * 1024; //64M
const int MAX_UDPMSG_SIZE =  1300 ; // 8K

///debug
int32_t TOTAL_REDUCE_UDP_PACK = 0;
int32_t TOTAL_FREE_UDP_PACK = 0;

static int64_t GetDateSecond(int idate)
{
	struct tm datetime = {0};
	char temp[12];
    sprintf(temp,"%08d",idate);
    sscanf(temp,"%04d%02d%02d",&(datetime.tm_year),
    		&(datetime.tm_mon),&(datetime.tm_mday));
	datetime.tm_mon -= 1; //0-11
	datetime.tm_year -= 1900; //1900
	time_t datesecond = mktime(&datetime);
	return ((int64_t)datesecond);
}

MarketProvider::MarketProvider(uv_loop_t *loop, const string& saddr, const string& topic, const char* filename, Consumer* pwork)
{
	m_loop = loop;
    m_run = false;
    m_saddr = saddr;
    m_topic = topic;
    // init zmq
    m_ctx = zmq::context_t(1);
    m_socket = make_shared<zmq::socket_t>(m_ctx, ZMQ_SUB);
    m_tid = 0 ;
    m_work = pwork;

    m_socket->connect(m_saddr);
    m_socket->setsockopt(ZMQ_SUBSCRIBE, topic.c_str(), topic.length());
    int timeout = 1000; //milliseconds
    m_socket->setsockopt(ZMQ_RCVTIMEO, &timeout, sizeof(timeout));
    printf("connect: [%s],  topic: [%s]! set timeout %d ms.\n", m_saddr.c_str(), topic.c_str(), timeout);

    uv_fs_t req;
    m_filehdl = uv_fs_open(m_loop, &req, filename, O_CREAT | O_TRUNC | O_WRONLY, 0644, nullptr);
    uv_fs_req_cleanup(&req);
}

MarketProvider::~MarketProvider()
{
    m_socket->close();
}

void MarketProvider::thread_func(void *pParam)
{
	MarketProvider *self = (MarketProvider*) pParam;
	printf("thread_func()\n");
	zmq::message_t msg;

	RecordTimeMs timems;
	timems.start();
	marketdata_task* ptask = nullptr;
	int failcnt = 0;
	int totalmsg = 0 ;
    while(self->m_run)
    {
        //recv topic
		if((self->m_socket->recv(&msg)) == false)
		{
			//printf("recv EAGAIN!\n");
			if (ptask == nullptr) {
			    printf(">>> TOTAL_REDUCE_UDP_PACK: %d ,TOTAL_FREE_UDP_PACK: %d .\n", TOTAL_REDUCE_UDP_PACK, TOTAL_FREE_UDP_PACK);
				usleep(1000000); //1s
				continue;
			}
			++failcnt;
			if (failcnt > 4) {
				printf("[%s]Continuous timeout %d times! send last %d packs.\n"
						,self->m_topic.c_str(), failcnt, ptask->total);
				self->m_work->SendAsync(ptask);
				ptask = nullptr;
				failcnt = 0;
			}
			continue;
		}else{
		    failcnt = 0;
		}
        if(msg.more())
        {
            self->m_socket->recv(&msg);
            // string quote = new string((char*)msg.data(), msg.size());
            if( ptask == nullptr)
            {
            	ptask = (marketdata_task*) malloc(MAX_MSGBUFF_SIZE);
            	//memset(task->data, 0, sizeof(M_BUFF_SIZE));
            	ptask->file_hdl = self->m_filehdl;
            	ptask->udp_hdl = nullptr;
            	ptask->udp_addr = nullptr;
            	ptask->udp_msg_ptr = nullptr;
            	ptask->total = 0;
            	ptask->used_bytes = 0;
            }

            int32_t msg_len =  msg.size();
            memcpy( (ptask->data + ptask->used_bytes), &msg_len, sizeof(msg_len)); //4 byte
            ptask->used_bytes += sizeof(msg_len);
            memcpy( (ptask->data + ptask->used_bytes), msg.data(), msg.size()); // msg
            ptask->used_bytes += msg_len;
            ptask->total++;
            //if full, send msg to another thread
            if( (MAX_MSGBUFF_SIZE - ptask->used_bytes) < 20 * 1024)
            {
            	if(self->m_work->SendAsync(ptask) )
            	    printf("error: fail to  send async !\n");
            	double cost = timems.stop();
            	printf("Recv speed: %.2f byte/ms, %d packs!  %.2f packs/ms.\n", ptask->used_bytes/cost, ptask->total, ptask->total/cost);
            	timems.start();
            	ptask = nullptr;
            }
        }
        //usleep(200 * 1000);// 200 ms
    }
}
bool MarketProvider::Start()
{
	m_run = true;
	uv_thread_create(&m_tid, MarketProvider::thread_func, (void*)this);
    return true;
}

bool MarketProvider::Stop()
{
	m_run = false;
	uv_thread_join(&m_tid);
	return true;
}

Consumer::Consumer(uv_loop_t* loop)
{
	m_loop = loop;
	uv_mutex_init(&m_mtx);

	m_async_hdl = (uv_async_t*)malloc(sizeof(uv_async_t));
    uv_async_init(this->m_loop, m_async_hdl, Consumer::OnAsync);

    // optional
    m_udp_addr = nullptr;
    m_udp_hdl = nullptr;
}
bool Consumer::InitUDP(const string& udp_addr, int port)
{
    m_udp_addr = (struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));
    uv_ip4_addr(udp_addr.c_str(), port, m_udp_addr);

    m_udp_hdl = (uv_udp_t*)malloc(sizeof(uv_udp_t));
    uv_udp_init(m_loop, m_udp_hdl);
    return true;
}

Consumer::~Consumer()
{
    uv_mutex_destroy(&m_mtx);
    //write a release function by ourself.
    uv_close((uv_handle_t*)m_async_hdl, del_async_cb);
    if(m_udp_addr){
        free(m_udp_addr);
    }
    if(m_udp_hdl){
        free(m_udp_hdl);
    }
}

void del_async_cb(uv_handle_t* handle)
{
    free(handle);
}

void on_write_done(uv_fs_t *req) {
    if (req) {
        uv_fs_req_cleanup(req);
        if (req->data) {
            //printf("release!  ");
            free(req->data);
        }
        free(req);
        ++TOTAL_FREE_UDP_PACK;
    }
}

void on_udp_send(uv_udp_send_t *req, int status) {
    if (status) {
        fprintf(stderr, "Send error %s\n", uv_strerror(status));
    }
    if (req) {
        // printf("release!  ");
        if (req->data)
            free(req->data);
        free(req);
       ++TOTAL_FREE_UDP_PACK;
    }
}

bool UnPackMsg2Str(int itype, const string& msg, string *result_str)
{
	bool bret = false;
	char sbuff[1024];
    if (itype == PB::MSGCARRIER::MsgCarrier_MsgType_SNAPSHOT)
    {
        PB::Quote::SnapShot snap;
        bret = snap.ParseFromString(msg);
        int64_t timestampsMs;
		if(bret)
		{
//shot_test,date=20180122,exch=1,code=600331 status=73,lastprice=0.0,prevclose=5.19,open=0.0,high=0.0,low=0.0,volume=0,value=0.0,highlimited=0.0,lowlimited=0.0,niopv=0.0,numtrades=0,totalBidVol=0,totalAskVol=0,askVolume0=0,askVolume1=0,askVolume2=0,askVolume3=0,askVolume4=0,askVolume5=0,askVolume6=0,askVolume7=0,askVolume8=0,askVolume9=0,bidVolume0=0,bidVolume1=0,bidVolume2=0,bidVolume3=0,bidVolume4=0,bidVolume5=0,bidVolume6=0,bidVolume7=0,bidVolume8=0,bidVolume9=0,askPrice0=0.0,askPrice1=0.0,askPrice2=0.0,askPrice3=0.0,askPrice4=0.0,askPrice5=0.0,askPrice6=0.0,askPrice7=0.0,askPrice8=0.0,askPrice9=0.0,bidPrice0=0.0,bidPrice1=0.0,bidPrice2=0.0,bidPrice3=0.0,bidPrice4=0.0,bidPrice5=0.0,bidPrice6=0.0,bidPrice7=0.0,bidPrice8=0.0,bidPrice9=0.0,nWeightedAvgBidPrice=0.0,nWeightedAvgAskPrice=0.0 1516583642361
			timestampsMs = GetDateSecond(snap.date())*1000 + snap.time();
			int n_size = snprintf(sbuff, sizeof(sbuff), format_snap
					,snap.date()
					,snap.exchange()
					,snap.code().c_str()
					,snap.status()
					,snap.lastprice()
					,snap.prevclose()
					,snap.open()
					,snap.high()
					,snap.low()
					,snap.volume()
					,snap.value()
					,snap.highlimited()
					,snap.lowlimited()
					,snap.niopv()
					,snap.numtrades()
					,snap.totalbidvol()
					,snap.totalaskvol());

			result_str->append(sbuff, n_size);
			int k;
			//askVolume
			for (k = 0; k < snap.askvolumes_size(); ++k) {
				sprintf(sbuff, ",askVolume%d=%lld", k, snap.askvolumes(k));
				result_str->append(sbuff);
			}
			while (k < 10) {
				sprintf(sbuff, ",askVolume%d=0", k++);
				result_str->append(sbuff);
			}
			//bidVolume
			for (k = 0; k < snap.bidvolumes_size(); ++k) {
				sprintf(sbuff, ",bidVolume%d=%lld", k, snap.bidvolumes(k));
				result_str->append(sbuff);
			}
			while (k < 10) {
				sprintf(sbuff, ",bidVolume%d=0", k++);
				result_str->append(sbuff);
			}
			//askPrice
			for (k = 0; k < snap.askprices_size(); ++k) {
				sprintf(sbuff, ",askPrice%d=%.2f", k, snap.askprices(k));
				result_str->append(sbuff);
			}
			while (k < 10) {
				sprintf(sbuff, ",askPrice%d=0.0", k++);
				result_str->append(sbuff);
			}
			//bidPrice
			for (k = 0; k < snap.bidprices_size(); ++k) {
				sprintf(sbuff, ",bidPrice%d=%.2f", k, snap.bidprices(k));
				result_str->append(sbuff);
			}
			while (k < 10) {
				sprintf(sbuff, ",bidPrice%d=0.0", k++);
				result_str->append(sbuff);
			}

			sprintf(sbuff, ",nWeightedAvgBidPrice=%.2f,nWeightedAvgAskPrice=%.2f %lld000000\n"
					,snap.nweightedavgbidprice()
					,snap.nweightedavgaskprice()
					,timestampsMs);
			result_str->append(sbuff);
		}
		else
		{
			printf("error: fail to parse snapshot!");
		}

	} else if (itype == PB::MSGCARRIER::MsgCarrier_MsgType_TRANSACTIONS) {
		PB::Quote::Transactions trans;
		bret = trans.ParseFromString(msg);
		if (bret) {
//tick,trade_date=20180122,exch=1,code=601727 price=6.63,nIndex=1,volume=100,turnover=663.0,nBSFlag=0,chOrderKind=48,chFunctionCode=48,nAskOrder=0,nBidOrder=0 1516584300000
			int64_t timestampsMs;
			int tran_no;
			for (int i = 0; i < trans.items_size(); ++i) {
				auto& pdata = trans.items(i);
				timestampsMs = GetDateSecond(pdata.date())*1000 + pdata.time();
				tran_no = (pdata.nindex() < 1000000 ? pdata.nindex():(pdata.nindex()%1000000));
				int n_size = snprintf(sbuff, sizeof(sbuff), format_tick
						,pdata.date()
						,pdata.exchange()
						,pdata.code().c_str()
						,pdata.lastprice()
						,pdata.nindex()
						,pdata.volume()
						,pdata.turnover()
						,pdata.nbsflag()
						,pdata.chorderkind()
						,pdata.chfunctioncode()
						,pdata.naskorder()
						,pdata.nbidorder()
						,timestampsMs
						,tran_no);//reserved after 6 number
				result_str->append(sbuff, n_size);
			}
		} else {
			printf("error: fail to parse trans!");
		}
	}
    return bret;
}

udp_msg * get_udp_node(const string* msgsrc)
{
    udp_msg * plist_now = nullptr;
    int batch_msg_size = msgsrc->size();
    if(batch_msg_size > 0)
    {
        plist_now = (udp_msg *) malloc( offsetof(udp_msg, data) + batch_msg_size + 10);
        if (plist_now) {
            plist_now->len = batch_msg_size;
            plist_now->next = nullptr;
            memcpy(plist_now->data, msgsrc->c_str(), batch_msg_size);
        } else {
            printf("error: fail to malloc for udp_msg!\n");
        }
    }
    return plist_now;
}

void Job(uv_work_t *req)
{
    printf("tid:%ld, working...\n", gettid());
    if(req->data ==nullptr)
    {
        return ;
    }
    marketdata_task* ptask = (marketdata_task*)(req->data);

    PB::MSGCARRIER::MsgCarrier mc;

    RecordTimeMs timems;
    timems.start();

    char *pbuf = ptask->data;
    int32_t next_msg_size = *((int32_t*)pbuf);
    int n_read_byte = 4;
    string result = string();
    udp_msg * plist_head = nullptr;
    udp_msg * plist_tail = nullptr;

    int32_t produce_msglen = 0;
    int32_t send_udp_packcnt =0;
    while (n_read_byte < ptask->used_bytes) {
        string msg = string(pbuf + n_read_byte, next_msg_size);
        n_read_byte += next_msg_size;
        //printf("=====> %d\n", msgsize);
        if (mc.ParseFromString(msg)) {
            UnPackMsg2Str(mc.type(), mc.message(), &result);
        }
        next_msg_size = *((int32_t*) (pbuf + n_read_byte));
        n_read_byte += 4; // 4 byte
        //over 1200 bytes, create an udp_msg, put into single linked list
        if (result.size() + next_msg_size > MAX_UDPMSG_SIZE) {
            ++send_udp_packcnt;
            produce_msglen += result.size();

            udp_msg * pNode = get_udp_node(&result);
            //remind single-linked list head
            if (plist_tail == nullptr) {
                plist_tail = pNode;
                plist_head = pNode;
            } else {
                // point to next, after move next
                plist_tail->next = pNode;
                plist_tail = pNode;
            }
            result.clear();
        }
    }

    //last msg
    if (result.size() > 0) {
        ++send_udp_packcnt;
        produce_msglen += result.size();

        udp_msg * pNode = get_udp_node(&result);
        //remind single-linked list head
        if (plist_tail == nullptr) {
            plist_tail = pNode;
            plist_head = pNode;
        } else {
            // point to next, after move next
            plist_tail->next = pNode;
            plist_tail = pNode;
        }
    }
    //save the head of single-list
    if(plist_head != nullptr){
         ptask->udp_msg_ptr = plist_head;
    }
    printf("++++ total send udp_packs count :%d, bytes: %d\n", send_udp_packcnt, produce_msglen);
    printf("===> Batch packages: %d, speed:%.2f packs/ms.\n", ptask->total, ptask->total/timems.stop());
}

void JobDone(uv_work_t *req, int status) {
    if (!req) {
        printf("error: JobDone, invalid req!\n");
        return;
    }
    //Distribute tasks, after release
    if (req->data != nullptr) {
        marketdata_task* ptask = (marketdata_task*) (req->data);
        udp_msg * pNode = ptask->udp_msg_ptr;
        while (pNode != nullptr) {

            uv_buf_t buf = uv_buf_init(pNode->data, pNode->len);
            uv_udp_send_t* request = (uv_udp_send_t*) malloc(sizeof(uv_udp_send_t));
            request->data = pNode; // for release node
            uv_udp_send(request, (uv_udp_t *) (ptask->udp_hdl), &buf, 1, (const struct sockaddr *) (ptask->udp_addr), on_udp_send);

            /*
            //TODO async write file
            uv_buf_t buf = uv_buf_init(pNode->data, pNode->len);
            uv_fs_t * request = (uv_fs_t*) malloc(sizeof(uv_fs_t));
            request->data = pNode; // for release node
            uv_fs_write(req->loop, request, ptask->file_hdl, &buf, 1, -1, on_write_done);
            */

            // move to next
            pNode = pNode->next;
            ++TOTAL_REDUCE_UDP_PACK;
        }
        printf("JobDone: release! distribute UDP.\n");
        free(req->data);
    }
    free(req);
}

//running inside main thread
void Consumer::OnAsync(uv_async_t* handle)
{
	Consumer* self = (Consumer*)(handle->data);
    vector<marketdata_task*> tmpset;
    uv_mutex_lock(&(self->m_mtx));
    tmpset.swap(self->m_tasks);
    // self->m_tasks.clear();
    uv_mutex_unlock(&(self->m_mtx));

	for (auto ptask : tmpset) {
	    ptask->udp_addr = self->m_udp_addr;
	    ptask->udp_hdl = self->m_udp_hdl;
	    //create a uv_work
		uv_work_t *request = (uv_work_t*) malloc(sizeof(uv_work_t));
		request->data = ptask;
		uv_queue_work(handle->loop, request, Job, JobDone);
	}
}

int Consumer::SendAsync(marketdata_task* data)
{
	uv_mutex_lock(&m_mtx);
    m_tasks.push_back(data);
    uv_mutex_unlock(&m_mtx);
    //notify
    m_async_hdl->data = this;
    return uv_async_send(m_async_hdl);
}




