#include "recv_quote.h"

#include <unistd.h>
#include <inttypes.h>
#include <time.h>
#include <sys/time.h>
#include <fcntl.h>
#include <stddef.h>

#define IS_LEAP_YEAR(y)  (((y)%4==0 && (y)%100!=0)||(y)%400==0)
#include "../../emsproto/quote.pb.h"
//#pragma GCC diagnostic ignored "-Wformat="

//public
void on_close_cb(uv_handle_t* handle){
    free(handle);
}

static void Job(uv_work_t *req);
static void JobDone(uv_work_t *req, int status);
void on_write_done(uv_fs_t *req);
void on_udp_send(uv_udp_send_t *req, int status);
// create an udp_msg node
tcp_msg * get_tcp_node(int buffsize);

//tcp

void alloc_tcp_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf);
void on_tcp_recvmsg(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf);
void on_tcp_send(uv_write_t* req, int status);

typedef struct snap_extend_tag{
    int level;
    const char * measure;
    const char * exfileds;
}snap_extend;

const char format_market[] = "%s,trade_date=%d,exchg=%d,security_id=%s status=%d,trade_time=%d,last_px=%.2f,prev_close_px=%.2f,open_px=%.2f,high_px=%.2f,low_px=%.2f,total_volume_traded=%lld,total_value_traded=%.2f,high_limit_px=%.2f,low_limit_px=%.2f";
snap_extend SnapExts[2]={
        {10, "market", ",iopv=%.4f,num_traded=%d,total_bid_qty=%lld,total_offer_qty=%lld,weighted_avg_bid_px=%.2f,weighted_avg_offer_px=%.2f"},
        {5, "future", ",settle_px=%.2f,prev_settle_px=%.2f,delta=%.4f,prev_delta=%.4f,openinterest=%.2f,prev_openinterest=%.2f"}
};

const char format_index[] = "index,trade_date=%d,exchg=%d,security_id=%s trade_time=%d,open_px=%lld,high_px=%lld,low_px=%lld,last_px=%lld,prev_close_px=%lld,total_volume_traded=%lld,total_value_traded=%lld,seqno=%llu %lld000000\n";

const char format_tick[] = "tick,trade_date=%d,exchg=%d,security_id=%s trade_time=%d,trade_ref=%d,trade_price=%.2f,trade_qty=%lld,trade_money=%.2f,ord_type=%d,trade_code=%d,bid_appl_seqnum=%d,offer_appl_seqnum=%d,seqno=%llu %lld%06d\n";

const char format_order[] = "order,trade_date=%d,exchg=%d,security_id=%s trade_time=%d,price=%.2f,appl_seqnum=%d,order_qty=%d,ord_type=%d,trade_code=%d,seqno=%llu %lld000000\n";
const char format_queue[] = "queue,trade_date=%d,exchg=%d,security_id=%s trade_time=%d,price=%.2f,num_orders=%d,side=%d,no_orders=%d";//OrderQty%d=%d ...%lld
const char HTTP_HEADER[] = "POST /write?db=%s&u=toptrade&p=toptrade&precision=ns HTTP/1.1\r\n"\
"Host: %s:%d\r\n"
"Content-Length:        $\r\n" \
"Connection: Keep-Alive\r\n" \
"User-Agent: uvHttpClient/1.1\r\n" \
"Accept: */*\r\n" \
"Content-Type: application/x-www-form-urlencoded\r\n" \
"Expect: 100-continue\r\n\r\n";
//cpu,host=serverCode,region=china_code value=0.111 1520320877369812809";

const int MAX_MSGBUFF_SIZE = (16 << 20); //32M , default is 64M
const int MAX_TCPMSG_SIZE = (64 << 20) ; // 20M , default udpmsg is less than 1200
const int PRINT_BYTE_SIZE = 1024; //1K
// debug
int32_t TOTAL_REDUCE_TCP_PACK = 0;
int32_t TOTAL_FREE_TCP_PACK = 0;

//calculate the date of milliseconds since 1970-01-01
const int gCalcYears = 300;
static int gYearOfDay[gCalcYears]={0};
static int InitYearOfDays(int years) {
    for (int i = 1; i < years; ++i)
        gYearOfDay[i] = gYearOfDay[i-1] + 365 + IS_LEAP_YEAR(1970 + i-1);
    return years;
}
int g_init_years = InitYearOfDays(gCalcYears);
//cost 16.73 ms/10^6times !
// CalcMsUTC(20180308);
static int64_t CalcMsUTC(int idate)
{
    /*static int months[12]={31,28,31,30,31,30,31,31,30,31,30,31};*/
    static int MonthOfDay[13] = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365 };
    int year, month, days;
    year = idate / 10000;
    month = (idate / 100 % 100);
    days = (idate % 100);
    int64_t diffms = (year < (1970 + gCalcYears) ? gYearOfDay[year - 1970] : 0);
    diffms += (MonthOfDay[month - 1] + days - 1);
    if (month > 2 && IS_LEAP_YEAR(year))
        diffms += 1;
    return ((diffms * 86400 - 28800) * 1000); //BeiJing - 8 hour => UTC
}

int GetCurTime()
{
    struct tm *local;
    time_t t;
    t = time(NULL);
    local = localtime(&t);
    return local->tm_hour*10000+local->tm_min*100+local->tm_sec;
}

MarketProvider::MarketProvider(uv_loop_t *loop, const std::string& saddr, const std::string& topic, const char* filename, Consumer* pwork)
{
    m_loop = loop;
    m_run = false;
    m_saddr = saddr;
    m_topic = topic;
    // init zmq
    m_ctx = zmq::context_t(1);
    m_socket = std::make_shared<zmq::socket_t>(m_ctx, ZMQ_SUB);
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
                printf(">>>Time:%d ,TOTAL_REDUCE_TCP_PACK: %d ,TOTAL_FREE_TCP_PACK: %d .\n", GetCurTime(), TOTAL_REDUCE_TCP_PACK, TOTAL_FREE_TCP_PACK);
                //usleep(1000000); //1s
                continue;
            }
            ++failcnt;
            if (failcnt > 4) {
                printf("[%s]Continuous timeout %d times! send last %d packs.\n"
                        ,self->m_topic.c_str(), failcnt, ptask->total_packs);
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
                ptask->pself = nullptr;
                ptask->file_hdl = self->m_filehdl;
                ptask->tcp_header = nullptr;
                ptask->tcp_header_len = 0;
                ptask->tcp_dollar_pos = 0;
                ptask->udp_msg_ptr = nullptr;
                ptask->total_packs = 0;
                ptask->used_bytes = 0;
            }

            int32_t msg_len =  msg.size();
            memcpy( (ptask->data + ptask->used_bytes), &msg_len, sizeof(msg_len)); //4 byte
            ptask->used_bytes += sizeof(msg_len);
            memcpy( (ptask->data + ptask->used_bytes), msg.data(), msg.size()); // msg
            ptask->used_bytes += msg_len;
            ptask->total_packs++;
            //if full, send msg to another thread
            if( (MAX_MSGBUFF_SIZE - ptask->used_bytes) < 10 * 1024)
            {
                if(self->m_work->SendAsync(ptask) )
                    printf("error: fail to  send async !\n");
                double cost = timems.stop();
                printf("Recv speed: %.2f byte/ms, %d packs!  %.2f packs/ms.\n", ptask->used_bytes/cost, ptask->total_packs, ptask->total_packs/cost);
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

//////
TcpClient::TcpClient(uv_loop_t* loop, const std::string& tcp_addr, int port, int id){
    m_id = id;
    m_loop = loop;
    m_tcpaddr = (struct sockaddr_in*)malloc(sizeof(struct sockaddr_in));
    uv_ip4_addr(tcp_addr.c_str(), port, m_tcpaddr);

    m_socket = (uv_tcp_t*)malloc(sizeof(uv_tcp_t));
    uv_tcp_init(m_loop, m_socket);
    uv_tcp_keepalive(m_socket, 1, 600); // 60 seconds
    uv_tcp_nodelay(m_socket,1);

    m_state = EConnStatus::Disconn;
    m_connect = (uv_connect_t*)malloc(sizeof(uv_connect_t));
    m_connect->data = this;
    uv_tcp_connect(m_connect, m_socket, (const struct sockaddr*)m_tcpaddr, TcpClient::on_tcp_connect);
}

TcpClient::~TcpClient() {
    m_state = EConnStatus::Disconn;
    if (m_connect)
        free(m_connect);
    if (m_socket) {
        uv_close((uv_handle_t*) m_socket, on_close_cb);
    }
}
bool TcpClient::Reconnect()
{
    int ret = uv_tcp_connect(m_connect, m_socket, (const struct sockaddr*)m_tcpaddr, TcpClient::on_tcp_connect);
    printf("Reconnect! handle:%d, retCode:%d:%s !\n", m_id, ret, uv_strerror(ret));
    return (ret == 0);
}
void TcpClient::on_tcp_connect(uv_connect_t* req, int status) {
    TcpClient* self = (TcpClient*) (req->data);
    if (status != 0) {
        self->m_state = EConnStatus::Disconn;
        fprintf(stderr, "New tcp connection error: %s\n", uv_strerror(status));
    } else {
        self->m_state = EConnStatus::Avail;
        printf("tcp-connected! m_id:%d.\n", self->m_id);
    }
}

/////
Consumer::Consumer(uv_loop_t* loop)
{
    m_loop = loop;
    uv_mutex_init(&m_mtx);

    m_async_hdl = (uv_async_t*)malloc(sizeof(uv_async_t));
    uv_async_init(this->m_loop, m_async_hdl, Consumer::OnAsync);
    m_tasks.clear();

    m_header = nullptr;
    m_headerlen = 0;
    m_dollarpos = 0;
    m_index = 0;
}
Consumer::~Consumer()
{
    uv_mutex_destroy(&m_mtx);
    //write a release function by ourself.
    uv_close((uv_handle_t*)m_async_hdl, on_close_cb);

    // free tcp, clear vector
    m_tcpConn.clear();
    // free tcp addr
    if (m_header) free(m_header);
}

bool Consumer::InitTCP(const std::string& tcp_addr, const std::string& dbname, int port, int cnt)
{
    for (int i = 0; i < cnt; ++i) {
        m_tcpConn.push_back(std::make_shared<TcpClient>(m_loop, tcp_addr, port, i));
    }
    int header_len = sizeof(HTTP_HEADER) + 128;
    m_header = (char*) malloc(header_len);
    m_headerlen = snprintf(m_header, header_len, HTTP_HEADER, dbname.c_str(), tcp_addr.c_str(),port);
    for (int i = 0; i < m_headerlen; ++i) {
        if (m_header[i] == '$') {
            m_dollarpos = i;
            break;
        }
    }
    printf("total tcpConn is %d, m_dollarpos = %d!\n",cnt, m_dollarpos);
    return true;
}
void on_tcp_send(uv_write_t* req, int status) {
    TcpClient *pTcp = (TcpClient *)( ((tcp_msg_tag*)(req->data))->pself);
    if (status == 0){
        if (req)
        {
            pTcp->m_state = EConnStatus::Recving;
            req->handle->data = pTcp;
            uv_read_start(req->handle, alloc_tcp_buffer, on_tcp_recvmsg);
            if (req->data)
            {
                // printf("release!  ");
                free(req->data);
            }
            ++TOTAL_FREE_TCP_PACK;
            free(req);
        }
    }else{
        pTcp->m_state = EConnStatus::Disconn;
        fprintf(stderr,"send tcp error! mid:%d ,%s!\n", pTcp->m_id, uv_strerror(status));
    }
}

void alloc_tcp_buffer(uv_handle_t* handle, size_t suggested_size, uv_buf_t* buf)
{
    int size = 1024;
    buf->base = (char*) malloc(size);
    buf->len = size;
}

void on_tcp_recvmsg(uv_stream_t* handle, ssize_t nread, const uv_buf_t* buf)
{
    TcpClient *pTcp = (TcpClient *)(handle->data);

    if (nread <= 0) {
        printf("ERROR: recv msg error: nread = %lld!\n", nread);
    } else {
        //HTTP/1.1 204 No Content
        if (buf->base) {
            if( buf->base[9]=='1' && buf->base[10]=='0' && buf->base[11]=='0' ){
                // try to read 204 response
               uv_read_start(handle, alloc_tcp_buffer, on_tcp_recvmsg);
               //printf("handle: %d <HTTP/1.1 100 Continue>\n", pTcp->m_id);

            }else if ( buf->base[9]=='2' && buf->base[10]=='0' && buf->base[11]=='4' ){
                // successful
                pTcp->m_state = EConnStatus::Avail;
               // printf("handle: %d <HTTP/1.1 204 No Content>\n", pTcp->m_id);

            } else {
                printf("ERROR! handle:%d recv msg:\n[%s]\n!", pTcp->m_id, buf->base);
            }
        }
    }
    if (buf->base)
        free(buf->base);
}

////write file
void on_write_done(uv_fs_t *req) {
    if (req) {
        uv_fs_req_cleanup(req);
        if (req->data) {
            //printf("release!  ");
            free(req->data);
        }
        free(req);
        ++TOTAL_FREE_TCP_PACK;
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
       ++TOTAL_FREE_TCP_PACK;
    }
}

/**
 * @param pfrom
 * @param sizes
 * @param pstr
 * @return int : the size of bytes writed into char* pstr
 */
int UnPackMsg2Str(const void *pfrom, const int sizes, char *pstr)
{
    PB::MSGCARRIER::MsgCarrier mc;
    if(mc.ParseFromArray(pfrom, sizes)== false)
    {
        printf("error: fail to parse msgCarrier!\n");
        return 0;
    }
    int itype = mc.type();
    const std::string &msg = mc.message();

    int byteoffset = 0;
    int64_t timestampsMS;
    if (itype == PB::MSGCARRIER::MsgCarrier_MsgType_SNAPSHOT)
    {
        PB::Quote::SnapShot snap;
        if(snap.ParseFromString(msg))
        {
            // market or future
            int format_idx = (snap.exchange() < 3 ? 0 : 1);
            const int PriceLevel = SnapExts[format_idx].level;

            timestampsMS = CalcMsUTC(snap.date()) + snap.time();
            int n_size = snprintf(pstr, PRINT_BYTE_SIZE, format_market
                    ,SnapExts[format_idx].measure
                    ,snap.date()
                    ,snap.exchange()
                    ,snap.code().c_str()
                    ,snap.status()
                    ,snap.time()
                    ,snap.lastprice()
                    ,snap.prevclose()
                    ,snap.open()
                    ,snap.high()
                    ,snap.low()
                    ,snap.volume()
                    ,snap.value()
                    ,snap.highlimited()
                    ,snap.lowlimited());
            byteoffset += n_size;

            // extend fields
            if(format_idx == 0)
            {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, SnapExts[format_idx].exfileds
                    ,snap.niopv()
                    ,snap.numtrades()
                    ,snap.totalbidvol()
                    ,snap.totalaskvol()
                    ,snap.nweightedavgbidprice()
                    ,snap.nweightedavgaskprice());
                byteoffset += n_size;
            }else{
                 n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, SnapExts[format_idx].exfileds
                    ,snap.settleprice()
                    ,snap.prevsettleprice()
                    ,snap.delta()
                    ,snap.prevdelta()
                    ,snap.iopeninterest()
                    ,snap.prevopeninterest());
                byteoffset += n_size;
            }

            int k;
            //卖一量
            for (k = 0; k < snap.askvolumes_size(); ++k) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",offer_size%d=%lld", k+1, snap.askvolumes(k));
                byteoffset += n_size;
            }
            while (k < PriceLevel) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",offer_size%d=0", ++k);
                byteoffset += n_size;
            }
            //买一量
            for (k = 0; k < snap.bidvolumes_size(); ++k) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",bid_size%d=%lld", k+1, snap.bidvolumes(k));
                byteoffset += n_size;
            }
            while (k < PriceLevel) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",bid_size%d=0", ++k);
                byteoffset += n_size;
            }
            //卖一价
            for (k = 0; k < snap.askprices_size(); ++k) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",offer_px%d=%.2f", k+1, snap.askprices(k));
                byteoffset += n_size;
            }
            while (k < PriceLevel) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",offer_px%d=0.0", ++k);
                byteoffset += n_size;
            }
            //买一价
            for (k = 0; k < snap.bidprices_size(); ++k) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",bid_px%d=%.2f", k+1, snap.bidprices(k));
                byteoffset += n_size;
            }
            while (k < PriceLevel) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",bid_px%d=0.0", ++k);
                byteoffset += n_size;
            }
            n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",seqno=%llu %lld000000\n" ,snap.seqno(), timestampsMS);
            byteoffset += n_size;
        }
        else
        {
            printf("error: fail to parse snapshot!\n");
        }

    } else if (itype == PB::MSGCARRIER::MsgCarrier_MsgType_TRANSACTIONS) {
        PB::Quote::Transactions trans;
        if (trans.ParseFromString(msg)) {
//tick,Date=20171227,Exchg=2,SecurityID=150118 TradePrice=0.00,TradeIndex=1324,TradeQty=20000,TradeMoney=0.00,OrdType=48,TradeCode=67,OfferApplSeqNum=1310,BidApplSeqNum=0 1514337301130001324
            int tran_no;
            for (int i = 0; i < trans.items_size(); ++i) {
                auto& pdata = trans.items(i);
                timestampsMS = CalcMsUTC(pdata.date()) + pdata.time();
                tran_no = (pdata.nindex() < 1000000 ? pdata.nindex():(pdata.nindex()%1000000));
                int n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, format_tick
                        ,pdata.date()
                        ,pdata.exchange()
                        ,pdata.code().c_str()
                        ,pdata.time()
                        ,pdata.nindex()
                        ,pdata.lastprice()
                        ,pdata.volume()
                        ,pdata.turnover()
                        ,pdata.chorderkind()
                        ,pdata.chfunctioncode()
                        ,pdata.nbidorder()
                        ,pdata.naskorder()
                        ,pdata.seqno()
                        ,timestampsMS
                        ,tran_no);//reserved after 6 number
                byteoffset += n_size;
            }
            if(trans.items_size()>1)
            {
                printf("\n WARN: maybe parse trans size too long!============>>>>> %d\n", trans.items_size());
            }
        } else {
            printf("error: fail to parse trans!\n");
        }
    } else if(itype == PB::MSGCARRIER::MsgCarrier_MsgType_INDEX)
    {
        PB::Quote::Index idx;
        if(idx.ParseFromString(msg))
        {
//index,Date=20171227,Exchg=1,SecurityID=000999 OpenPx=18221344,HighPx=18228986,LowPx=18220479,LastPx=18228054,PrevClosePx=18231449,TotalVolumeTraded=659693,TotalValueTraded=9517583 1514337309000000000
            timestampsMS = CalcMsUTC(idx.date()) + idx.time();
            int n_size = snprintf(pstr, PRINT_BYTE_SIZE, format_index
                    ,idx.date()
                    ,idx.exchange()
                    ,idx.code().c_str()
                    ,idx.time()
                    ,idx.openindex()
                    ,idx.highindex()
                    ,idx.lowindex()
                    ,idx.lastindex()
                    ,idx.precloseindex()
                    ,idx.totalvolume()
                    ,idx.turnover()
                    ,idx.seqno()
                    ,timestampsMS);
            byteoffset += n_size;
        }else {
            printf("error: fail to parse index!");
        }
    }else if(itype == PB::MSGCARRIER::MsgCarrier_MsgType_ORDER)
    {
        PB::Quote::Order order;
        if(order.ParseFromString(msg))
        {
           //order,Date=20171227,Exchg=2,SecurityID=300036 Price=15.01,ApplSeqNum=1959,OrderQty=500,OrdType=48,TradeCode=66 1514337300050000000
            timestampsMS = CalcMsUTC(order.date()) + order.time();
            int n_size = snprintf(pstr, PRINT_BYTE_SIZE, format_order
                    ,order.date()
                    ,order.exchange()
                    ,order.code().c_str()
                    ,order.time()
                    ,order.nprice()
                    ,order.norder() //entrust_no
                    ,order.nvolume()
                    ,order.chorderkind()
                    ,order.chfunctioncode()
                    ,order.seqno()
                    ,timestampsMS);
             byteoffset += n_size;
        }else {
            printf("error: fail to parse order!\n");
        }
    }else if(itype == PB::MSGCARRIER::MsgCarrier_MsgType_ORDERQUEUE)
    {
        PB::Quote::OrderQueue queue;
        if(queue.ParseFromString(msg))
        {
            // "queue,Date=%d,exch=%d,SecurityID=%s Price=%.2f,NoOrders=%d,Side=%d,nABItems=%d";//OrderQty%d=%d ...%lld
            timestampsMS = CalcMsUTC(queue.date()) + queue.time();
            int n_size = snprintf(pstr, PRINT_BYTE_SIZE, format_queue
                    ,queue.date()
                    ,queue.exchange()
                    ,queue.code().c_str()
                    ,queue.time()
                    ,queue.nprice()
                    ,queue.norders()
                    ,queue.nside()
                    ,queue.nabitems());
            byteoffset += n_size;
            int k;
            int max_volume_cnt = (queue.nabvolume_size() > 50 ? 50 : queue.nabvolume_size());
            for (k = 0; k < max_volume_cnt; ++k) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",order_qty%d=%d", k+1, queue.nabvolume(k));
                byteoffset += n_size;
            }
            while (k < 50) {
                n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",order_qty%d=0", ++k);
                byteoffset += n_size;
            }
            n_size = snprintf(pstr + byteoffset, PRINT_BYTE_SIZE, ",seqno=%llu %lld000000\n",queue.seqno(), timestampsMS);
            byteoffset += n_size;
        }else {
            printf("error: fail to parse queue!\n");
        }
    }else{
        printf("error: no support msg type:%d !\n", itype);
    }
    return byteoffset;
}

tcp_msg * get_tcp_node(int buffsize)
{
    tcp_msg * pNode = nullptr;
    assert(buffsize > 0);
    if(buffsize > 0)
    {
        pNode = (tcp_msg *) malloc(buffsize);
        if (pNode) {
            pNode->pself = nullptr;
            pNode->capcity = (buffsize - offsetof(tcp_msg, data));
            pNode->size = 0;
            pNode->next = nullptr;
        } else {
            printf("error: fail to malloc for udp_msg!\n");
            assert(0);
        }
    }
    return pNode;
}

void Job(uv_work_t *req)
{
    printf("tid:%ld, working...\n", gettid());
    if(req->data ==nullptr)
    {
        return ;
    }
    marketdata_task* ptask = (marketdata_task*)(req->data);

    RecordTimeMs timems;
    timems.start();

    char *pbuf = ptask->data;
    int byteoffset = 4;
    int32_t next_msg_size = *((int32_t*)pbuf);
    tcp_msg * plist_head = nullptr;
    tcp_msg * plist_tail = nullptr;
    tcp_msg * pNode = nullptr;

    int32_t gen_msglen = 0;
    int32_t send_tcp_packcnt =0;
    while (byteoffset < ptask->used_bytes) {
        //malloc node
        if(pNode == nullptr)
        {
            pNode = get_tcp_node(MAX_TCPMSG_SIZE);
            // fill tcp_header
            memcpy(pNode->data,ptask->tcp_header, ptask->tcp_header_len);
            pNode->size = pNode->size + ptask->tcp_header_len;
            //first
            if (plist_tail == nullptr) {
                plist_tail = pNode;
                plist_head = pNode;
            } else {
                // point to next, after move next
                plist_tail->next = pNode;
                plist_tail = pNode;
            }
        }
        int n_size = UnPackMsg2Str( (pbuf + byteoffset), next_msg_size, (pNode->data + pNode->size));
        byteoffset += next_msg_size;
        next_msg_size = *((int32_t*) (pbuf + byteoffset));
        byteoffset += 4; // 4 byte

        pNode->size = pNode->size + n_size;
        //if surplus buffer size is less than 3K
        if ((pNode->capcity - pNode->size) < (2 * 1024)) {
            // fill datalen
            int tcp_data_len = pNode->size - ptask->tcp_header_len;
            int dollar_pos = ptask->tcp_dollar_pos;
            assert( pNode->data[dollar_pos]=='$');
            while(tcp_data_len >0 && dollar_pos > 0)
            {
                pNode->data[dollar_pos]= ((tcp_data_len%10)+'0');
                tcp_data_len /= 10;
                dollar_pos--;
            }
            ++send_tcp_packcnt;
            gen_msglen += pNode->size;
            pNode = nullptr;
        }
    }
    //be careful! the "content-length" must be fill!
    if ((plist_tail != nullptr) && (plist_tail->data[ptask->tcp_dollar_pos] == '$'))
    {
        int tcp_data_len = plist_tail->size - ptask->tcp_header_len;
        int dollar_pos = ptask->tcp_dollar_pos;
        while (tcp_data_len > 0 && dollar_pos > 0) {
            plist_tail->data[dollar_pos] = ((tcp_data_len % 10) + '0');
            tcp_data_len /= 10;
            dollar_pos--;
        }
        ++send_tcp_packcnt;
        gen_msglen += plist_tail->size;
    }
    //save the head of single-list, plist_head maybe nullptr, nothing
    ptask->udp_msg_ptr = plist_head;
    printf("++++ total send tcp_packs count :%d, gen bytes: %d\n", send_tcp_packcnt, gen_msglen);
    printf("===> Batch packages: %d, speed:%.2f packs/ms.\n", ptask->total_packs, ptask->total_packs/timems.stop());
}

void JobDone(uv_work_t *req, int status) {
    if (!req) {
        fprintf(stderr, "error: JobDone %s!\n", uv_strerror(status));
        return;
    }
    //Distribute tasks, after release
    if (req->data != nullptr) {
        marketdata_task* ptask = (marketdata_task*) (req->data);
        tcp_msg * pNode = ptask->udp_msg_ptr;
        Consumer * pCon = (Consumer *)(ptask->pself);
        while (pNode != nullptr) {
            /*
            uv_buf_t buf = uv_buf_init(pNode->data + ptask->tcp_header_len, pNode->size - ptask->tcp_header_len);
            uv_udp_send_t* request = (uv_udp_send_t*) malloc(sizeof(uv_udp_send_t));
            request->data = pNode; // for release node
            uv_udp_send(request, (uv_udp_t *) (ptask->udp_hdl), &buf, 1, (const struct sockaddr *) (ptask->udp_addr), on_udp_send);
            */

            //TODO async write file
            uv_buf_t buf = uv_buf_init(pNode->data + ptask->tcp_header_len, pNode->size - ptask->tcp_header_len);
            //uv_buf_t buf = uv_buf_init(pNode->data , pNode->size);
            uv_fs_t * request = (uv_fs_t*) malloc(sizeof(uv_fs_t));
            request->data = pNode; // for release node
            uv_fs_write(req->loop, request, ptask->file_hdl, &buf, 1, -1, on_write_done);

            // TODO send tcp
            /* uv_buf_t buf = uv_buf_init(pNode->data, pNode->size);
            uv_write_t * request = (uv_write_t*) malloc(sizeof(uv_write_t));
            request->data = pNode; // for release node
            pNode->pself = pCon->m_tcpConn[pCon->m_index].get();
            uv_write(request, pCon->m_tcpConn[pCon->m_index]->m_connect->handle , &buf, 1, on_tcp_send);
            pCon->m_index = (pCon->m_index + 1) % (pCon->m_tcpConn.size()) ;
            */
            // move to next
            pNode = pNode->next;
            ++TOTAL_REDUCE_TCP_PACK;
        }
        printf("JobDone: release! distribute TCP pack.\n");
        free(req->data);
    }
    free(req);
}

//running inside main thread
void Consumer::OnAsync(uv_async_t* handle)
{
    Consumer* self = (Consumer*)(handle->data);
    std::vector<marketdata_task*> tmpset;
    uv_mutex_lock(&(self->m_mtx));
    tmpset.swap(self->m_tasks);
    // self->m_tasks.clear();
    uv_mutex_unlock(&(self->m_mtx));

    for (auto ptask : tmpset) {
        ptask->pself = self;
        ptask->tcp_header = self->m_header;
        ptask->tcp_header_len = self->m_headerlen;
        ptask->tcp_dollar_pos = self->m_dollarpos;
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


