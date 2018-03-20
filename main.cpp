#include <cstdlib>
#include <cstdio>
#include <unistd.h>

#include "recv_quote.h"


int main(int argc, char **argv) {
	if (argc < 4) {
		printf("input format: ./recv_quote tcp://192.168.4.62:8046  192.168.10.10 8089 \n");
		return 0;
	}
	string saddr = argv[1];
	string udp_addr = argv[2];
	int udp_port = atoi(argv[3]);

	uv_loop_t* loop =  uv_default_loop();

	auto works = new Consumer(loop);
	works->InitUDP(udp_addr, udp_port);

	auto pMarket = new MarketProvider(loop, saddr, "26.","./market.data", works);
	auto pTick = new MarketProvider(loop, saddr, "94.", "./tick.data", works);

	printf("init ok! start ...\n");
	pMarket->Start();
	pTick->Start();

	printf("main: pid is %d\n", gettid());
    uv_run(loop, UV_RUN_DEFAULT);
	return 0;
}
