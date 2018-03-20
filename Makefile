# date 2018-03-01 sunquan
.PHONY:clean  
CFLAG := -Wall -g -std=c++11
LIBRARIES :=  -L../../../bin/linux
LIBRARIES += -lpthread -lprotobuf -lzmq -luv -lemsproto 
BIN := recv_quote

all:$(BIN)


%.o: %.cpp
		g++ $(CFLAG) -c $< -o $@

recv_quote: recv_quote.o main.o
		g++  $(CFLAG) $(LIBRARIES)  $^ -o $@
clean:
		rm $(BIN) *.o  *.data