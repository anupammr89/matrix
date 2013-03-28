
#TARGETS = zht_c_binding zht_benchmark zht_server
TARGETS = client server
CC = gcc
CCFLAGS = -g -I${USER_INCLUDE} -L${USER_LIB}
#LIBFLAGS = -lstdc++ -lrt -lpthread -lm -lc -lprotobuf -lprotobuf-c
LIBFLAGS = -lstdc++ -lrt -lpthread -lm -lc -lprotobuf

all:	$(TARGETS)
	

zht_c_binding: c_zhtclient_main.o c_zhtclient.o c_zhtclientStd.o cpp_zhtclient.o meta.pb-c.o lru_cache.o meta.pb.o net_util.o novoht.o zht_util.o
	$(CC) $(CCFLAGS) -o $@ $^ $(LIBFLAGS)


#zht_benchmark: benchmark_client.o cpp_zhtclient.o meta.pb-c.o lru_cache.o meta.pb.o net_util.o novoht.o zht_util.o
#	$(CC) $(CCFLAGS) -o $@ $^ $(LIBFLAGS)

#zht_server: server_general.o meta.pb-c.o lru_cache.o meta.pb.o net_util.o novoht.o zht_util.o
#        $(CC) $(CCFLAGS) -o $@ $^ $(LIBFLAGS)

client: benchmark_client.o cpp_zhtclient.o lru_cache.o meta.pb.o net_util.o novoht.o zht_util.o matrix_util.o matrix_client.o
	$(CC) $(CCFLAGS) -o $@ $^ $(LIBFLAGS)

server: server_general.o cpp_zhtclient.o lru_cache.o meta.pb.o net_util.o novoht.o zht_util.o matrix_util.o matrix_client.o matrix_server.o
	$(CC) $(CCFLAGS) -o $@ $^ $(LIBFLAGS)



%.o: %.c
	$(CC) $(CCFLAGS) -c $< $^ $(LIBFLAGS)
	
%.o: %.cpp
	$(CC) $(CCFLAGS) -c $< $^ $(LIBFLAGS)
	
%.o: %.cc
	$(CC) $(CCFLAGS) -c $< $^ $(LIBFLAGS)



c_zhtclient_main.o:		c_zhtclient_main.c
c_zhtclient.o:			c_zhtclient.cpp
c_zhtclientStd.o:		c_zhtclientStd.cpp

benchmark_client.o:		benchmark_client.cpp
cpp_zhtclient.o:		cpp_zhtclient.cpp

server_general.o:		server_general.cpp

meta.pb-c.o:			meta.pb-c.c
lru_cache.o:			lru_cache.cpp	
meta.pb.o:				meta.pb.cc
net_util.o:				net_util.cpp
novoht.o:				novoht.cpp
zht_util.o:				zht_util.cpp
matrix_util.o:				matrix_util.cpp
matrix_client.o:			matrix_client.cpp
mtrix_server.o:				matrix_server.cpp

.PHONY:	clean
	

clean:
	rm *.o
	rm $(TARGETS)
	rm *.gch*
	rm *.bin*
