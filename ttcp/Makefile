all:
	rm -rf ttcp_network_server ttcp_network_client
	gcc -std=c99 -g dict.h dict.c ttcp-common.h ttcp-common.c ttcp-network-server.c  -lpthread -o  ttcp_network_server
	gcc -std=c99 -g ttcp-common.h ttcp-common.c ttcp-network-client.c -o ttcp_network_client
