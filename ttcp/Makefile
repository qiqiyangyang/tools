all:
	rm -rf ttcp_server ttcp_client
	gcc -std=c99 -g dict.h dict.c ttcp.h ttcp.c ttcp-server.c  -lpthread -o  ttcp_server
	gcc -std=c99 -g ttcp.h ttcp.c ttcp-client.c -o ttcp_client
