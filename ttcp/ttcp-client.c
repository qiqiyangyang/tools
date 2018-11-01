/*************************************************************************
  > File Name: ttcp-client.c
  > Author:perrynzhou
  > Mail:perrynzhou@gmail.com
  > Created Time: 三 10/31 10:14:59 2018
 ************************************************************************/

#include "ttcp.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define BACKLOG (1024)
void signal_handle(int signo) {}
static void rand_str(char *buf, size_t len)
{
  const char o[] = "0123456789ABCDEF";
  size_t olen = sizeof(o) / sizeof(char);
  for (size_t i = 0; i < len; i++)
  {
    buf[i] = o[(olen + rand()) % olen];
  }
}
int usage(const char *s)
{
  fprintf(stdout, "\nusage: %s {host} {port} {block_size} {total_mb_size}\n", s);
  return -1;
}
int main(int argc, char *argv[])
{
  if (argc != 5)
  {
    return usage(argv[0]);
  }
  char *host = argv[1];
  int port = atoi(argv[2]);
  int block_size = atoi(argv[3]);
  int bytes = atoi(argv[4]) * 1024 * 1024;
  int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock == -1)
  {
    printf("socket error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }
  if (block_size % 8 != 0)
  {
    block_size = ((block_size + 0x7) & ~(0x7));
  }
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(host);
  addr.sin_port = htons(port);

  session_msg sm;
  sm.number = bytes / block_size;
  sm.length = block_size;
  if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == -1)
  {
    close(sock);
    printf("connect error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }

    char server_ip[128] = {'\0'};
    get_sock_info(sock, (char *)&server_ip);
    size_t len = strlen(server_ip);


  fprintf(stdout,"****client start transmission data to server[%s]\n",server_ip);
  int w = write_n(sock, &sm, sizeof(sm));
  if (w != sizeof(sm))
  {
    printf("send error: %s(errno: %d)\n", strerror(errno), errno);
  }
  else
  {

    size_t ac_size = sizeof(payload_msg) + sm.length;
    payload_msg *pm = (payload_msg *)calloc(1, ac_size);
    pm->length = block_size;
    rand_str(pm->data, pm->length);
    pm->data[pm->length - 1] = '\0';

    double total = (sizeof(char) * pm->length * sm.number) / 1024 / 1024;
    char buf[1024] = {'\0'};
    sprintf((char *)&buf, " ****client write %.3f Mib to server[%s],", total,server_ip);
    clock_t start = clock();
    clock_t finish;
    for (int i = 0; i < sm.number; i++)
    {
      int write_len = write_n(sock,pm, ac_size);
      assert(write_len == ac_size);
      int  ack = 0;
      int read_len = read_n(sock, &ack, sizeof(int));
      assert(read_len == sizeof(ack));
      ack = ntohl(ack);
      assert(ack == sm.length);
    }
    finish = clock();
    double elapsed = (double)(finish - start)/CLOCKS_PER_SEC;
    fprintf(stdout, "%s elapsed: %.3f seconds,network bandwidth :%.3f MiB/s\n", buf,elapsed, bytes / 1024 / 1024 / elapsed);
    if (pm != NULL)
    {
      free(pm);
    }
  }
  close(sock);
}