/*************************************************************************
  > File Name: ttcp-client.c
  > Author:perrynzhou
  > Mail:perrynzhou@gmail.com
  > Created Time: 三 10/31 10:14:59 2018
 ************************************************************************/

#include "ttcp-common.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <ctype.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define STD_BACKLOG (1024)
#define STD_BLOCK_LEN (1 * 1024 * 1024)
static void rand_str(char *buf, size_t len)
{
  const char o[] = "0123456789ABCDEF";
  size_t olen = sizeof(o) / sizeof(char);
  for (size_t i = 0; i < len; i++)
  {
    buf[i] = o[(olen + rand()) % olen];
  }
}
static bool str_is_int(char *s)
{
  while (*s != '\0')
  {
    if (!isdigit(*s))
    {
      return false;
    }
    s++;
  }
  return true;
}
static uint64_t uint_convert(char *s, bool flag)
{
  size_t len = strlen(s);
  char buf[66] = {'\0'};
  char *save_ptr;
  uint64_t block_bytes = 0;
  uint64_t ut = strtoul(s, &save_ptr, 10);
  strncpy((char *)&buf, s, len - 2);
  if (str_is_int((char *)&buf))
  {
    if (strncmp(save_ptr, "kb", 2) == 0 || strncmp(save_ptr, "k", 1) == 0)
    {
      block_bytes = ut * 1024;
    }
    else if (strncmp(save_ptr, "mb", 2) == 0 || strncmp(save_ptr, "m", 1) == 0)
    {
      block_bytes = ut * 1024 * 1024;
    }
    else if (flag && strncmp(save_ptr, "gb", 2) == 0 || strncmp(save_ptr, "g", 1) == 0)
    {
      block_bytes = ut * 1024 * 1024 * 1024;
    }
  }
  return block_bytes;
}
int usage(const char *s)
{
  fprintf(stdout, "\nusage: %s {host} {port} {count} {total_mb_size} {option_block_size}\n", s);
  fprintf(stdout, "        --port              server port\n");
  fprintf(stdout, "        --count             times for sending data to server\n");
  fprintf(stdout, "        --total_mb_size     total mb size(kb|k|mb|m|gb|mb)\n");
  fprintf(stdout, "        --option_block_size block size(kb|k|mb|m}\n");
  fprintf(stdout, "example:%s  127.0.0.1 6789 10  3gb  2mb\n", s);
  return -1;
}
int main(int argc, char *argv[])
{
  if (argc == 2 && strncmp(argv[1], "-h", 2) == 0)
  {
    return usage(argv[0]);
  }
  char *host = "127.0.0.1";
  int port = 6789;
  uint64_t count = 1;
  uint64_t block_bytes = STD_BLOCK_LEN;
  uint64_t bytes = 0;
  if (argv[1] != NULL)
  {
    host = argv[1];
  }
  if (argv[2] != NULL)
  {
    port = atoi(argv[2]);
  }
  if (argv[3] != NULL)
  {
    count = atoi(argv[3]);
  }
  if (argv[4] != NULL)
  {
    bytes = uint_convert(argv[4], true);
    if (bytes == 0)
    {
      bytes = STD_BLOCK_LEN * 10;
    }
  }
  if (argv[5] != NULL)
  {
    block_bytes = uint_convert(argv[5], false);
    if (block_bytes == 0)
    {
      block_bytes = STD_BLOCK_LEN;
    }
  }
  int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock == -1)
  {
    fprintf(stdout, "socket error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr(host);
  addr.sin_port = htons(port);

  session_msg sm;
  sm.number = bytes / block_bytes;
  sm.length = block_bytes;
  sm.count = count;
  if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) == -1)
  {
    close(sock);
    fprintf(stdout, "connect error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }

  char server_ip[128] = {'\0'};
  get_sock_info(sock, (char *)&server_ip);
  size_t len = strlen(server_ip);

  fprintf(stdout, "****client start transmission data to server[%s]\n", server_ip);
  int w = write_n(sock, &sm, sizeof(sm));
  if (w != sizeof(sm))
  {
    printf("send error: %s(errno: %d)\n", strerror(errno), errno);
  }
  else
  {

    size_t ac_size = sizeof(payload_msg) + sm.length;
    payload_msg *pm = (payload_msg *)calloc(1, ac_size);

    if (NULL == pm)
    {
      fprintf(stdout, "malloc error: %s(errno: %d)\n", strerror(errno), errno);
      return -1;
    }
    pm->length = sm.length;
    rand_str(pm->data, pm->length);
    pm->data[pm->length - 1] = '\0';

    double total_bytes = sm.length * sm.number * sm.count;
    char buf[1024] = {'\0'};
    sprintf((char *)&buf, " |****client write %.3f Mib to server[%s],", (double)total_bytes / 1024 / 1024, server_ip);
    struct timeval start;
    struct timeval finish;
    gettimeofday(&start, NULL);

    for (int j = 1; j <= sm.count; j++)
    {
      for (int i = 1; i <= sm.number; i++)
      {
        int write_len = write_n(sock, pm, ac_size);
        assert(write_len == ac_size);
        int ack = 0;
        int read_len = read_n(sock, &ack, sizeof(int));
        assert(read_len == sizeof(ack));
        ack = ntohl(ack);
        assert(ack == sm.length);
      }
      fprintf(stdout, "    ...client finish  transmission %.3f Mib\n", total_bytes / sm.count / 1024 / 1024);
    }
    gettimeofday(&finish, NULL);
    double elapsed = (double)((finish.tv_sec - start.tv_sec) * 1000000 + (finish.tv_usec - start.tv_usec)) / 1000000;
    fprintf(stdout, "%s elapsed: %.3f seconds,network bandwidth :%.3f MiB/s\n", buf, elapsed, total_bytes / 1024 / 1024 / elapsed);
    if (pm != NULL)
    {
      free(pm);
    }
  }
  close(sock);
}