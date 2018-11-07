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
#include <strings.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#define STD_BACKLOG (1024)
#define STD_MAX_BLOCK_LEN (64)
#define STD_BLOCK_LEN (1 * 1024 * 1024)
static void rand_str(char *buf, size_t len)
{
  const char o[] = "0123456789ABCDEF";
  size_t olen = sizeof(o) / sizeof(char);
  for (size_t i = 0; i < len - 1; i++)
  {
    buf[i] = o[(olen + rand()) % olen];
  }
  buf[len - 1] = '\0';
}
static uint64_t string_to_uint64(const char *s, bool flag)
{

  char *save_ptr = NULL;
  uint64_t uint_value = strtold(s, &save_ptr);
  uint64_t val = 0;
  if (strlen(save_ptr) > 0)
  {
    if (strncasecmp(save_ptr, "mb", 2) == 0 || strncasecmp(save_ptr, "m", 1) == 0)
    {
      if (!flag && uint_value > STD_MAX_BLOCK_LEN)
      {
        val = STD_MAX_BLOCK_LEN * 1024 * 1024;
      }
      else
      {
        val = uint_value * 1024 * 1024;
      }
    }
    else if (strncasecmp(save_ptr, "kb", 2) == 0 || strncasecmp(save_ptr, "k", 1) == 0)
    {
      val = uint_value * 1024;
    }
    else if (flag && strncasecmp(save_ptr, "gb", 2) == 0 || strncasecmp(save_ptr, "g", 1) == 0)
    {
      val = uint_value * 1024 * 1024 * 1024;
    }
  }
  return val;
}
int usage(const char *s)
{
  fprintf(stdout, "\nusage: %s {host} {port} {count} {total_mb_size} {option_block_size}\n", s);
  fprintf(stdout, "        --host              server host address\n");
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
    bytes = string_to_uint64(argv[4], true);
    if (bytes == 0)
    {
      bytes = STD_BLOCK_LEN * 10;
    }
  }
  if (argv[5] != NULL)
  {
    block_bytes = string_to_uint64(argv[5], false);
    if (block_bytes == 0)
    {
      block_bytes = STD_BLOCK_LEN;
    }
  }
  if (bytes < block_bytes)
  {
    return usage(argv[0]);
  }
  int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock == -1)
  {
    fprintf(stdout, "socket error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }

  //bytes = ((bytes + (block_bytes - 1)) & ~(block_bytes - 1))
  if (bytes % block_bytes != 0)
  {
    bytes = (bytes / block_bytes + 1) * block_bytes;
    fprintf(stdout, "\n****client command convert: %s %s %s %s %.3f Mib %.3f Mib\n", argv[0], argv[1], argv[2], argv[3], (double)bytes / 1024 / 1024, (double)block_bytes / 1024 / 1024);
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

  fprintf(stdout, "****client start transmission data to server[%s],packet size:%.3f Mib\n", server_ip, (double)bytes * count / 1024 / 1024);
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

    double total_bytes = sm.length * sm.number * sm.count;
    char buf[1024] = {'\0'};
    sprintf((char *)&buf, "|****client finish %.3f Mib to server[%s],", (double)total_bytes / 1024 / 1024, server_ip);
    struct timeval start;
    struct timeval finish;
    gettimeofday(&start, NULL);
    double single_bytes = total_bytes / sm.count;
    for (int j = 1; j <= sm.count; j++)
    {
      struct timeval cur_start;
      struct timeval cur_finish;
      gettimeofday(&cur_start, NULL);
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
      gettimeofday(&cur_finish, NULL);
      double cur_elapsed = (double)((cur_finish.tv_sec - cur_start.tv_sec) * 1000000 + (cur_finish.tv_usec - cur_start.tv_usec)) / 1000000;
      fprintf(stdout, "    |--client finish  transmission %.3f Mib,elapsed: %.3f seconds,bandwidth :%.3f Mib/s\n", single_bytes / 1024 / 1024, cur_elapsed, single_bytes / 1024 / 1024 / cur_elapsed);
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