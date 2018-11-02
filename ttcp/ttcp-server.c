/*************************************************************************
  > File Name: ttcp-server.c
  > Author:perrynzhou
  > Mail:perrynzhou@gmail.com
  > Created Time: 三 10/31 10:15:53 2018
 ************************************************************************/

#include "dict.h"
#include "ttcp.h"
#include <assert.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/time.h>
#define STD_BACKLOG (1024)
#define MAX_BUCKET_SIZE (16383)
static pthread_mutex_t lock;
static dict *map;

typedef struct request
{
  session_msg sm;
  int cfd;
  pthread_t id;
  pthread_t parent_id;
} request;
static request *request_create(int cfd)
{
  request *req = (request *)calloc(1, sizeof(*req));
  req->cfd = cfd;
}
static int client_cmp(const void *key1, const void *key2)
{
  int *ptr1 = (int *)key1;
  int *ptr2 = (int *)key2;
  if (*ptr1 == *ptr2)
  {
    return 0;
  }
  else if (*ptr1 > *ptr2)
  {
    return -1;
  }
  else
  {
    return 1;
  }
}
size_t client_len(const void *key) { return sizeof(int); }
static int init_socket(int port)
{
  int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (sock == -1)
  {
    printf("socket error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (bind(sock, (struct sockaddr *)&addr, sizeof(struct sockaddr)) < 0)
  {
    printf("bind  error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }
  if (listen(sock, STD_BACKLOG) < 0)
  {
    printf("listen error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }
  int yes;
  if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
  {
    printf("setsockopt error: %s(errno: %d)\n", strerror(errno), errno);
    return -1;
  }
  return sock;
}
void handle_connection(void *arg)
{

  struct timeval start;
  struct timeval finish;
  gettimeofday(&start, NULL);
  request *req = (request *)arg;

  // got client ip address
  char client_ip[128] = {'\0'};
  get_sock_info(req->cfd, (char *)&client_ip);

  int read_len = read_n(req->cfd, &req->sm, sizeof(session_msg));
  int write_len = 0;
  assert(read_len == sizeof(req->sm));

  char result[2048] = {'\0'};
  double pkg_size = (double)req->sm.length / 1024 / 1024;
  double total = (req->sm.length * req->sm.number * req->sm.count) / 1024 / 1024;
  fprintf(stdout, " **new connection %s ,session:{count=%d,number=%d,packet length=%.4fMib}, runing in %ld thread,handing by sub-thread %ld\n", client_ip, req->sm.count, req->sm.number, pkg_size, req->parent_id, pthread_self());

  size_t ac_size = sizeof(payload_msg) + req->sm.length;
  payload_msg *pm = (payload_msg *)calloc(1, ac_size);
  assert(pm != NULL);
  pm->length = req->sm.length;

  for (int j = 0; j < req->sm.count; j++)
  {
    for (int i = 0; i < req->sm.number; i++)
    {
      if (read_n(req->cfd, &pm->length, sizeof(pm->length)) !=
          sizeof(pm->length))
      {
        perror("read");
        break;
      }
      assert(pm->length == req->sm.length);
      // read data
      if (read_n(req->cfd, (char *)pm->data, pm->length) != pm->length)
      {
        perror("read");
        break;
      }
      uint32_t ack = htonl(pm->length);
      if (write_n(req->cfd, &ack, sizeof(int)) != sizeof(uint32_t))
      {
        perror("write");
        break;
      }
    }
  }
  gettimeofday(&finish, NULL);
  double elapsed = (double)((finish.tv_sec - start.tv_sec) * 1000000 + (finish.tv_usec - start.tv_usec)) / 1000000;
  fprintf(stdout, "    **thread %ld elapsed:%.3f seconds,recieve %.3f Mib from %s, network-bandwidth:%.3f MiB/s\n\n", pthread_self(), elapsed, total, client_ip, total / elapsed);

  if (pm != NULL)
  {
    free(pm);
    pm = NULL;
  }
  int cfd_dup = req->cfd;
  close(req->cfd);
  dict_del(map, &cfd_dup);
  pthread_detach(req->id);
}
void handle_accept_request(void *arg)
{
  int *sock = (int *)arg;
  fprintf(stdout, "|######### start worker thread %ld for accpet connection\n", pthread_self());
  while (1)
  {
    int cfd = accept(*sock, (struct sockaddr *)NULL, NULL);
    if (cfd == -1)
    {
      continue;
    }
    request *req = (request *)dict_fetch(map, &cfd);
    if (req == NULL)
    {
      req = request_create(cfd);
      req->parent_id = pthread_self();
      pthread_mutex_lock(&lock);
      if (dict_add(map, &req->cfd, req, 0) != 0)
      {
        fprintf(stdout, "dict error: add %p into dict failed\n", req);
        free(req);
        req = NULL;
        pthread_mutex_unlock(&lock);
        break;
      }
      pthread_mutex_unlock(&lock);
      pthread_create(&req->id, NULL, (void *)&handle_connection, (void *)req);
    }
  }
}

int main(int argc, char *argv[])
{
  if (argc == 2 && strncmp(argv[1], "-h", 2) == 0)
  {
    fprintf(stdout, "\nusage:%s {port} {thread_count}\n", argv[0]);
    fprintf(stdout, "          --port           listen port for server\n");
    fprintf(stdout, "          --thread_count   thread for worker size\n");
    fprintf(stdout, "example:%s 6789 4   \n", argv[0]);
    return -1;
  }
  int port = (NULL == argv[1]) ? 6789 : atoi(argv[1]);
  int sock = init_socket(port);
  if (sock == -1)
  {
    return -1;
  }
  int thd_size = (NULL == argv[2]) ? 1 : atoi(argv[2]);
  if (thd_size <= 0)
  {
    thd_size = 1;
  }
  signal(SIGPIPE, SIG_IGN);
  pthread_mutex_init(&lock, NULL);
  if (map == NULL)
  {
    map = dict_create(NULL, MAX_BUCKET_SIZE, 0);
    map->key_cmp = &client_cmp;
    map->key_len = &client_len;
    map->key_destroy = map->val_destroy = &free;
  }
  fprintf(stdout, "|************perrynzhou@gmail.com****************|\n\n");
  pthread_t thds[thd_size];
  fprintf(stdout, "|************start ttcp server running at %s:%d************|\n", "127.0.0.1", port);
  for (int i = 0; i < thd_size; i++)
  {
    pthread_create(&thds[i], NULL, (void *)&handle_accept_request,
                   (void *)&sock);
  }
  for (int i = 0; i < thd_size; i++)
  {
    pthread_join(thds[i], NULL);
  }
  if (sock != -1)
  {
    close(sock);
  }
  dict_destroy(map);
  fprintf(stdout, "|************sserver stop at %d************|\n", port);
}