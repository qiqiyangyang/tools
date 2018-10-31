/*************************************************************************
    > File Name: event.h
  > Author: perrynzhou
  > Mail: perrynzhou@gmail.com
  > Created Time: Wed 20 Sep 2017 06:46:23 AM AKDT
 ************************************************************************/

#ifndef _CONNECTION_EVENT_H
#define _CONNECTION_EVENT_H
#define CONNECTION_EVENT_BUF_SIZE 128
#include "connection_base.h"
#include <pthread.h>
typedef struct connection_event_s {
    char time[CONNECTION_EVENT_BUF_SIZE];
    //double real_seconds;
    double real_seconds;
    char table_name[CONNECTION_EVENT_BUF_SIZE];
    uint64_t start_value;
    connection* master;
    connection* slave;
    pthread_mutex_t lock;
} connection_event;
connection_event* connection_event_create(connection** master, connection** slave);
void connection_event_refresh(connection_event* ce);
void connection_event_destroy(connection_event** ce);
#endif
