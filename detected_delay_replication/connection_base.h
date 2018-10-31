/*************************************************************************
    > File Name: connection.h
  > Author: perrynzhou
  > Mail: perrynzhou@gmail.com
  > Created Time: Wed 20 Sep 2017 04:36:40 AM AKDT
 ************************************************************************/

#ifndef _CONNECTION_H
#define _CONNECTION_H
#include "ini.h"
#include <mysql.h>
#include <stdbool.h>
#include <stdint.h>
#define MYSQL_HOST "127.0.0.1"
#define MYSQL_PASSWORD "root"
#define MYSQL_PORT1 3306
#define MYSQL_USER "root"
#define MYSQL_BUF_SIZE 1024
#define MYSQL_REP_TABLE rep_delay
typedef struct connection_s {
    bool is_master;
    char name[MYSQL_BUF_SIZE];
    char host[MYSQL_BUF_SIZE];
    int port;
    MYSQL c;
    uint64_t tps;
    uint64_t qps;
    uint64_t seconds_behind_master;
    bool sql_thd;
    bool io_thd;
} connection;
connection* connection_create(section_item* si);
void connection_refresh_basic(connection* cc);
void connection_destroy(connection** cc);
void connection_print(connection *c);
#endif
