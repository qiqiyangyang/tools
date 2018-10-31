/*************************************************************************
    > File Name: main.c
  > Author: perrynzhou
  > Mail: perrynzhou@gmail.com
  > Created Time: Wed 20 Sep 2017 05:43:01 AM AKDT
 ************************************************************************/

#include "common.h"
#include "connection_base.h"
#include "connection_event.h"
#include "ini.h"
#include "vector.h"
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#define DBINSTANCE_SIZE 64
#define CONF_FILE "./mysql.cfg"
int main(int argc, char* argv[])
{

    if (argc != 2) {
        fprintf(stdout, "usage:./delay [second_time]\n");
        fprintf(stdout, "            --second_time default seconds is zero\n");
        return -1;
    }
    int time = 0;
    if (is_digit(argv[1])) {
        time = atoi(argv[1]);
    } else {
        time = 1;
    }
    ini* cfg = ini_create(CONF_FILE);
    vector* vec = vector_mysql_connections(cfg);

    int size = vector_size(vec);
    connection* master[DBINSTANCE_SIZE] = { NULL };
    connection* slave[DBINSTANCE_SIZE] = { NULL };
    connection_event* inst[DBINSTANCE_SIZE] = { NULL };
    int master_count = 0;
    int slave_count = 0;
    int inst_count = 0;
    for (int i = 0; i < size; i++) {
        connection* cur = vector_get(vec, i);
        if (cur->is_master) {
            master[master_count] = cur;
            master_count++;
        } else {
            slave[slave_count] = cur;
            slave_count++;
        }
    }
    for (int i = 0; i < master_count; i++) {
        for (int j = 0; j < slave_count; j++) {
            inst[j] = connection_event_create(&master[i], &slave[j]);
        }
    }
    do {
        for (int i = 0; i < master_count; i++) {
            for (int j = 0; j < slave_count; j++) {
                connection_event_refresh(inst[j]);
                connection_refresh_basic(inst[j]->master);
                connection_refresh_basic(inst[j]->slave);
                //printf("real_seconds:%lf\n", inst[j]->real_seconds);
                //connection_event_refresh(inst[j]);
                fprintf(stdout, "master addr:%s:%d, tps:%lld, qps:%lld\n", inst[j]->master->host, inst[j]->master->port, inst[j]->master->tps, inst[j]->master->qps);
                fprintf(stdout, "slave  addr:%s:%d, tps:%lld, qps:%lld, io_thread:%d, sql_thread:%d, seconds_behind_real:%lf, seconds_behind_master:%d\n", inst[j]->slave->host, inst[j]->slave->port, inst[j]->slave->tps, inst[j]->slave->qps, inst[j]->slave->io_thd, inst[j]->slave->sql_thd, (double)(inst[j]->real_seconds / CLOCKS_PER_SEC), inst[j]->slave->seconds_behind_master);
                //fprintf(stdout, "-----------------------------------%s-----------------------------------\n", inst[j]->time);
            }
            sleep(time);
        }
    } while (1);
    //fprintf(stdout, "master size=%d,slave size=%d\n", master_count, slave_count);

    for (int i = 0; i < master_count; i++) {
        for (int j = 0; j < slave_count; j++) {
            connection_event_destroy((connection_event**)&inst[j]);
        }
    }
    return -1;
}
