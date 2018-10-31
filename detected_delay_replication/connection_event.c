/*************************************************************************
    > File Name: event.c
  > Author: perrynzhou
  > Mail: perrynzhou@gmail.com
  > Created Time: Wed 20 Sep 2017 06:49:52 AM AKDT
 ************************************************************************/

#include "connection_event.h"
#include "common.h"
#include <mysql.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
static const char* table_name = "test.delay_data_?";
inline static uint64_t connection_event_timestamp()
{
    struct timeval tv;
    uint64_t timestamp;
    if (!gettimeofday(&tv, NULL)) {
        timestamp = ((long long int)tv.tv_sec) * 1000000ll + (long long int)tv.tv_usec;
    } else {
        srand(time(NULL));
        timestamp = rand();
    }
    return timestamp;
}
connection_event* connection_event_create(connection** master, connection** slave)
{
    if (master == NULL || *master == NULL || slave == NULL || *slave == NULL) {
        return NULL;
    }
    char value[64] = { '\0' };
    uint64_t v = connection_event_timestamp();
    sprintf(value, "%lld", v);

    connection_event* ce = NULL;
    char sql_table[64] = { '\0' };
    strncpy(sql_table, table_name, strlen(table_name) - 2);
    strncpy(sql_table + strlen(sql_table), "_", 1);
    strncpy(sql_table + strlen(sql_table), (*master)->name, strlen((*master)->name));

    ce = (connection_event*)calloc(1, sizeof(*ce));
    ce->master = *master;
    ce->slave = *slave;
    ce->start_value = v;
    strncpy(ce->table_name, sql_table, strlen(sql_table));

    char master_create_table_sql[1024] = { '\0' };
    char buf[128] = { '\0' };
    strncpy(buf, rep_delay_create_sql, strlen(rep_delay_create_sql));
    char* prev_sql = strchr(buf, '?');
    *prev_sql = '\0';
    prev_sql++;
    strncpy(master_create_table_sql, buf, strlen(buf));
    strncpy(master_create_table_sql + strlen(master_create_table_sql), sql_table, strlen(sql_table));
    strncpy(master_create_table_sql + strlen(master_create_table_sql), prev_sql, strlen(prev_sql));
    if (mysql_real_query(&ce->master->c, master_create_table_sql, strlen(master_create_table_sql)) != 0) {
        fprintf(stdout, "master create table :%s\n", mysql_error(&ce->master->c));
        goto _ERROR;
    }

    char master_truncate_sql[512] = { '\0' };
    char* t_p1 = "truncate table ";
    strncpy(master_truncate_sql, t_p1, strlen(t_p1));
    strncpy(master_truncate_sql + strlen(master_truncate_sql), sql_table, strlen(sql_table));
    if (mysql_real_query(&ce->master->c, master_truncate_sql, strlen(master_truncate_sql)) != 0) {
        fprintf(stdout, "master create table :%s\n", mysql_error(&ce->master->c));
        goto _ERROR;
    }

    char master_insert_sql[512] = { '\0' };
    char* d_p1 = "insert into ";
    char* d_p2 = "(data) values(";
    char* d_p3 = ")";
    strncpy(master_insert_sql, d_p1, strlen(d_p1));
    strncpy(master_insert_sql + strlen(master_insert_sql), sql_table, strlen(sql_table));
    sprintf(master_insert_sql + strlen(master_insert_sql), d_p2, strlen(d_p2));
    sprintf(master_insert_sql + strlen(master_insert_sql), value, strlen(value));
    sprintf(master_insert_sql + strlen(master_insert_sql), d_p3, strlen(d_p3));
    if (mysql_real_query(&ce->master->c, master_insert_sql, strlen(master_insert_sql)) != 0) {
        fprintf(stdout, "master insert table:%s\n", mysql_error(&ce->master->c));
        goto _ERROR;
    }
    pthread_mutex_init(&ce->lock, NULL);
    return ce;
_ERROR:
    if (ce != NULL) {
        connection_event_destroy(&ce);
        ce = NULL;
    }
    return NULL;
}
void connection_event_refresh(connection_event* ce)
{
    if (ce == NULL) {
        return;
    }
    char value[64] = { '\0' };
    sprintf(value, "%lld", ce->start_value);

    uint64_t v = connection_event_timestamp();
    char value0[64] = { '\0' };
    sprintf(value0, "%lld", v);
    char master_update_sql[512] = { '\0' };
    char* d_p1 = "update  ";
    char* d_p2 = " set data= ";
    char* d_p3 = " where data=";
    strncpy(master_update_sql, d_p1, strlen(d_p1));
    strncpy(master_update_sql + strlen(master_update_sql), ce->table_name, strlen(ce->table_name));
    sprintf(master_update_sql + strlen(master_update_sql), d_p2, strlen(d_p2));
    sprintf(master_update_sql + strlen(master_update_sql), value0, strlen(value0));
    sprintf(master_update_sql + strlen(master_update_sql), d_p3, strlen(d_p3));
    sprintf(master_update_sql + strlen(master_update_sql), value, strlen(value));

    char slave_select_sql[256] = { '\0' };
    const char* select_sql = "select data from ";
    const char* select_sql_cond = " where data =";
    strncpy(slave_select_sql, select_sql, strlen(select_sql));
    strncpy(slave_select_sql + strlen(slave_select_sql), ce->table_name, strlen(ce->table_name));
    strncpy(slave_select_sql + strlen(slave_select_sql), select_sql_cond, strlen(select_sql_cond));
    strncpy(slave_select_sql + strlen(slave_select_sql), value0, strlen(value0));

    pthread_mutex_lock(&ce->lock);
    ce->start_value = v;
    clock_t start, finish;
    double duration;
    start = clock();
    if (mysql_real_query(&ce->master->c, master_update_sql, strlen(master_update_sql)) != 0) {
        pthread_mutex_destroy(&ce->lock);
        fprintf(stdout, "master :%s\n", mysql_error(&ce->master->c));
        return;
    }
    my_ulonglong rows_size = 0;
    MYSQL_RES* res;
    clock_t loop_time_start = clock();
    do {

        if (mysql_real_query(&ce->slave->c, slave_select_sql, strlen(slave_select_sql)) != 0) {
            fprintf(stdout, "slave:%s\n", mysql_error(&ce->slave->c));
            break;
        }
        res = mysql_store_result(&ce->slave->c);
        rows_size = mysql_num_rows(res);
        if (((double)(clock() - loop_time_start) / CLOCKS_PER_SEC) > (0.1)) {
            break;
        }
    } while (rows_size == 0);
    finish = clock();
    if (ce->real_seconds == 0) {
        ce->real_seconds = finish - start;
    }
    if (!ce->slave->io_thd || !ce->slave->sql_thd) {
        ce->real_seconds = ce->real_seconds + (finish - start) + CLOCKS_PER_SEC;
    } else {
        ce->real_seconds = (finish - start);
    }
    //fprintf(stdout, " interval :%ld,per_sec:%ld,re:%f\n", (finish - start), CLOCKS_PER_SEC, ce->real_seconds);
    time_t now;
    struct tm* tm_now;

    time(&now);
    tm_now = localtime(&now);
    sprintf(ce->time, "%d-%d-%d %d:%d:%d", tm_now->tm_year + 1900, tm_now->tm_mon, tm_now->tm_mday, tm_now->tm_hour, tm_now->tm_min, tm_now->tm_sec);
    pthread_mutex_unlock(&ce->lock);
}
void connection_event_destroy(connection_event** ce)
{
    if (ce != NULL && *ce != NULL) {
        connection* master = (*ce)->master;
        connection_destroy(&master);
        connection* slave = (*ce)->slave;
        connection_destroy(&slave);
        free(*ce);
        *ce = NULL;
    }
}
