/*************************************************************************
    > File Name: connection.c
  > Author: perrynzhou
  > Mail: perrynzhou@gmail.com
  > Created Time: Fri 22 Sep 2017 01:34:40 AM EDT
 ************************************************************************/

#include "connection_base.h"
#include "common.h"
#include "ini.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#define MYSQL_HOST_KEY "host"
#define MYSQL_PORT_KEY "port"
#define MYSQL_USER_KEY "user"
#define MYSQL_PASSWORD_KEY "password"
//static const char* tqps = "select variable_name,variable_value from information_schema.global_status where variable_name in ('questions','uptime','com_commit','com_rollback')";
static const char* tqps = "show global status";
static const char* table_create = "create table IF NOT EXISTS  test.delay_data_?(data bigint unsigned)";
static const char* slave_sql = "show slave status";
static const char* thread_status[] = { "Slave_IO_Running", "Slave_SQL_Running", "Seconds_Behind_Master" };
connection* connection_create(section_item* si)
{
    connection* cn = NULL;
    if (si != NULL) {
        cn = (connection*)calloc(1, sizeof(*cn));
        char* host = section_item_value(si, MYSQL_HOST_KEY);
        char* user = section_item_value(si, MYSQL_USER_KEY);
        char* port_str = section_item_value(si, MYSQL_PORT_KEY);
        char* pwd = section_item_value(si, MYSQL_PASSWORD_KEY);
        if (host == NULL) {
            strncpy(cn->host, MYSQL_HOST, strlen(MYSQL_HOST));
        } else {
            strncpy(cn->host, host, strlen(host));
        }
        if (user == NULL) {
            user = MYSQL_USER;
        }
        if (port_str == NULL) {
            cn->port = MYSQL_PORT1;
        } else {
            cn->port = atoi(port_str);
        }
        if (pwd == NULL) {
            pwd = MYSQL_PASSWORD;
        }
        if (strstr(si->section_name, base_names[0]) != NULL) {
            cn->is_master = true;
        } else {
            cn->is_master = false;
        }
        memset((char*)&cn->name, '\0', MYSQL_BUF_SIZE);
        strncpy(cn->name, si->section_name, strlen(si->section_name));
        mysql_init(&cn->c);
        if (mysql_real_connect(&cn->c, host, user, pwd, "mysql", cn->port, NULL, 0) == NULL) {
            fprintf(stdout, "connection error:%s\n", mysql_error(&cn->c));
            goto _ERROR;
        }
        if (cn != NULL && cn->is_master) {
            char tmp[1024] = { '\0' };
            strncpy(tmp, table_create, strlen(table_create));
            char sql[1024] = { '\0' };
            char* find = strchr(tmp, '?');
            *find = '\0';
            strncpy(sql, tmp, strlen(tmp));
            strncpy(sql + strlen(sql), si->section_name, strlen(si->section_name));
            ++find;
            strncpy(sql + strlen(sql), find, strlen(find));
            if (mysql_real_query(&cn->c, sql, strlen(sql)) != 0) {
                fprintf(stdout, "query err:%s\n", mysql_error(&cn->c));
                goto _ERROR;
            }
        }
    }
    cn->tps = cn->qps = cn->seconds_behind_master = 0;
    return cn;
_ERROR:
    if (cn != NULL) {
        free(cn);
        cn = NULL;
    }
    return NULL;
}
void connection_refresh_basic(connection* cc)
{
    if (cc != NULL) {
        connection* ct = cc;
        if (mysql_real_query(&ct->c, tqps, strlen(tqps)) != 0) {
            fprintf(stdout, "%s\n", mysql_error(&ct->c));
            return;
        }

        MYSQL_RES* res = mysql_store_result(&ct->c);
        MYSQL_ROW rows;
        uint64_t questions = 0;
        uint64_t uptime = 0;
        uint64_t commits = 0;
        uint64_t rollback = 0;
        char* tmp;
        while ((rows = mysql_fetch_row(res))) {
            char* vname = rows[0];
            if (strncasecmp(vname, "uptime", 6) == 0) {
                uptime = strtol(rows[1], &tmp, 10);
            } else if (strncasecmp(vname, "questions", 9) == 0) {
                questions = strtol(rows[1], &tmp, 10);
            } else if (strncasecmp(vname, "com_commit", 10) == 0) {
                commits = strtol(rows[1], &tmp, 10);
            } else if (strncasecmp(vname, "com_rollback", 12) == 0) {
                rollback = strtol(rows[1], &tmp, 10);
            } else {
                continue;
            }
            tmp = NULL;
        }
        ct->tps = (commits + rollback) / uptime;
        ct->qps = questions / uptime;
        if (!ct->is_master) {
            if ((mysql_real_query(&ct->c, slave_sql, strlen(slave_sql))) != 0) {
                fprintf(stdout, "%s\n", mysql_error(&ct->c));
            }

            //
            res = mysql_store_result(&ct->c);

            MYSQL_FIELD* fields = mysql_fetch_fields(res);
            rows = mysql_fetch_row(res);
            uint32_t num_fields = mysql_num_fields(res);
            for (uint32_t i = 0; i < num_fields; i++) {
                char* field_name = fields[i].name;
                if (strncasecmp(thread_status[0], field_name, strlen(field_name)) == 0) {
                    if (strncasecmp(rows[i], "yes", 3) == 0) {
                        ct->sql_thd = true;
                    } else {
                        ct->sql_thd = false;
                    }
                }
                if (strncasecmp(thread_status[1], field_name, strlen(field_name)) == 0) {
                    if (strncasecmp(rows[i], "yes", 3) == 0) {
                        ct->io_thd = true;
                    } else {
                        ct->io_thd = false;
                    }
                }
                if (strncasecmp(thread_status[2], field_name, strlen(field_name)) == 0) {
                    //fprintf(stdout, "---rows[i] = %s\n", rows[i]);
                    if (!ct->io_thd || !ct->sql_thd || rows[i] == NULL) {
                        ct->seconds_behind_master = -1;

                    } else {
                        ct->seconds_behind_master = atoi(rows[i]);
                    }
                    //fprintf(stdout, "---rows[i] = %s,ct->seconds_behind_master=%d\n", rows[i], ct->seconds_behind_master);
                    break;
                }
            }
        }
    }
}
void connection_destroy(connection** cc)
{
    if (cc != NULL && *cc != NULL) {
        connection* c = *cc;
        mysql_close(&c->c);
        free(c);
        *cc = NULL;
    }
}
