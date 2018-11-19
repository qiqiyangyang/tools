#include "dba_tool.h"
#include <assert.h>
#include <fcntl.h>
#include <mysql.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#define bufSize 512
#define max 4096
#define db_max 64
#define sql_max 4096
const char* status_sql = "select variable_name,variable_value from information_schema.global_status where  variable_name like '%com%' order by variable_name";
typedef struct _sdb {
    char db[db_max];
    char sql[sql_max];
} sdb;
static char* my_tolower(char* s)
{
    int len = strlen(s);
    int i = 0;
    while (i < len) {
        if (s[i] >= 65 && s[i] <= 90) {
            s[i] += 32;
        }
        i++;
    }
    return s;
}

static int check_comment(char* buf)
{
    if (buf == NULL) {
        return -1;
    }
    int len = strlen(buf), count = 0, i = 0;
    if (buf[0] == '#') {
        return 0;
    }
    while (i < len) {
        if (isspace(buf[i]) != 0 || iscntrl(buf[i]) != 0) {
            count++;
        }
        if (buf[i] == '#' && count == i) {
            return 0;
        }
        i++;
    }
    if (len == count) {
        return 0;
    }
    return 1;
}

int read_my_cnf(const char* file, char* newfile, int rewirte)
{
    int fd = open(file, O_RDONLY, 0666);
    int new_fd;
    int isw = 0;
    if (newfile != NULL && strlen(newfile) > 0 && rewirte == 1) {
        isw = 1;
        new_fd = open(newfile, O_RDWR | O_CREAT | O_TRUNC, 0666);
    }
    if (fd == -1 || (new_fd == -1)) {
        fprintf(stderr, "-----open file error------\n");
        return -1;
    }
    char buf[bufSize] = { '\0' };
    FILE* input = fdopen(fd, "r");
    assert(input != NULL);
    while (fgets(buf, bufSize, input) != NULL) {
        if (check_comment(buf) == 1) {
            if (isw == 1) {
                int len = strlen(buf);
                if (write(new_fd, buf, len) != len) {
                    return -1;
                }
            }
            printf("%s", buf);
        }
        memset(buf, bufSize, '\0');
    }
    if (isw == 1 && new_fd != -1) {
        close(new_fd);
    }
    if (fd != -1 && input != NULL) {
        fclose(input);
        close(fd);
    }
    return 0;
}

int total_memory(char* args[])
{
    MYSQL_RES* res_pt;
    MYSQL_FIELD* fd;
    MYSQL_ROW sqlrow;
    MYSQL *tmp, con;
    long long perMem = 0, gMem = 0, tMem = 0;
    int timeout = 2;
    int max_connection = 0;
    char* sql[6] = {
        "select variable_name,variable_value  from global_variables \
                where variable_name in ('read_buffer_size','read_rnd_buffer_size','sort_buffer_size', \
                                        'thread_stack','join_buffer_size','binlog_cache_size','tmp_table_size',\
                                        'innodb_buffer_pool_size','innodb_additional_mem_pool_size','tmp_table_size',\
                'max_heap_table_size','max_binlog_size','innodb_page_size','innodb_log_file_size',\
                'innodb_log_buffer_size') order by variable_name",
        "select sum(variable_value) perMem from global_variables \
                where variable_name in ('read_buffer_size','read_rnd_buffer_size','sort_buffer_size', \
                                       'thread_stack','join_buffer_size','binlog_cache_size','tmp_table_size')",
        "select sum(variable_value) perMem from global_variables where variable_name in  \
                        ('innodb_buffer_pool_size','innodb_additional_mem_pool_size',\
                         'innodb_log_buffer_size','key_buffer_size','query_cache_size')",
        "select variable_name,variable_value  from global_variables \
                 where variable_name in ('datadir','log_bin','binlog_format','innodb_io_capacity',\
        'innodb_flush_log_at_trx_commit','slow_query_log','sync_binlog','log_queries_not_using_indexes',\
                'max_connections','max_user_connections','max_allowed_packet','innodb_flush_method',\
        'innodb_read_io_threads','innodb_write_io_threads','innodb_purge_threads','innodb_adaptive_hash_index',\
        'innodb_file_per_table','innodb_open_files') order by variable_name",
        "select table_schema,round(sum(data_length)/1024/1024,2) MB from tables \
            group by table_schema order by MB desc",
        "select table_schema,table_name,round(sum(data_length)/1024/1024,2) MB from tables where table_schema not in ('mysql','information_schema','performance_schema') \
        group by table_schema,table_name order by MB desc limit 20"
    };
    tmp = mysql_init(&con);
    mysql_options(&con, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    tmp = mysql_real_connect(&con, args[0] + 2, args[1] + 2, args[2] + 2, "information_schema", atoi(args[3] + 2), NULL, 0);
    if (tmp == NULL) {
        return -1;
    }
    //get all variable
    int exec = mysql_real_query(tmp, sql[0], strlen(sql[0]));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        fd = mysql_fetch_fields(res_pt);
        int column_len = mysql_num_fields(res_pt);
        int start = 0;
        fprintf(stdout, "--------------------------------thread  and gloabl memory--------------------------------\n");
        while (sqlrow = mysql_fetch_row(res_pt)) {
            for (start = 0; start < column_len; start++) {
                if (start == column_len - 1) {
                    int v = atoi(sqlrow[start]) / 1024;
                    fprintf(stdout, "   | %-*d  KB\n", 15, v);
                    break;
                }
                fprintf(stdout, "  %-*s", 45, my_tolower(sqlrow[start]));
            }
        }
        mysql_free_result(res_pt);
    }
    //sum thread per memory
    exec = mysql_real_query(tmp, sql[1], strlen(sql[1]));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        while (sqlrow = mysql_fetch_row(res_pt)) {
            perMem = atoi(sqlrow[0]);
        }
        mysql_free_result(res_pt);
    }
    //sum gloabl memory
    exec = mysql_real_query(tmp, sql[2], strlen(sql[2]));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        while (sqlrow = mysql_fetch_row(res_pt)) {
            gMem = atoi(sqlrow[0]);
        }
        mysql_free_result(res_pt);
    }
    //get max_connection
    exec = mysql_real_query(tmp, sql[3], strlen(sql[3]));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        fd = mysql_fetch_fields(res_pt);
        int column_len = mysql_num_fields(res_pt);
        int start = 0;
        fprintf(stdout, "--------------------------------core config--------------------------------\n");
        while (sqlrow = mysql_fetch_row(res_pt)) {
            for (start = 0; start < column_len; start++) {
                if (start == column_len - 1) {
                    fprintf(stdout, "   | %-*s\n", 15, sqlrow[start]);
                    break;
                }
                fprintf(stdout, "  %-*s", 45, my_tolower(sqlrow[start]));
                if (strncmp(sqlrow[start], "max_connections", strlen(sqlrow[start])) == 0) {
                    max_connection = atoi(sqlrow[column_len - 1]);
                }
            }
        }
        mysql_free_result(res_pt);
    }
    tMem = (perMem * max_connection + gMem) / 1024 / 1024;
    fprintf(stdout, "--------------------------------cost max memory--------------------------------\n");
    fprintf(stdout, "  %-*s", 45, "thread consume memory");
    fprintf(stdout, "   | %-*d MB\n", 15, perMem * max_connection / 1024 / 1024);
    fprintf(stdout, "  %-*s", 45, "global config memory");
    fprintf(stdout, "   | %-*d MB\n", 15, gMem / 1024 / 1024);
    fprintf(stdout, "  %-*s", 45, "max cost memory");
    fprintf(stdout, "   | %-*d MB\n", 15, tMem);
    //db size info
    exec = mysql_real_query(tmp, sql[4], strlen(sql[4]));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        fd = mysql_fetch_fields(res_pt);
        int column_len = mysql_num_fields(res_pt);
        int start = 0;
        fprintf(stdout, "--------------------------------database size info--------------------------------\n");
        while (sqlrow = mysql_fetch_row(res_pt)) {
            for (start = 0; start < column_len; start++) {
                if (start == column_len - 1) {
                    fprintf(stdout, "   | %-*s MB\n", 15, sqlrow[start]);
                    break;
                }
                fprintf(stdout, "  %-*s", 45, sqlrow[start]);
            }
        }
        mysql_free_result(res_pt);
    }

    // db size
    exec = mysql_real_query(tmp, sql[5], strlen(sql[5]));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        fd = mysql_fetch_fields(res_pt);
        int column_len = mysql_num_fields(res_pt);
        int start = 0;
        char schema[96] = { '\0' };
        fprintf(stdout, "--------------------------------table size top20--------------------------------\n");
        while (sqlrow = mysql_fetch_row(res_pt)) {
            memset(schema, '\0', 96);
            int len = strlen(schema);
            for (start = 0; start < column_len; start++) {
                if (start == column_len - 1) {
                    fprintf(stdout, "  %-*s", 45, schema);
                    fprintf(stdout, "   | %-*s MB\n", 15, sqlrow[start]);
                    break;
                }
                if (start == column_len - 2) {
                    strncat(schema, ".", 1);
                    strncat(schema, sqlrow[start], strlen(sqlrow[start]));
                } else {
                    strncpy(schema + len, sqlrow[start], strlen(sqlrow[start]));
                }
                //fprintf(stdout," schema = %s",schema);
            }
        }
        mysql_free_result(res_pt);
    }
    if (tmp != NULL) {
        mysql_close(tmp);
    }
    FILE* os_f = fopen("/proc/meminfo", "r");
    char cur_buf[96] = { '\0' };
    char cur_v[32] = { '\0' };
    fprintf(stdout, "--------------------------------os memory info--------------------------------\n");
    while (fgets(cur_buf, 128, os_f) != NULL) {
        char* k = strtok(cur_buf, ":");
        char* v = strtok(NULL, ":");
        int len = strlen(v), vlen = 0, i = 0, j = 0;
        while (i < len) {
            if (isdigit(v[i]) != 0) {
                cur_v[j] = v[i];
                j++;
            }
            i++;
        }
        if (strncmp(k, "MemTotal", 8) == 0 || strncmp(k, "MemFree", 7) == 0 || strncmp(k, "SwapTotal", 9) == 0 || strncmp(k, "SwapFree", 8) == 0) {
            long long vi = atoi(cur_v);
            fprintf(stdout, "  %-*s", 45, k);
            fprintf(stdout, "   | %-*d MB\n", 15, vi / 1024);
        }
        memset(cur_buf, '\0', 128);
        memset(cur_v, '\0', 32);
    }
    if (os_f != NULL) {
        fclose(os_f);
    }
    return 0;
}

int get_status_persecond(char* args[])
{
    MYSQL_RES* res_pt;
    MYSQL_FIELD* fd;
    MYSQL_ROW sqlrow;
    MYSQL *tmp, con;
    int timeout = 2, exec = -1, i = 0;
    int max_connection = 0;
    char buf[128] = { '\0' };
    tmp = mysql_init(&con);
    mysql_options(&con, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    tmp = mysql_real_connect(&con, args[0] + 2, args[1] + 2, args[2] + 2, "information_schema", atoi(args[3] + 2), NULL, 0);
    if (tmp == NULL) {
        fprintf(stderr, "%s", mysql_error(&con));
        return -1;
    }
    fprintf(stdout, "*************status per second***************\n\n");
    while (1) {
        exec = mysql_real_query(tmp, status_sql, strlen(status_sql));
        if (exec != 0) {
            fprintf(stderr, "%s", mysql_error(&con));
            return -1;
        }
        int column_len = 0, start = 0;
        if ((res_pt = mysql_store_result(&con)) != NULL) {
            int start = 0;
            column_len = mysql_num_fields(res_pt);
            fd = mysql_fetch_fields(res_pt);
            struct tm* tm_now;
            time_t now;
            time(&now);
            tm_now = gmtime(&now);
            sprintf(buf, "%d-%d-%d %d:%d:%d", tm_now->tm_year + 1900, tm_now->tm_mon, tm_now->tm_mday, tm_now->tm_hour, tm_now->tm_min, tm_now->tm_sec);
            printf("-------------------%s----------------\n", buf);
            memset(buf, '\0', 128);
            while (sqlrow = mysql_fetch_row(res_pt)) {
                for (start = 0; start < column_len; start++) {
                    printf("  %-*s : ", 34, sqlrow[0]);
                    printf("  %-*s\n", 10, sqlrow[1]);
                }
            }
        }
        mysql_free_result(res_pt);
        sleep(1);
    }
    if (tmp != NULL) {
        mysql_close(tmp);
    }
    return 0;
}

void help(char* s)
{
    fprintf(stdout, "usage:\n");
    fprintf(stdout, "    %s format file1\n", s);
    fprintf(stdout, "    %s rebuild old_file1 new_file1\n", s);
    fprintf(stdout, "    %s info     -h127.0.0.1 -uroot -proot -P3306\n", s);
    fprintf(stdout, "    %s dbstatus -h127.0.0.1 -uroot -proot -P3306\n", s);
    fprintf(stdout, "    %s conf    -h127.0.0.1 -uroot -proot -P3306\n", s);
    fprintf(stdout, "    %s killq    -h127.0.0.1 -uroot -proot -P3306\n", s);
    fprintf(stdout, "    %s killc    -h127.0.0.1 -uroot -proot -P3306\n", s);
    fprintf(stdout, "    %s explain  -h127.0.0.1 -uroot -proot -P3306\n", s);
}

void show_variables(char* args[], const char* name)
{
    MYSQL_RES* res_pt;
    MYSQL_FIELD* fd;
    MYSQL_ROW sqlrow;
    MYSQL *tmp, con;
    long long perMem = 0, gMem = 0, tMem = 0;
    int timeout = 2;
    int max_connection = 0;
    char* var_start = "select variable_name,variable_value  from global_variables ";
    char* var_order = "order by variable_name";
    char var_sql[2048] = { '\0' };
    strncpy(var_sql, var_start, strlen(var_start));
    if (name != NULL) {
        char* p = " where variable_name like '%";
        strncpy(var_sql + strlen(var_sql), p, strlen(p));
        strncpy(var_sql + strlen(var_sql), name, strlen(name));
        strncpy(var_sql + strlen(var_sql), "%''", 2);
    }
    strncpy(var_sql + strlen(var_sql), var_order, strlen(var_order));
    tmp = mysql_init(&con);
    mysql_options(&con, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    tmp = mysql_real_connect(&con, args[0] + 2, args[1] + 2, args[2] + 2, "information_schema", atoi(args[3] + 2), NULL, 0);
    if (tmp == NULL) {
        return;
    }
    int exec = mysql_real_query(tmp, var_sql, strlen(var_sql));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        fd = mysql_fetch_fields(res_pt);
        int column_len = mysql_num_fields(res_pt);
        int start = 0;
        fprintf(stdout, "*************Current MySQL Configuration*************\n");
        while (sqlrow = mysql_fetch_row(res_pt)) {
            for (start = 0; start < column_len; start++) {
                if (start == column_len - 1) {
                    fprintf(stdout, "  |%-*s\n", 20, my_tolower(sqlrow[start]));
                    break;
                }
                fprintf(stdout, "  %-*s", 45, my_tolower(sqlrow[start]));
            }
        }
        mysql_free_result(res_pt);
    }
    if (tmp != NULL) {
        mysql_close(tmp);
    }
}

static char* time2Str()
{
    static char buf[64];
    memset(buf, '\0', 64);
    struct tm* tm_now;
    time_t now;
    time(&now);
    tm_now = gmtime(&now);
    sprintf(buf, "%d-%d-%d_%d_%d_%d", tm_now->tm_year + 1900, tm_now->tm_mon, tm_now->tm_mday, tm_now->tm_hour, tm_now->tm_min, tm_now->tm_sec);
    return buf;
}

int kill_sql(char* args[], int flag)
{
    MYSQL_RES* res_pt;
    MYSQL_FIELD* fd;
    MYSQL_ROW sqlrow;
    MYSQL *tmp, con;
    int timeout = 2, exec = -1, i = 0;
    int max_connection = 0;
    char buf[128] = { '\0' };
    tmp = mysql_init(&con);
    char* sql = { "select id,info from information_schema.processlist order by id" };
    mysql_options(&con, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    tmp = mysql_real_connect(&con, args[0] + 2, args[1] + 2, args[2] + 2, "information_schema", atoi(args[3] + 2), NULL, 0);
    if (tmp == NULL) {
        fprintf(stderr, "%s\n", mysql_error(&con));
        return -1;
    }
    exec = mysql_real_query(tmp, sql, strlen(sql));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        int start = 0;
        fd = mysql_fetch_fields(res_pt);
        int my = mysql_thread_id(tmp);
        if (my < 0) {
            fprintf(stdout, "%s\n", mysql_error(tmp));
            mysql_free_result(res_pt);
            return -1;
        }
        memset(buf, '\0', 128);
        switch (flag) {
        case 0:
            strncpy(buf, "kill query ", 11);
            break;
        case 1:
            strncpy(buf, "kill connection ", 16);
            break;
        }
        char* t_sql = (char*)malloc(2048);
        while (sqlrow = mysql_fetch_row(res_pt)) {

            if (sqlrow[0] != NULL) {
                unsigned int id = atoi(sqlrow[0]);
                if (id == mysql_thread_id(tmp)) {
                    continue;
                }
                char k[32];
                memset(k, '\0', 32);
                memset(t_sql, '\0', 2048);
                if (sqlrow[1] != NULL && strlen(sqlrow[1]) > 0) {
                    strncpy(t_sql, sqlrow[1], strlen(sqlrow[1]));
                } else {
                    strncpy(t_sql, "NULL", 4);
                }
                strncpy(k, buf, strlen(buf));
                strncpy(k + strlen(k), sqlrow[0], strlen(sqlrow[0]));
                int cur = atoi(sqlrow[0]);
                if (sqlrow[1] != NULL && strncmp(my_tolower(sqlrow[1]), sql, strlen(sql)) == 0) {
                    continue;
                }
                if (cur != my && mysql_real_query(tmp, k, strlen(k)) != 0) {
                    fprintf(stdout, "%s\n", mysql_error(tmp));
                    return -1;
                }
                fprintf(stdout, "---- kill thread %d :%s \n", id, t_sql);
            }
        }
        mysql_free_result(res_pt);
        if (tmp != NULL) {
            free(t_sql);
            mysql_close(tmp);
            t_sql = NULL;
        }
    }
    return 0;
}

int explain(char* args[])
{
    MYSQL_RES* res_pt;
    MYSQL_FIELD* fd;
    MYSQL_ROW sqlrow, rows;
    MYSQL *tmp, con;
    int timeout = 2, exec = -1, i = 0;
    int max_connection = 0;
    char buf[128] = { '\0' };
    tmp = mysql_init(&con);
    char* sql = { "select db,info from information_schema.processlist order by id" };
    mysql_options(&con, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
    tmp = mysql_real_connect(&con, args[0] + 2, args[1] + 2, args[2] + 2, "information_schema", atoi(args[3] + 2), NULL, 0);
    if (tmp == NULL) {
        fprintf(stderr, "[err]:%s", mysql_error(&con));
        return -1;
    }
    exec = mysql_real_query(tmp, sql, strlen(sql));
    if (exec == 0 && (res_pt = mysql_store_result(&con)) != NULL) {
        int i = 0;
        fd = mysql_fetch_fields(res_pt);
        int my = mysql_thread_id(tmp);
        int len = mysql_num_rows(res_pt) - 1;
        sdb* info[max];
        for (i = 0; i < max; i++) {
            info[i] = NULL;
        }
        i = 0;
        while (sqlrow = mysql_fetch_row(res_pt)) {
            if (sqlrow[0] != NULL && sqlrow[1] != NULL && strlen(sqlrow[1]) > 0) {
                if (strncmp(my_tolower(sqlrow[1]), sql, strlen(sql)) == 0) {
                    continue;
                }
                info[i] = (sdb*)malloc(sizeof(sdb));
                if (info[i] == NULL) {
                    perror("malloc");
                    return -1;
                }
                memset(info[i]->db, '\0', db_max);
                memset(info[i]->sql, '\0', sql_max);
                strncpy(info[i]->db, "use  ", 5);
                strncpy(info[i]->db + strlen(info[i]->db), sqlrow[0], strlen(sqlrow[0]));
                strncpy(info[i]->sql, "explain ", 8);
                strncpy(info[i]->sql + strlen(info[i]->sql), sqlrow[1], strlen(sqlrow[1]));
                i++;
            }
        }
        if (res_pt != NULL) {
            mysql_free_result(res_pt);
        }
        for (i = 0; i < max && info[i] != NULL; i++) {
            //fprintf(stdout,"info db= %s,sql =%s\n",info[i]->db,info[i]->sql);
            if (mysql_real_query(tmp, info[i]->db, strlen(info[i]->db)) != 0) {
                fprintf(stdout, "[err] :%s\n", mysql_error(tmp));
                break;
            }
            int fe = mysql_real_query(tmp, info[i]->sql, strlen(info[i]->sql));
            if (fe == 0 && (res_pt = mysql_store_result(tmp)) != NULL) {
                fprintf(stdout, "---------------------------------explain %d----------------------------------------\n", i);
                int start = 0;
                int len = mysql_num_fields(res_pt);
                fd = mysql_fetch_fields(res_pt);
                while (rows = mysql_fetch_row(res_pt)) {
                    printf("  %-*s : ", 14, "sql");
                    printf("  %-*s\n", 14, info[i]->sql + 8);
                    printf("  %-*s : ", 14, "schema");
                    printf("  %-*s\n", 14, info[i]->db + 5);
                    for (start = 0; start < len; start++) {
                        printf("  %-*s : ", 14, fd[start].name);
                        printf("  %-*s\n", 14, rows[start]);
                    }
                    fprintf(stdout, "************************************************************************************\n");
                }
                if (res_pt != NULL) {
                    mysql_free_result(res_pt);
                }
            }
        }
        for (i = 0; i < max; i++) {
            if (info[i] != NULL) {
                free(info[i]);
                info[i] = NULL;
            }
        }
        if (tmp != NULL) {
            mysql_close(tmp);
        }
    }
    return 0;
}

int main(int argc, char* args[])
{
    if (argc <= 1) {
        help(args[0]);
    }
    //Backup(NULL);
    if (args[1] != NULL && args[2] != NULL && strncmp(args[1], "format", 6) == 0) {
        read_my_cnf(args[2], NULL, 0);
    } else if (args[1] != NULL && args[2] != NULL && args[3] != NULL && strncmp(args[1], "rebuild", 7) == 0) {
        read_my_cnf(args[2], args[3], 1);
    } else if (args[1] != NULL && args[2] != NULL && args[3] != NULL && args[4] != NULL && args[5] != NULL) {
        if (strncmp(args[1], "info", 4) == 0) {
            char* ps[4] = { args[2], args[3], args[4], args[5] };
            total_memory(ps);
        }
        if (strncmp(args[1], "dbstatus", 8) == 0) {
            char* ps[4] = { args[2], args[3], args[4], args[5] };
            get_status_persecond(ps);
        }
        if (strncmp(args[1], "killq", 5) == 0) {
            char* ps[4] = { args[2], args[3], args[4], args[5] };
            kill_sql(ps, 0);
        }
        if (strncmp(args[1], "killc", 5) == 0) {
            char* ps[4] = { args[2], args[3], args[4], args[5] };
            kill_sql(ps, 1);
        }
        if (strncmp(args[1], "explain", 7) == 0) {
            char* ps[4] = { args[2], args[3], args[4], args[5] };
            explain(ps);
        }
        if (strncmp(args[1], "conf", 4) == 0) {
            char* ps[4] = { args[2], args[3], args[4], args[5] };
            show_variables(ps, args[6]);
        }
    }
    return 0;
}
