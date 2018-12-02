/*************************************************************************
    > File Name: common.c
  > Author: perrynzhou
  > Mail: perrynzhou@gmail.com
  > Created Time: Mon 09 Oct 2017 12:17:40 AM EDT
 ************************************************************************/

#include "common.h"
#include "connection_base.h"
#include <ctype.h>
#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
vector* vector_mysql_connections(ini* cfg)
{
    if (cfg == NULL || cfg->size <= 1) {
        return NULL;
    }
    int size = cfg->size;
    vector* vs = vector_create(size, NULL, NULL);
    section_item* item = cfg->head;
    int master_size = 0;
    while (item != NULL) {
        connection* ct = connection_create(item);
        if (ct != NULL && ct->is_master) {
            master_size++;
        }
        item = item->next;
        vector_add(vs, ct);
    }
    if (master_size == 0) {
        vector_destroy(vs);
        vs = NULL;
    }
    return vs;
}
char* str_to_lower(char* src)
{
    char* base = src;
    while (*base != '\0') {
        if (*base >= 'A' && *base <= 'Z') {
            *base ^= 0x20;
        }
    }
    return src;
}
inline bool is_digit(const char* src)
{
    while (*src != '\0') {
        if (!isdigit(*src)) {
            return false;
        }
        src++;
    }
    return true;
}
