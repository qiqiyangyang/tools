/*************************************************************************
    > File Name: common.h
  > Author: perrynzhou
  > Mail: perrynzhou@gmail.com
  > Created Time: Fri 22 Sep 2017 02:01:34 AM EDT
 ************************************************************************/

#ifndef _COMMON_H
#define _COMMON_H
#include "ini.h"
#include "vector.h"
static const char* base_names[] = { "master", "slave" };
static const char* rep_delay_create_sql = "create table IF NOT EXISTS  ? (data bigint unsigned)";
static const char* rep_delay_select_sql = "select data from ?";
vector* vector_mysql_connections(ini* cfg);
char* str_to_lower(char* src);
bool is_digit(const char* src);
#endif
