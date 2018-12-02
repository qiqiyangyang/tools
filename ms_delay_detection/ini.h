/*************************************************************************
	> File Name: ini.h
	> Author: perrynzhou
	> Mail: 715169549@qq.com
	> Created Time: Mon 28 Nov 2016 12:49:08 PM HKT
 ************************************************************************/

#ifndef _INI_H
#define _INI_H
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
typedef struct kv_s {
    char* k;
    char* v;
    struct kv_s* next;
} kv;
/*global struct */
typedef struct section_item_s {
    char* section_name;
    kv* head;
    struct section_item_s* next;
} section_item;
typedef struct ini_s {
    size_t size;
    section_item* head;
} ini;
ini* ini_create(const char* file);
void ini_destroy(ini* in);
section_item* ini_get_item(ini* in, const char* sec_name);
char* section_item_value(section_item* it, const char* key);
bool ini_contain_keys(ini* in, const char* sec_name);
char* ini_get_value(ini* in, const char* sec_name, const char* key);
#endif
