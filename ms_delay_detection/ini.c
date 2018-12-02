/*************************************************************************
	> File Name: ini.c
	> Author: perrynzhou
	> Mail: perrynzhou@gmail.com
	> Created Time: Mon 28 Nov 2016 12:55:58 PM HKT
 ************************************************************************/

#include "ini.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define INI_LINE_BUFFER_SIZE 512
/* split string char */
static const char* g_split = "=";
static char* trim_space(char* s)
{
    if (s != NULL) {
        char buffer[INI_LINE_BUFFER_SIZE] = { '\0' };
        uint32_t len = 0;
        uint32_t i = 0;
        while (*s != '\0') {
            if (*s != ' ') {
                buffer[i] = *s;
                i++;
            }
            s++;
        }
        if ((len = strlen(buffer)) > 0) {
            memcpy(s, buffer, len);
            s[len - 1] = '\0'; //last of line content is '\n',must be remove
        }
    }
    return s;
}

inline static bool is_section_head(char* s)
{
    if (s != NULL) {
        uint32_t len = strlen(s);
        if (*s == '[' && *(s + len - 1) == ']') {
            return true;
        }
    }
    return false;
}

static section_item* section_item_link(section_item* im, char* section_name)
{
    section_item* rs = NULL;
    if (section_name == NULL || (rs = (section_item*)malloc(sizeof(*rs))) == NULL) {
        return NULL;
    }
    if (im == NULL) {
        im = rs;
    } else {
        rs->next = im;
        im = rs;
    }
    size_t len = strlen(section_name) - 2;
    im->section_name = (char*)malloc(len + 1);
    memset(im->section_name, '\0', len + 1);
    memcpy(im->section_name, section_name + 1, len);
    return im;
}

static bool kv_link(section_item* im, char* key, char* val)
{
    char* k = NULL;
    char* v = NULL;
    if (im != NULL && key != NULL && val != NULL) {
        uint32_t k_len = strlen(key);
        uint32_t v_len = strlen(val);
        k = (char*)malloc(k_len + 1);
        v = (char*)malloc(v_len + 1);
        if (k == NULL || v == NULL) {
            goto _ERROR;
        }
        struct kv_s* p = (struct kv_s*)malloc(sizeof(*p));
        if (p == NULL) {
            goto _ERROR;
        }
        memset(k, '\0', k_len + 1);
        memset(v, '\0', v_len + 1);
        memcpy(k, key, k_len);
        memcpy(v, val, v_len);
        p->k = k;
        p->v = v;
        if (im->head != NULL) {
            p->next = im->head;
            im->head = p;
        } else {
            im->head = p;
        }
        return true;
    }
    return false;
_ERROR:
    if (!k) {
        free(k);
    }
    if (!v) {
        free(v);
    }
    return false;
}

ini* ini_create(const char* file)
{
    ini* in = NULL;
    FILE* f = NULL;
    if (file == NULL || (f = fopen(file, "r")) == NULL) {
        return in;
    }
    size_t len = strlen(g_split);
    char line[INI_LINE_BUFFER_SIZE] = { '\0' };
    section_item* head = NULL;
    in = (ini*)malloc(sizeof(*in));
    if (in != NULL) {
        while (fgets(line, INI_LINE_BUFFER_SIZE, f) != NULL) {
            char* s = trim_space(line);
            uint32_t s_len = strlen(s);
            if (s_len == 0 || *s == '#') {
                continue;
            }
            if (is_section_head(s)) {
                head = section_item_link(head, s);
                __sync_fetch_and_add(&(in->size), 1);
            } else {
                char* tmp = strstr(s, g_split);
                *tmp = '\0';
                char* v = ++tmp;
                char* k = s;
                if (!kv_link(head, k, v)) {
                    fprintf(stdout, "kv linenk error\n");
                }
            }
        }
        in->head = head;
    }
    if (f != NULL) {
        fclose(f);
    }
    return in;
}

void ini_destroy(ini* in)
{
    if (in != NULL) {
        section_item* im = in->head;
        while (im != NULL) {
            kv* cur = im->head;
            while (cur != NULL) {
                kv* p = cur->next;
                cur->next = NULL;
                if (cur->k != NULL) {
                    free(cur->k);
                    cur->k = NULL;
                }
                if (cur->v) {
                    free(cur->v);
                    cur->v = NULL;
                }
                if (cur != NULL) {
                    free(cur);
                    cur = NULL;
                }
                cur = p;
            }
            im = im->next;
        }
        free(in);
        in = NULL;
    }
}

section_item* ini_get_item(ini* in, const char* sec_name)
{
    if (in == NULL || sec_name == NULL || strlen(sec_name) == 0) {
        return NULL;
    }
    section_item* cur = in->head;
    while (cur != NULL) {
        if (memcmp(cur->section_name, sec_name, strlen(sec_name)) == 0) {
            return cur;
        }
        cur = cur->next;
    }
    return cur;
}

bool ini_contain_key(ini* in, const char* sec_name, const char* key)
{
    if (in == NULL || sec_name == NULL || key == NULL) {
        return false;
    }
    section_item* cur = in->head;
    section_item* target = NULL;
    size_t key_len = strlen(key);
    size_t section_len = strlen(sec_name);
    if (key_len == 0 || section_len == 0) {
        return false;
    }
    while (cur != NULL) {
        if (memcmp(cur->section_name, sec_name, strlen(sec_name)) == 0) {
            target = cur;
            kv* tmp = cur->head;
            while (tmp != NULL) {
                kv* next = tmp->next;
                if (memcpy(tmp->k, key, key_len) == 0) {
                    return true;
                }
            }
        }
        cur = cur->next;
    }
    return false;
}

char* ini_get_value(ini* in, const char* sec_name, const char* key)
{
    if (in == NULL || sec_name == NULL || key == NULL) {
        return NULL;
    }
    if (in->size > 0) {
        section_item* im = in->head;
        size_t key_len = strlen(key);
        size_t sec_len = strlen(sec_name);
        while (im != NULL) {
            if (memcmp(im->section_name, sec_name, sec_len) == 0) {
                kv* t = im->head;
                while (t != NULL) {
                    if (memcmp(t->k, key, key_len) == 0) {
                        return t->v;
                    }
                    t = t->next;
                }
            }
            im = im->next;
        }
    }
    return NULL;
}
char* section_item_value(section_item* it, const char* key)
{
    char* value = NULL;
    kv* cur = it->head;
    while (cur != NULL) {
        if (strncmp(key, cur->k, strlen(cur->k)) == 0) {
            value = cur->v;
            break;
        }
        cur = cur->next;
    }
    return value;
}
#ifdef INI_TEST
int main(void)
{
    ini* in = ini_create("./1.cfg");
    if (in != NULL) {
        section_item* im = in->head;
        while (im != NULL) {
            kv* cur = im->head;
            while (cur != NULL) {
                kv* p = cur->next;
                fprintf(stdout, "-----key=%s,val=%s,current = %p,next=%p\n", cur->k, cur->v, cur, cur->next);
                cur = p;
            }
            im = im->next;
        }
    }
    fprintf(stdout, " ---find val:%s(key:%s)\n", ini_get_value(in, "a", "a1"), "a1");
    fprintf(stdout, " ---find val:%s(key:%s)\n", ini_get_value(in, "a", "axx"), "axx");
    fprintf(stdout, " ---find val:%s(key:%s)\n", ini_get_value(in, "a", "b2"), "b2");
    fprintf(stdout, " ---find val:%s(key:%s)\n", ini_get_value(in, "a", "a4"), "a4");
    fprintf(stdout, " ---find val:%s(key:%s)\n", ini_get_value(in, "mm", "ppp"), "ppp");
    fprintf(stdout, " ---find val:%s(key:%s)\n", ini_get_value(in, "mm", "yyy"), "yyy");
    fprintf(stdout, " ---find val:%s(key:%s)\n", ini_get_value(in, "asdfas", "yyy"), "yyy");
    ini_destroy(in);
    return 0;
}
#endif
