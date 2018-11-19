#ifndef  _DBA_TOOL_H
#define  _DBA_TOOL_H
/*read file to init mysql conenct */
int read_my_cnf (const char *file, char *newfile, int rewirte);
/* current default config that need maxsize of memory */
int total_memory (char *args[]);
/* sleep 1 second get some status */
int get_status_persecond (char *args[]);
/* kill connection or kill query */
int kill_sql (char *args[], int flag);
/* get explain of sql that in processlist that running */
int explain (char *args[]);
/* get current running mysqld variables */
void show_variables (char *args[], const char *name);
#endif /**/
