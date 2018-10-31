```
--------------------------[host]--------------------------
host.name                        |Debian
os.info                          |linux,3.16.0-4-amd64
host.ip                          |192.168.1.127,172.17.42.1
uptime                           |4106
--------------------------[cpu]--------------------------
cpu0                             |2712
cpu1                             |2712
cpu-total.user                   |159.26
cpu-total.system                 |17.71
cpu-total.idle                   |8018.5
cpu-total.nice                   |0
cpu-total.iowait                 |3.88
cpu-total.steal                  |0
--------------------------[memory]--------------------------
mem.total                        |997mb
mem.used                         |626mb
mem.available                    |370mb
mem.used_pct                     |62.8%
mem.free                         |319mb
mem.buffers                      |19mb
mem.cached                       |135mb
swap.total                       |2047mb
swap.used                        |26mb
swap.free                        |2021mb
swap.used_pct                    |1.3%
swap.sin                         |18993152
swap.sout                        |2179264512
--------------------------[workloader]--------------------------
load.1                           |0.08
load.5                           |0.08
load.15                          |0.08
procs.running                    |1
procs.blocked                    |0
--------------------------[disk]--------------------------
dm-0.total                       |mount:/,size:62117mb
dm-0.free                        |mount:/,size:53394mb
dm-0.used                        |mount:/,size:5545mb
sda1.total                       |mount:/boot,size:235mb
sda1.free                        |mount:/boot,size:189mb
sda1.used                        |mount:/boot,size:33mb
dm-0.total                       |mount:/var/lib/docker/aufs,size:62117mb
dm-0.free                        |mount:/var/lib/docker/aufs,size:53394mb
dm-0.used                        |mount:/var/lib/docker/aufs,size:5545mb
--------------------------[process]--------------------------
pid                              |9296
name                             |mysqld
thread.size                      |22
fd.size                          |36
process.rss                      |457mb
process.vms                      |1528mb
process.swap                     |0mb
io.readcount                     |950
io.writecount                    |35
io.readsize                      |25mb
io.writesize                     |0mb

--------------------------[oom]--------------------------
oom                              |Jul 28 22:27:02 Debian kernel: [  168.773680] a.out invoked oom-killer: gfp_mask=0x201da, order=0, oom_score_adj=0
oom                              |Jul 28 22:27:02 Debian kernel: [  168.773710]  [<ffffffff81142dbd>] ? oom_kill_process+0x21d/0x370
--------------------------[network]--------------------------
lo.mtu                           |65536
lo.macaddr                       |
eth0.mtu                         |1500
eth0.macaddr                     |00:1c:42:44:ee:c9
docker0.mtu                      |1500
docker0.macaddr                  |02:42:f1:ea:5c:71
all.send_kb.size                 |9266kb
all.recv_kb.size                 |4396kb
all.send_package.count           |16141
all.recv_package_count           |29387
all.send_errors_count            |0
all.recv_errors_count            |0
--------------------------[mysql]--------------------------
binlog_cache_size                |0.03 MB
innodb_additional_mem_pool_size  |8.00 MB
innodb_buffer_pool_instances     |8
innodb_buffer_pool_size          |0.12 GB
innodb_file_per_table            |on
innodb_flush_log_at_trx_commit   |1
innodb_io_capacity               |200
innodb_log_files_in_group        |2
innodb_log_file_size             |48.00 MB
innodb_read_io_threads           |4
innodb_write_io_threads          |4
join_buffer_size                 |0.25 MB
key_buffer_size                  |8.00 MB
max_allowed_packet               |4.00 MB
max_connections                  |151
max_heap_table_size              |16.00 MB
read_buffer_size                 |0.12 MB
read_rnd_buffer_size             |0.25 MB
sort_buffer_size                 |0.25 MB
thread_stack                     |0.25 MB
version                          |5.6.36-debug-log

select                           |4
update                           |0
delete                           |0
insert                           |0
qps                              |0
tps                              |0
read_page_from_bp                |2215 request
read_page_from_disk              |0 request
buffer_pool_size                 |0.12 GB
buffer_pool_usage                |4.10 %
threads_create                   |1
threads_connected                |1
threads_running                  |1
threads_cache                    |0
connection_errors_max_connections|0
connection_errors_internal       |0
aborted_connects                 |0
binlog_cache_disk_use            |0
create_tmp_disk_tables           |0
slow_queries                     |0
uptime                           |0h
```

## Contributors

 - perrynzhou@gmail.com(zhou.lin)  微信：perrynzhou

