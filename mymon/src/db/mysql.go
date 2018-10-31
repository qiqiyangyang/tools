package db

import (
	"fmt"
	"github.com/go-ini/ini"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native" // Native engine
	"inst"
	"os"
	"strconv"
	"strings"
)

const (
	host        = "host"
	port        = "port"
	user        = "user"
	password    = "password"
	dbsize_file = "./database_gb.txt"
)

var sql0 string = `select variable_name ,variable_value from information_schema.global_status where variable_name in (?) order by variable_name`

var condition string = "'Com_update','Com_update_multi','Com_select','Com_insert','Com_insert_select','Com_delete','Com_delete_multi','Com_rollback','Com_commit','Uptime','Questions','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_pages_total','Innodb_buffer_pool_pages_free','Innodb_page_size','Threads_cached','Threads_connected','Threads_created','Threads_running','Binlog_cache_disk_use','Created_tmp_disk_tables','Connection_errors_internal','Connection_errors_max_connections','Aborted_connects','Slow_queries','Innodb_row_lock_current_waits','Innodb_row_lock_time','Innodb_row_lock_time_avg','Innodb_row_lock_time_max','Innodb_row_lock_waits'"

var sql1 string = `select variable_name ,variable_value from information_schema.global_variables where variable_name in (?) order by variable_name`
var condition1 string = "'version','innodb_buffer_pool_size','innodb_additional_mem_pool_size','innodb_read_io_threads','innodb_write_io_threads','join_buffer_size','read_buffer_size','read_rnd_buffer_size','innodb_buffer_pool_instances','sort_buffer_size','max_heap_table_size','innodb_io_capacity','max_allowed_packet','thread_stack','max_connections','binlog_cache_size','innodb_file_per_table','innodb_flush_method','innodb_max_dirty_pages_pct','innodb_flush_log_at_trx_commit','key_buffer_size','innodb_log_file_size','innodb_log_files_in_group'"

var database_gb = "select now() time, round(sum(DATA_LENGTH)/1024/1024/1024,6) database_size_Gb from information_schema.tables"

var table_top_10 = "select table_schema,table_name,round(sum(data_length)/1024/1024,2) mb from information_schema.tables  where table_schema not in ('sys','mysql','information_schema','performance_schema') group by table_schema,table_name order by mb desc limit 0,20"

var db mysql.Conn

func NewMysqlLoader(name string, sec *ini.Section) *inst.Loader {
	mysqlloader := &inst.Loader{}
	ms := instance(sec)
	mysqlloader.Name = name
	mysqlloader.Ms = ms
	return mysqlloader
}
func connection(sec *ini.Section) (mysql.Conn, error) {
	if db != nil {
		return db, nil
	} else {
		h, _ := sec.GetKey(host)
		pt, _ := sec.GetKey(port)
		u, _ := sec.GetKey(user)
		pd, _ := sec.GetKey(password)
		db = mysql.New("tcp", "", h.Value()+":"+pt.Value(), u.Value(), pd.Value(), "information_schema")

		err := db.Connect()
		if err != nil {
			return nil, err
		}
	}
	return db, nil
}

func databasesize(con mysql.Conn) (string, error) {
	var val string
	var file *os.File
	var err error
	if _, err := os.Stat(dbsize_file); os.IsNotExist(err) {
		file, err = os.Create(dbsize_file)
		if err != nil {
			return val, err
		}
	}
	file, err = os.OpenFile(dbsize_file, os.O_APPEND|os.O_WRONLY, 0600)
	if err == nil {
		defer file.Close()
		rows, _, err := con.Query(database_gb)
		if err == nil {
			for _, row := range rows {
				k := strings.ToLower(row.Str(0))
				v := strings.ToLower(row.Str(1))
				val = v + "GB (update time:" + k + ")\n"
				if _, err = file.WriteString(val); err != nil {
					return val, err
				}
			}
		}
	}
	return val, nil
}
func instance(sec *ini.Section) []*inst.Metric {
	ms := make([]*inst.Metric, 0)

	con, err := connection(sec)

	if err != nil {
		ms = append(ms, inst.NewMetric("err", err))
		return ms
	}
	defer con.Close()

	var rollback int64
	var commit int64
	var uptime int64
	var question int64
	var vselect int64
	var vdelete int64
	var vupdate int64
	var vinsert int64

	//bp performance
	var read_page_from_bp int64
	var read_page_from_disk int64
	//slow log
	var slow_queries int64

	var page_total float64
	var page_free float64
	var page_size float64

	//Threads_cached
	var threads_create int64
	var threads_cache int64
	var threads_running int64
	var threads_connected int64
	var connection_errors_internal int64
	var connection_errors_max_connections int64
	//binlog and tmp table
	var binlog_cache_disk_use int64
	var create_tmp_disk_tables int64

	var aborted_connects int64
	var innodb_row_lock_current_waits int64
	var innodb_row_lock_time int64
	var innodb_row_lock_time_avg int64
	var innodb_row_lock_time_max int64
	var innodb_row_lock_waits int64

	var read_times int64
	var write_times int64
	var stmt1 string
	var stmt string
	rows, _, err := con.Query("select version()")
	if err == nil {
		for _, row := range rows {
			if strings.Contains(strings.ToLower(row.Str(0)), "5.7") {
				stmt1 = strings.Replace(sql1, "information_schema", "performance_schema", -1)
				stmt = strings.Replace(sql0, "information_schema", "performance_schema", -1)
			} else {
				stmt1 = sql1
				stmt = sql0
			}
		}
	} else {

		ms = append(ms, inst.NewMetric("err", err))
		return ms
	}
	//variables
	stmt1 = strings.Replace(stmt1, "?", condition1, -1)
	rows, _, err = con.Query(stmt1)

	if err == nil {
		for _, row := range rows {
			key := strings.ToLower(row.Str(0))
			val := strings.ToLower(row.Str(1))
			switch {
			case strings.Compare(key, "version") == 0:
				ms = append(ms, inst.NewMetric("version", val))
				break
			case strings.Compare(key, "innodb_buffer_pool_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("innodb_buffer_pool_size", strconv.FormatFloat(v/1024/1024/1024, 'f', 2, 64)+" GB"))
				break
			case strings.Compare(key, "innodb_buffer_pool_instances") == 0:
				ms = append(ms, inst.NewMetric("innodb_buffer_pool_instances", val))
				break
			case strings.Compare(key, "innodb_additional_mem_pool_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("innodb_additional_mem_pool_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "key_buffer_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("key_buffer_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "join_buffer_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("join_buffer_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "read_buffer_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("read_buffer_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "read_rnd_buffer_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("read_rnd_buffer_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "sort_buffer_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("sort_buffer_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break

			case strings.Compare(key, "max_heap_table_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("max_heap_table_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "thread_stack") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("thread_stack", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "binlog_cache_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("binlog_cache_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "innodb_io_capacity") == 0:
				ms = append(ms, inst.NewMetric("innodb_io_capacity", val))
				break
			case strings.Compare(key, "max_connections") == 0:
				ms = append(ms, inst.NewMetric("max_connections", val))
				break
			case strings.Compare(key, "innodb_read_io_threads") == 0:
				ms = append(ms, inst.NewMetric("innodb_read_io_threads", val))
				break
			case strings.Compare(key, "innodb_write_io_threads") == 0:
				ms = append(ms, inst.NewMetric("innodb_write_io_threads", val))
				break
			case strings.Compare(key, "innodb_flush_log_at_trx_commit") == 0:
				ms = append(ms, inst.NewMetric("innodb_flush_log_at_trx_commit", val))
				break
			case strings.Compare(key, "innodb_file_per_table") == 0:
				ms = append(ms, inst.NewMetric("innodb_file_per_table", val))
				break
			case strings.Compare(key, "innodb_log_file_size") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("innodb_log_file_size", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			case strings.Compare(key, "innodb_log_files_in_group") == 0:
				ms = append(ms, inst.NewMetric("innodb_log_files_in_group", val))
				break
			case strings.Compare(key, "max_allowed_packet") == 0:
				v, _ := strconv.ParseFloat(val, 64)
				ms = append(ms, inst.NewMetric("max_allowed_packet", strconv.FormatFloat(v/1024/1024, 'f', 2, 64)+" MB"))
				break
			}
		}
		if len(ms) > 0 {
			ms = append(ms, inst.NewMetric("end", "end"))
		}
	}

	stmt = strings.Replace(stmt, "?", condition, -1)
	rows, _, err = con.Query(stmt)
	if err == nil {
		for _, row := range rows {
			key := strings.ToLower(row.Str(0))
			val, err := strconv.ParseInt(row.Str(1), 10, 64)
			if err != nil {
				fmt.Println(err)
				return nil
			}
			switch {
			case strings.Compare(key, "com_insert_select") == 0 || strings.Compare(key, "com_select") == 0:
				vselect = vselect + val
				break
			case strings.Compare(key, "com_update") == 0 || strings.Compare(key, "com_update_multi") == 0:
				vupdate = vupdate + val
				break
			case strings.Compare(key, "com_delete") == 0 || strings.Compare(key, "com_delete_multi") == 0:
				vdelete = vdelete + val
				break
			case strings.Compare(key, "com_insert") == 0 || strings.Compare(key, "com_insert_select") == 0:
				vinsert = vinsert + val
				break
			case strings.Compare(key, "uptime") == 0:
				uptime = val
				break
			case strings.Compare(key, "slow_queries") == 0:
				slow_queries = val
				break
			case strings.Compare(key, "threads_cached") == 0:
				threads_cache = val
				break
			case strings.Compare(key, "threads_connected") == 0:
				threads_connected = val
				break
			case strings.Compare(key, "threads_running") == 0:
				threads_running = val
				break
			case strings.Compare(key, "threads_created") == 0:
				threads_create = val
				break
			case strings.Compare(key, "binlog_cache_disk_use") == 0:
				binlog_cache_disk_use = val
				break
			case strings.Compare(key, "created_tmp_disk_tables") == 0:
				create_tmp_disk_tables = val
				break
			case strings.Compare(key, "innodb_page_size") == 0:
				page_size = float64(val)
				break
			case strings.Compare(key, "innodb_buffer_pool_read_requests") == 0:
				read_page_from_bp = val
				break
			case strings.Compare(key, "innodb_buffer_pool_reads") == 0:
				read_page_from_disk = val
				break
			case strings.Compare(key, "innodb_buffer_pool_pages_total") == 0:
				page_total = float64(val)
				break
			case strings.Compare(key, "innodb_buffer_pool_pages_free") == 0:
				page_free = float64(val)
				break
			case strings.Compare(key, "com_commit") == 0:
				commit = val
				break
			case strings.Compare(key, "com_rollback") == 0:
				rollback = val
				break
			case strings.Compare(key, "questions") == 0:
				question = val
				break
			case strings.Compare(key, "connection_errors_max_connections") == 0:
				connection_errors_max_connections = val
				break
			case strings.Compare(key, "connection_errors_internal") == 0:
				connection_errors_internal = val
				break
			case strings.Compare(key, "aborted_connects") == 0:
				aborted_connects = val
				break
			case strings.Compare(key, "innodb_row_lock_current_waits") == 0:
				innodb_row_lock_current_waits = val
				break
			case strings.Compare(key, "innodb_row_lock_time") == 0:
				innodb_row_lock_time = val
				break
			case strings.Compare(key, "innodb_row_lock_time_avg") == 0:
				innodb_row_lock_time_avg = val
				break
			case strings.Compare(key, "innodb_row_lock_time_max") == 0:
				innodb_row_lock_time_max = val
				break
			case strings.Compare(key, "innodb_row_lock_waits") == 0:
				innodb_row_lock_waits = val
				break
			default:
				break
			}
		}

		//innodbStatus(con)
		read_times = vselect
		write_times = vupdate + vdelete + vinsert
		ms = append(ms, inst.NewMetric("qps", question/uptime))
		ms = append(ms, inst.NewMetric("tps", (rollback+commit)/uptime))
		ms = append(ms, inst.NewMetric("select", vselect))
		ms = append(ms, inst.NewMetric("update", vupdate))
		ms = append(ms, inst.NewMetric("delete", vdelete))
		ms = append(ms, inst.NewMetric("insert", vinsert))
		ms = append(ms, inst.NewMetric("read_times", read_times))
		ms = append(ms, inst.NewMetric("write_times", write_times))
		ms = append(ms, inst.NewMetric("read_page_from_bp", strconv.FormatInt(read_page_from_bp, 10)+" request"))
		ms = append(ms, inst.NewMetric("read_page_from_disk", strconv.FormatInt(read_page_from_disk, 10)+" request"))
		ms = append(ms, inst.NewMetric("buffer_pool_size", strconv.FormatFloat((page_total*page_size)/1024/1024/1024, 'f', 2, 64)+" GB"))
		ms = append(ms, inst.NewMetric("buffer_pool_usage", strconv.FormatFloat(((page_total-page_free)/page_total)*100, 'f', 2, 64)+" %"))
		ms = append(ms, inst.NewMetric("threads_create", threads_create))
		ms = append(ms, inst.NewMetric("threads_connected", threads_connected))
		ms = append(ms, inst.NewMetric("threads_running", threads_running))
		ms = append(ms, inst.NewMetric("threads_cache", threads_cache))
		ms = append(ms, inst.NewMetric("connection_errors_max_connections", connection_errors_max_connections))
		ms = append(ms, inst.NewMetric("connection_errors_internal", connection_errors_internal))
		ms = append(ms, inst.NewMetric("binlog_cache_disk_use", binlog_cache_disk_use))
		ms = append(ms, inst.NewMetric("create_tmp_disk_tables", create_tmp_disk_tables))
		ms = append(ms, inst.NewMetric("slow_queries", slow_queries))
		ms = append(ms, inst.NewMetric("aborted_connects", aborted_connects))
		ms = append(ms, inst.NewMetric("innodb_row_lock_current_waits", innodb_row_lock_current_waits))
		ms = append(ms, inst.NewMetric("innodb_row_lock_time", innodb_row_lock_time))

		ms = append(ms, inst.NewMetric("innodb_row_lock_time_avg", innodb_row_lock_time_avg))
		ms = append(ms, inst.NewMetric("innodb_row_lock_time_max", innodb_row_lock_time_max))
		ms = append(ms, inst.NewMetric("innodb_row_lock_waits", innodb_row_lock_waits))
		ms = append(ms, inst.NewMetric("uptime", strconv.FormatInt(uptime/60/60, 10)+"h"))

		ms = append(ms, inst.NewMetric("end", "end"))
		val, err := databasesize(con)
		if err == nil {
			ms = append(ms, inst.NewMetric("database_size_GB", val))
		}

		ms = append(ms, inst.NewMetric("end", "end"))

		rows, _, err = con.Query(table_top_10)
		if err == nil {
			for _, row := range rows {
				k := strings.ToLower(row.Str(0)) + "." + strings.ToLower(row.Str(1))
				v := strings.ToLower(row.Str(2)) + " MB"
				ms = append(ms, inst.NewMetric(k, v))

			}
		}
	}
	return ms
}
