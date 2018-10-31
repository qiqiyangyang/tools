package processlist

import (
	"bufio"
	"common"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	processlist_sql = "select ID,USER,HOST,COMMAND,TIME,STATE,INFO from information_schema.processlist order by TIME desc "
	slow_log_sql    = "select variable_value from information_schema.global_variables where variable_name = 'slow_query_log_file'"
	self_sql        = "select CONNECTION_ID()"
	show_type       = "show"
	kill_type       = "kill"
)

func Check_error_for_mysql() {
	db, err := common.Init_database()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	var path string
	rows, _, err := db.Query(slow_log_sql)
	if err == nil {
		for _, row := range rows {
			path = row.Str(0)
		}
		if len(path) > 0 {
			file, err := os.Open(path)
			if err != nil {
				fmt.Println(err)
				return
			}
			defer file.Close()
			scanner := bufio.NewScanner(file)
			fmt.Println("********************************mysql unnormal log**********************************************")
			for scanner.Scan() {
				line := strings.ToLower(scanner.Text())
				if strings.Contains(line, strings.ToLower("[ERROR]")) || strings.Contains(line, strings.ToLower("[Warning]")) {
					fmt.Println(line)
				}
			}
			if err := scanner.Err(); err != nil {
				fmt.Println(err)
				return
			}
		}
	}
}
func Op_rocesslist_for_mysql(op_type string) {
	db, err := common.Init_database()
	if err == nil {
		defer db.Close()
		var kill_list []string
		format := "%-12s|%v\n"
		prev := "kill connection "

		rows, _, err := db.Query(self_sql)
		var self int
		if err == nil {
			for _, row := range rows {
				self = row.Int(0)
			}
		}
		//
		rows, _, err = db.Query(processlist_sql)
		if err == nil {
			for _, row := range rows {
				fmt.Printf(format, "id:", row.Int(0))
				fmt.Printf(format, "user:", strings.ToLower(row.Str(1)))
				fmt.Printf(format, "host:", strings.ToLower(row.Str(2)))
				fmt.Printf(format, "command:", strings.ToLower(row.Str(3)))
				fmt.Printf(format, "time:", row.Str(4))
				fmt.Printf(format, "state:", strings.ToLower(row.Str(5)))
				fmt.Printf(format, "sql:", strings.ToLower(row.Str(6)))
				fmt.Println()
				cur_sql := strings.ToLower(row.Str(6))
				if self != row.Int(0) && (!strings.Contains(cur_sql, "delete") || !strings.Contains(cur_sql, "update") || !strings.Contains(cur_sql, "insert") || !strings.Contains(cur_sql, "binlog") || !strings.Contains(cur_sql, "change") || !strings.Contains(cur_sql, "daemon")) {
					thd_sql := prev + strconv.Itoa(row.Int(0))
					kill_list = append(kill_list, thd_sql)
				}
			}
			if strings.Compare(op_type, kill_type) == 0 {
				for _, v := range kill_list {
					_, _, err := db.Query(v)
					if err != nil {
						fmt.Println(err)
						return
					}
				}
				fmt.Printf("::total kill %d connection ok::\n", len(kill_list))
			}

		}
	}
}
