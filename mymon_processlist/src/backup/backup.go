package processlist

import (
	"bufio"
	"common"
	"fmt"
	"os"
	"strconv"
	"strings"
)
type struct OpLog {
	CurSql string
	Backup string
}
const (
	select_key = "select"
	delete_key = "delete"
	update_key = "update"
	where_key = "where"
	from_key = "from"
	set_key = "set"
)

func selectconvert(s string) *OpLog {
	low_str = strings.ToLower(s)
	if len(s) ==0 || strings.Contains(low_str,select_key) {
		return nil
	}
	op = &OpLog{}
	op.CurSql = low_str
	var bakupSql  string

	switch {
	case strings.Contains(low_str,update_key):
		vs :=strings.Split(low_str,set_key)
		backupSql = strings.Replace(vs[0],update_key,select_key,1)
		break
	case strings.Contains(low_str,delete_key):
		vs := strings.Split(low_str,delete_key)
		backupSql = strings.Replace(vs[0],delete_key,select_key,1)
		break
	default:
		break
	}

	ws := strings.Split(low_str,where_key)
	backupSql = backupSql + " where "+ ws[1]
	op.BackupSql = backupSql
	return op

}
func init_oplog(file string) []*OpLog {
	file,err := os.Open(path)
	ops := make([]*OpLog,0)
	if  err == nil {
		defer file.Close()
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			cur := strings.Replace(scanner.Text(),";","",-1)
			op := SelectConvert(cur)
			if op != nil {
				ops = append(ops,cur)
			}

		}
		if err := scanner.Err();err != nil {
			return nil
		}
	}
	return ops
}
func BackupData(path string)  {
	db, err := common.Init_database()
	if err != nil {
		fmt.Println(err)
		return
	}
	defer db.Close()
	var path string
	ops := init_oplog(path)
	for _,v:= range ops {
		rows, _, err := db.Query(slow_log_sql)
		if err == nil {
			for _, row := range rows {
				path = row.Str(0)
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
