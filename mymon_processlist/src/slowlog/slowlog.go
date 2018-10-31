package slowlog

import (
	"bufio"
	"common"
	"fmt"
	"os"
	"strings"
)

const (
	time_word      = "# Time:"
	userhost_word  = "# User@Host:"
	query_word     = "# Query_time:"
	ignore_word0   = "use"
	ignore_word1   = "SET timestamp"
	slow_sql       = "select variable_value from information_schema.global_variables where variable_name='slow_query_log_file'"
	ignore_version = "Version:"
	ignore_tcp     = "Tcp port:"
	ignore_command = "Id Command"
)

type Explain struct {
	Time      string
	UserHost  string
	QueryInfo string
	SqlInfo   string
}

func GetSqlExplain() ([]*Explain, error) {
	db, err := common.Init_database()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	var path string
	defer db.Close()
	rows, _, err := db.Query(slow_sql)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	for _, row := range rows {
		path = strings.ToLower(row.Str(0))
	}
	es := make([]*Explain, 0)
	if len(path) > 0 {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			fmt.Println(err)
			return nil, err
		}
		file, err := os.Open(path)
		if err != nil {
			fmt.Println(err)
			return nil, err
		}
		scanner := bufio.NewScanner(file)
		var e *Explain
		for scanner.Scan() {
			line := scanner.Text()
			if strings.Contains(line, ignore_version) || strings.Contains(line, ignore_tcp) || strings.Contains(line, ignore_command) {
				continue
			}
			if strings.Contains(line, time_word) {
				if e != nil {
					e.SqlInfo = e.SqlInfo[0 : len(e.SqlInfo)-1]
				}
				e = &Explain{}
				e.Time = line
				es = append(es, e)
			} else if strings.Contains(line, userhost_word) {
				e.UserHost = line
			} else if strings.Contains(line, query_word) {
				e.QueryInfo = line
			} else {
				_line := strings.TrimSpace(line)
				if len(_line) > 0 && !strings.Contains(line, ignore_word1) {
					e.SqlInfo += line
				}

			}
		}
		if e != nil && len(e.SqlInfo) > 0 {
			e.SqlInfo = e.SqlInfo[0 : len(e.SqlInfo)-1]

		}
	}
	return es, nil
}
