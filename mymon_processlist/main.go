package main

import (
	"flag"
	"fmt"
	"processlist"
	"slowlog"
	"strings"
)

func usage() {
	fmt.Println("usage: ./mmymon_processlist show|kill|elog|slog ")
	fmt.Println("          show  --show current processlist during running ")
	fmt.Println("          kill  --kill all connection that is for select ")
	fmt.Println("          elog  --show current error/warnning log for mysql ")
	fmt.Println("          slog  --show current slog log and get explain of it ")
	fmt.Println("[usage description: current program must load mymon.ini file] ")
	fmt.Println("*********perrynzhou@gmail.com********* ")
}
func main() {
	flag.Parse()

	if flag.NArg() == 1 {
		args := flag.Args()
		if strings.Compare(args[0], "show") == 0 {
			processlist.Op_rocesslist_for_mysql("show")
		} else if strings.Compare(args[0], "kill") == 0 {
			processlist.Op_rocesslist_for_mysql("kill")
		} else if strings.Compare(args[0], "elog") == 0 {
			processlist.Check_error_for_mysql()
		} else if strings.Compare(args[0], "slog") == 0 {
			es, err := slowlog.GetSqlExplain()
			fmt.Println(es)
			if err != nil || len(es) == 0 {
				return
			}
			for _, v := range es {
				fmt.Println(v.Time)
				fmt.Println(v.UserHost)
				fmt.Println(v.QueryInfo)
				fmt.Println(v.SqlInfo)
			}
			v0 := es[0]
			v1 := es[len(es)-1]
			fmt.Println("*****************", v0.Time[8:], "~", v1.Time[8:], ",slowlog items:", len(es), "********************")
		} else {
			usage()
		}
	} else {
		usage()
	}
}
