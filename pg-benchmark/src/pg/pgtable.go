package pg

import (
	"common"
	"database/sql"
	"fmt"
	"strings"
)

var (
	SelectMetaPrepareStmt = "select a.column_name,a.data_type,a.character_maximum_length from information_schema.columns a WHERE a.table_schema = 'public' and a.table_name = '$'"
)

func NewTable(name string, conn common.Connection) (*common.Table, error) {
	defer conn.Close()
	metaStmt := strings.Replace(SelectMetaPrepareStmt, "$", name, -1)
	rows, err := conn.Query(metaStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	table := &common.Table{
		Name:       name,
		ColumnInfo: make(map[string]*common.Column, 0),
	}

	for rows.Next() {
		var name string
		var ctype string
		var clen sql.NullInt64

		err = rows.Scan(&name, &ctype, &clen)
		if err != nil {
			return nil, err
		}
		table.ColumnInfo[name] = &common.Column{
			Name: name,
			Type: ctype,
			Len:  clen.Int64,
		}
		fmt.Printf("%v", table.ColumnInfo[name])
	}
	return table, nil
}
