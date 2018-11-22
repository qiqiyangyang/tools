package pg

import (
	"common"
	"database/sql"
	"fmt"
	"strings"
)

var (
	SelectMetaPrepareStmt = "select a.column_name,lower(a.data_type),a.character_maximum_length from information_schema.columns a WHERE a.table_schema = 'public' and a.table_name = '$'"
)
var (
	PgDataTypes = []string{
		"integer",
		"charactervarying",
		"character",
		"text",
		"date",
	}
)

const (
	Integer = iota
	CharacterVarying
	Character
	TEXT
	DATE
)

type Column struct {
	Name string
	Type string
	Len  int64
}
type Table struct {
	Name       string
	ColumnInfo map[string]*Column
}

func NewTable(name string, conn common.Connection) (*Table, error) {
	defer conn.Close()
	metaStmt := strings.Replace(SelectMetaPrepareStmt, "$", name, -1)
	rows, err := conn.Query(metaStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	table := &Table{
		Name:       name,
		ColumnInfo: make(map[string]*Column, 0),
	}

	for rows.Next() {
		var name string
		var ctype string
		var clen sql.NullInt64

		err = rows.Scan(&name, &ctype, &clen)
		if err != nil {
			return nil, err
		}

		table.ColumnInfo[name] = &Column{
			Name: name,
			Type: strings.TrimSpace(ctype),
			Len:  clen.Int64,
		}
		fmt.Printf("%v", table.ColumnInfo[name])
	}
	return table, nil
}
func (table *Table) CreatePrepareStateForInsert() string {
	sqlStmt := fmt.Sprintf("insert into %s(?)values(#)", table.Name)
	tcolumns := make([]string, 0)
	prepareValues := make([]string, len(table.ColumnInfo))
	i := 1
	for k, _ := range table.ColumnInfo {
		tcolumns = append(tcolumns, k)
		prepareValues = append(prepareValues, fmt.Sprintf("$%d", i))
		i = i + 1
	}
	strings.Join(prepareValues, ",")
	strings.Replace(sqlStmt, "?", strings.Join(tcolumns, ","), -1)
	strings.Replace(sqlStmt, "#", strings.Join(prepareValues, ","), -1)
	return sqlStmt
}
func (table *Table) Insert(prepareSqlStmt string) (uint64, error) {
	return 0, nil

}
