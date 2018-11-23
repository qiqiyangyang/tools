package mysql

import (
	"bytes"
	"common"
	"database/sql"
	"fmt"
	"strings"
	"sync/atomic"
)

const (
	IntegerTypeIndex = iota
	CharacterVaryingTypeIndex
	CharacterTypeIndex
	TextTypeIndex
	DateTypeIndex
)

var (
	SelectMetaPrepareStmt = "select a.column_name,lower(a.data_type),a.character_maximum_length,lower(a.column_default) from information_schema.columns a WHERE a.table_schema = 'public' and a.table_name = '$'"
)
var (
	DataTypes = []string{
		"integer",
		"charactervarying",
		"character",
		"text",
		"date",
	}
)

type Column struct {
	Name   string
	Type   string
	Len    int64
	IsAuto bool
}
type Table struct {
	Name       string
	ColumnInfo map[string]*Column
	Conn       common.Connection
}

func NewTable(name string, conn common.Connection) (*Table, error) {
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
		var isauto string
		err = rows.Scan(&name, &ctype, &clen, &isauto)
		if err != nil {
			return nil, err
		}
		table.ColumnInfo[name] = &Column{
			Name:   name,
			Type:   strings.TrimSpace(ctype),
			Len:    clen.Int64,
			IsAuto: strings.Contains(isauto, "nextval"),
		}
		fmt.Printf("%v", table.ColumnInfo[name])
	}
	return table, nil
}
func (table *Table) CreatePrepareStateForInsert() string {
	sqlStmt := fmt.Sprintf("insert into %s(?)values(#)", table.Name)
	tcolumns := make([]string, 0)
	prepareValues := make([]string, 0)
	i := int32(1)
	for k, v := range table.ColumnInfo {
		if !v.IsAuto {
			tcolumns = append(tcolumns, k)
			prepareValues = append(prepareValues, fmt.Sprintf("$%d", i))
			atomic.AddInt32(&i, 1)
		}
	}
	strings.Join(prepareValues, ",")
	strings.Replace(sqlStmt, "?", strings.Join(tcolumns, ","), -1)
	strings.Replace(sqlStmt, "#", strings.Join(prepareValues, ","), -1)
	return sqlStmt
}
func (table *Table) Insert(prepareSqlStmt string, conn common.Connection) (uint64, error) {

	textLen := uint32(1024 * 1024 * 10)
	var sbuf bytes.Buffer
	for _, v := range table.ColumnInfo {
		if !v.IsAuto {
			switch v.Type {
			case PgDataTypes[IntegerTypeIndex]:
				sbuf.WriteString(fmt.Sprintfcommon.GenInt())
				break
			case PgDataTypes[CharacterVaryingTypeIndex]:
				values = append(values, common.GenVarch(uint32(v.Len)))
				break
			case PgDataTypes[CharacterTypeIndex]:
				values = append(values, common.GenVarch(uint32(v.Len)))
				break
			case PgDataTypes[TextTypeIndex]:
				values = append(values, common.GenVarch(textLen))
				break
			case PgDataTypes[DateTypeIndex]:

			}
		}
		stmt, err := conn.ExecPrepareStmt(prepareSqlStmt)
		if err != nil {
			return 0, err
		}
	}
	return 0, nil

}
