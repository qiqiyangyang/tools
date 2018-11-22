package common

import "database/sql"

type Column struct {
	Name string
	Type string
	Len  int64
}
type Table struct {
	Name       string
	ColumnInfo map[string]*Column
}

type TableOp interface {
	Insert(sqlStmt string, values ...interface{}) (uint64, error)
	Delete(sqlStmt string, values ...interface{}) (uint64, error)
	Select(sqlStmt string, values ...interface{}) (sql.Result, error)
	Update(sqlStmt string, values ...interface{}) (uint64, error)
}
