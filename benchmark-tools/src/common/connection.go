package common

import "database/sql"

type Connection interface {
	Exec(s string) (sql.Result, error)
	Query(sqlStmt string) (*sql.Rows, error)
	Close()
}
