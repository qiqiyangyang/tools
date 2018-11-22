package common

import "database/sql"

type TableOp interface {
	Insert() (uint64, error)
	Delete(sqlStmt string, values ...interface{}) (uint64, error)
	Select(sqlStmt string, values ...interface{}) (sql.Result, error)
	Update(sqlStmt string, values ...interface{}) (uint64, error)
}
