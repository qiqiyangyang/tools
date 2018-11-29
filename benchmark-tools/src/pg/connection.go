package pg

import (
	"conf"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type PgConnection struct {
	conn *sql.DB
}

func NewPgConnection(Config *conf.PgServerConfig) (*PgConnection, error) {
	pgConInfo := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", Config.Host, Config.Port, Config.User, Config.PassWord, Config.DbName)
	pcon, err := sql.Open("postgres", pgConInfo)
	if err != nil {
		return nil, err
	}
	if err = pcon.Ping(); err != nil {
		return nil, err
	}
	pgConnection := &PgConnection{
		conn: pcon,
	}
	return pgConnection, nil
}
func (pgConnection *PgConnection) Exec(s string) (sql.Result, error) {
	rs, err := pgConnection.conn.Exec(s)
	if err != nil {
		return nil, err
	}
	return rs, nil
}
func (pgConnection *PgConnection) Ping() error {
	return pgConnection.conn.Ping()
}

//    stmt, err := db.Prepare("INSERT INTO userinfo(username,departname,created) VALUES($1,$2,$3)")
func (pgPgConnection *PgConnection) ExecPrepareStmt(sqlStmt string, values ...interface{}) (sql.Result, error) {
	stmt, err := pgPgConnection.conn.Prepare(sqlStmt)
	if err != nil {
		return nil, err
	}
	res, err := stmt.Exec(values...)
	return res, err
}

func (pgConnection *PgConnection) Query(sqlStmt string) (*sql.Rows, error) {
	stmt, err := pgConnection.conn.Prepare(sqlStmt)
	if err != nil {
		return nil, err
	}
	return stmt.Query()
}
func (pgConnection *PgConnection) Close() {
	pgConnection.conn.Close()
}
