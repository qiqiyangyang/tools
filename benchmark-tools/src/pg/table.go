package pg

import (
	"bytes"
	"common"
	"conf"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type TypeIndex int

const (
	IntegerTypeIndex TypeIndex = iota
	CharacterVaryingTypeIndex
	CharacterTypeIndex
	TextTypeIndex
	DateTypeIndex
	BigIntTypeIndex
)
const (
	SerialKeyWord = "nextval"
)
const (
	QueryTableStmtFmt        = "select %s from %s  ORDER BY random() limit %d"
	PreDropTableStmtFmt      = "drop table if exists  ?"
	CreateTableStmtFmt       = "create table %s(?)"
	DeleteTableStmtFmt       = "delete * from %s where %s = ?"
	UpdateTableStmtFmt       = "update %s set %s=? where %s = ?"
	SelectTableStmtFmt       = "select ? from %s where %s = ?"
	InsertTableStmtFmt       = "insert into %s(?)values"
	SelectMetaPrepareStmtFmt = "select a.table_name, a.column_name,a.data_type,a.character_maximum_length,COALESCE(a.column_default,'nil') from information_schema.columns a WHERE a.table_schema = 'public' and a.table_name = '?'"
)

var (
	PgDataTypes = []string{
		"integer",
		"charactervarying",
		"character",
		"text",
		"date",
		"bigint",
	}
)

type Column struct {
	Name     string
	Type     string
	Len      uint32
	IsSerial bool
}

type Table struct {
	columnInfo   []*Column
	conn         common.Connection
	pgConfig     *conf.PgConfig
	mtx          sync.Mutex
	Qps          *uint64
	SerialColNum int
	wg           *sync.WaitGroup
	stop         chan struct{}
}

func NewTable(config *conf.PgConfig) (*Table, error) {

	conn, err := NewPgConnection(config.ServerConfig)
	if err != nil {
		return nil, err
	}

	dropTableStmt := PreDropTableStmtFmt
	if _, err := conn.Exec(strings.Replace(dropTableStmt, "?", config.TargetTable, -1)); err != nil {
		conn.Close()
		return nil, err
	}
	createSqlStmt := fmt.Sprintf(CreateTableStmtFmt, config.TargetTable)
	originFieldList := strings.SplitN(config.TargetTableFiledList, ",", -1)
	fieldList := make([]string, len(originFieldList))
	for index, field := range originFieldList {
		fieldList[index] = field
	}
	createSqlStmt = strings.Replace(createSqlStmt, "?", strings.Join(fieldList, ","), -1)
	_, err = conn.Exec(createSqlStmt)
	if err != nil {
		return nil, err
	}
	metaStmt := strings.Replace(SelectMetaPrepareStmtFmt, "?", config.TargetTable, -1)
	rows, err := conn.Query(metaStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	table := &Table{
		columnInfo: make([]*Column, 0),
		conn:       conn,
		pgConfig:   config,
	}
	var tableName string
	var buf bytes.Buffer
	for rows.Next() {
		var name string
		var ctype string
		var clen sql.NullInt64
		var isserial string
		err = rows.Scan(&tableName, &name, &ctype, &clen, &isserial)
		if err != nil {
			return nil, err
		}
		types := strings.SplitN(ctype, " ", -1)
		for _, v := range types {
			buf.WriteString(strings.ToLower(v))
		}
		col := &Column{
			Name: strings.ToLower(name),
			Type: buf.String(),
			Len:  uint32(clen.Int64),
		}
		buf.Reset()
		if strings.Contains(strings.ToLower(isserial), SerialKeyWord) {
			col.IsSerial = true
			table.SerialColNum = table.SerialColNum + 1
		} else {
			col.IsSerial = false
		}
		table.columnInfo = append(table.columnInfo, col)
	}
	table.wg = &sync.WaitGroup{}
	return table, nil
}
func (table *Table) Run(stop chan struct{}) {
	createStmt := table.CreatePrepareStateForInsert()
	table.wg.Add(common.OperationCount)
	table.stop = make(chan struct{})
	defer table.conn.Close()

	go table.Insert(createStmt)
	go table.Update()
	//go table.Delete()
	//go table.Select()
	for {
		select {
		case <-stop:
			for i := 0; i < common.OperationCount; i++ {
				table.stop <- struct{}{}
			}
			log.Println("...stop current thread")
			return
		}
	}

}

func (table *Table) CreatePrepareStateForInsert() string {
	sqlStmt := fmt.Sprintf(InsertTableStmtFmt, table.pgConfig.TargetTable)
	tcolumns := make([]string, len(table.columnInfo)-table.SerialColNum)
	var index int
	for i := 0; i < len(table.columnInfo); i++ {
		if !table.columnInfo[i].IsSerial {
			tcolumns[index] = table.columnInfo[i].Name
			index = index + 1
		}
	}
	return strings.Replace(sqlStmt, "?", strings.Join(tcolumns, ","), -1)
}
func (table *Table) Insert(prepareSqlStmt string) {

	var sbuf bytes.Buffer
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			sbuf.WriteString(prepareSqlStmt)
			for i := 0; i < table.pgConfig.MaxBatchSize; i++ {
				if i == 0 {
					sbuf.WriteString("(")
				} else {
					sbuf.WriteString(",(")
				}
				for index, v := range table.columnInfo {
					var value string
					if !v.IsSerial {
						if v.Type == PgDataTypes[IntegerTypeIndex] {
							if index != len(table.columnInfo)-1 {
								value = fmt.Sprintf("%d,", common.GenInt())
							} else {
								value = fmt.Sprintf("%d", common.GenInt())
							}
						} else if v.Type == PgDataTypes[CharacterVaryingTypeIndex] || v.Type == PgDataTypes[CharacterTypeIndex] || v.Type == PgDataTypes[TextTypeIndex] {
							if index != len(table.columnInfo)-1 {
								value = fmt.Sprintf("'%s',", common.GenVarch(uint32(v.Len)))

							} else {
								value = fmt.Sprintf("'%s'", common.GenVarch(uint32(v.Len)))
							}
						} else if v.Type == PgDataTypes[DateTypeIndex] {
						}
						if len(value) > 0 {
							sbuf.WriteString(value)
						}
					}
				}
				sbuf.WriteString(")")
			}

			res, err := table.conn.Exec(sbuf.String())
			sbuf.Reset()
			if err != nil {
				log.Fatal(err)
				return
			} else {
				n, err := res.RowsAffected()
				if err == nil {
					atomic.AddUint64(table.Qps, uint64(n))
				}
			}
		}
	}
}

func (table *Table) Update() {
	table.wg.Done()
	ticker := time.NewTicker(time.Millisecond * table.pgConfig.TimeIntervalMilliSecond)
	defer ticker.Stop()
	preSqlStmt := QueryTableStmtFmt
	tmpSqlStmt := make(map[int]string)
	for index, col := range table.columnInfo {
		tmpSqlStmt[index] = fmt.Sprintf(preSqlStmt, col.Name, table.pgConfig.TargetTable, table.pgConfig.MaxBatchSize)
	}
	updateSqlStmt := make([][]string, len(table.columnInfo))
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:

			for index, usql := range tmpSqlStmt {
				rows, err := table.conn.Query(usql)
				if err != nil {
					panic(err)
					return
				}
				defer rows.Close()
				v := table.columnInfo[index]
				if updateSqlStmt[index] == nil {
					updateSqlStmt[index] = make([]string, 0)
				}
				atomic.AddUint64(table.Qps, 1)
				for rows.Next() {
					updateSqlFmt := fmt.Sprintf(UpdateTableStmtFmt, table.pgConfig.TargetTable, v.Name, v.Name)
					var val interface{}
					var rval string
					var cond string
					err = rows.Scan(&val)
					if err != nil {
						panic(err)
						return
					}
					if v.Type == PgDataTypes[IntegerTypeIndex] {
						cond = fmt.Sprintf("%d", val.(int64))
						rval = fmt.Sprintf("%d", common.GenInt())
					} else if v.Type == PgDataTypes[CharacterVaryingTypeIndex] || v.Type == PgDataTypes[CharacterTypeIndex] || v.Type == PgDataTypes[TextTypeIndex] {
						cond = fmt.Sprintf("'%s'", val.(string))
						rval = fmt.Sprintf("'%s'", common.GenVarch(uint32(v.Len)))
					}
					updateSqlFmt = strings.Replace(updateSqlFmt, "?", rval, 1)
					updateSqlStmt[index] = append(updateSqlStmt[index], strings.Replace(updateSqlFmt, "?", cond, 1))
				}
				for _, vs := range updateSqlStmt {
					for _, v := range vs {
						_, err := table.conn.Exec(v)
						if err == nil {
							atomic.AddUint64(table.Qps, 1)
						} else {
							panic(err)
						}
					}
				}
			}
		}
	}
	return
}
