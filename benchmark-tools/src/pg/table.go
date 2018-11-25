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

const (
	IntegerTypeIndex = iota
	CharacterVaryingTypeIndex
	CharacterTypeIndex
	TextTypeIndex
	DateTypeIndex
)
const (
	SerialKeyWord = "nextval"
)
const (
	CreateTableStmtFmt       = "create table %s(?)"
	DeleteTableStmtFmt       = "delete * from %s where %s = ?"
	UpdateTableStmtFmt       = "update %s set %s=? where %s = ?"
	SelectTableStmtFmt       = "select ? from %s where %s = ?"
	InsertTableStmtFmt       = "insert into %s(?)values?"
	SelectMetaPrepareStmtFmt = "select a.table_name, a.column_name,a.data_type,a.character_maximum_length,a.column_default from information_schema.columns a WHERE a.table_schema = 'public' and a.table_name = '?'"
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

type Column struct {
	Name     string
	Type     string
	Len      int64
	IsSerial bool
}
type Condition struct {
	Cond []interface{}
	Size int
}
type Table struct {
	columnInfo  []*Column
	conn        common.Connection
	pgConfig    *conf.PgConfig
	condition   map[string]*Condition
	mtx         sync.Mutex
	insertCount uint64
	updateCount uint64
	deleteCount uint64
	selectCount uint64
	done        chan struct{}
	wg          *sync.WaitGroup
}

func NewTable(config *conf.PgConfig) (*Table, error) {

	conn, err := NewPgConnection(config.ServerConfig)
	if err != nil {
		return nil, err
	}
	createSqlStmt := fmt.Sprintf(CreateTableStmtFmt, config.TargetTable)
	originFieldList := strings.SplitN(config.TargetTableFiledList, ",", -1)
	fieldList := make([]string, len(originFieldList)+1)
	index := 0
	fieldList[index] = fmt.Sprintf("%s_id bigserial", config.TargetTable)
	for _, field := range originFieldList {
		index = index + 1
		fieldList[index] = field
	}
	createSqlStmt = strings.Replace(createSqlStmt, "?", strings.Join(fieldList, ","), -1)
	_, err = conn.Exec(createSqlStmt)
	if err != nil {
		return nil, err
	}
	log.Println(createSqlStmt)
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
		condition:  make(map[string]*Condition),
	}
	var tableName string
	for rows.Next() {
		var name string
		var ctype string
		var clen sql.NullInt64
		var isserial string
		err = rows.Scan(&tableName, &name, &ctype, &clen, &isserial)
		if err != nil {
			return nil, err
		}
		c := &Column{
			Name:     strings.ToLower(name),
			Type:     strings.TrimSpace(ctype),
			Len:      clen.Int64,
			IsSerial: strings.Contains(strings.ToLower(isserial), SerialKeyWord),
		}
		cond := &Condition{
			Cond: nil,
			Size: table.pgConfig.MaxBatchSize,
		}
		table.condition[name] = cond

		table.columnInfo = append(table.columnInfo, c)
	}
	table.wg = &sync.WaitGroup{}
	table.wg.Add(common.OperationCount)
	table.done = make(chan struct{}, common.OperationCount)
	return table, nil
}
func (table *Table) Run(stop chan struct{}) {
	createStmt := table.CreatePrepareStateForInsert()
	defer table.conn.Close()
	defer table.wg.Wait()
	go table.Insert(createStmt)
	go table.Update()
	go table.Delete()
	go table.Select()
	for {
		select {
		case <-stop:
			for i := 0; i < common.OperationCount; i++ {
				table.done <- struct{}{}
			}
			log.Println("...stop current thread")
			return
		}
	}

}

func (table *Table) addCondition(key string, val interface{}) {
	table.mtx.Lock()
	defer table.mtx.Unlock()

	if table.condition[key].Cond == nil {
		table.condition[key].Cond = make([]interface{}, 0)
	}
	if len(table.condition[key].Cond) < table.condition[key].Size {
		table.condition[key].Cond = append(table.condition[key].Cond, val)
	}
}
func (table *Table) deleteCondition(key string) {
	table.mtx.Lock()
	if table.condition[key].Cond != nil {
		table.condition[key].Cond = table.condition[key].Cond[1:table.condition[key].Size]
		table.condition[key].Size = table.condition[key].Size - 1

	}
	defer table.mtx.Unlock()
}
func (table *Table) clearCondition(key string) {
	table.mtx.Lock()
	if table.condition[key] != nil {
		table.condition[key] = nil
	}
	defer table.mtx.Unlock()
}
func (table *Table) CreatePrepareStateForInsert() string {
	sqlStmt := fmt.Sprintf(InsertTableStmtFmt, table.pgConfig.TargetTable)
	tcolumns := make([]string, 0)
	prepareValues := make([]string, 0)
	for _, v := range table.columnInfo {
		if !v.IsSerial {
			tcolumns = append(tcolumns, v.Name)
		}
	}
	strings.Join(prepareValues, ",")
	strings.Replace(sqlStmt, "?", strings.Join(tcolumns, ","), 1)
	return sqlStmt
}
func (table *Table) Insert(prepareSqlStmt string) {
	defer table.wg.Done()
	var sbuf bytes.Buffer
	var value string
	ticker := time.NewTicker(time.Microsecond * 500)
	defer ticker.Stop()
	for {
		select {
		case <-table.done:
			return
		case <-ticker.C:
			sbuf.WriteString(strings.Replace(prepareSqlStmt, "?", "", 1))
			for i := 0; i < table.pgConfig.MaxBatchSize; i++ {
				sbuf.WriteString("(")
				for index, v := range table.columnInfo {
					if !v.IsSerial {
						if v.Type == PgDataTypes[IntegerTypeIndex] {
							if index != len(table.columnInfo)-1 {
								value = fmt.Sprintf("%d,", common.GenInt())
							} else {
								value = fmt.Sprintf("%d", common.GenInt())
							}
							table.addCondition(v.Name, value)
							sbuf.WriteString(value)
						} else if v.Type == PgDataTypes[CharacterVaryingTypeIndex] || v.Type == PgDataTypes[CharacterTypeIndex] || v.Type == PgDataTypes[TextTypeIndex] {
							if index != len(table.columnInfo)-1 {
								value = fmt.Sprintf("'%s,'", common.GenVarch(uint32(v.Len)))
							} else {
								value = fmt.Sprintf("'%s'", common.GenVarch(uint32(v.Len)))
							}
							table.addCondition(v.Name, value)
							sbuf.WriteString(value)
						} else if v.Type == PgDataTypes[DateTypeIndex] {
							table.addCondition(v.Name, value)
						}
					}
					if i != table.pgConfig.MaxBatchSize-1 {
						sbuf.WriteString("),")
					} else {
						sbuf.WriteString(")")
					}
				}

			}
			res, err := table.conn.Exec(sbuf.String())
			if err != nil {
				return
			} else {
				n, err := res.RowsAffected()
				if err == nil {
					atomic.AddUint64(&table.insertCount, uint64(n))
				}
			}
			sbuf.Reset()
		default:
		}

	}

}

func (table *Table) Delete() {
	defer table.wg.Done()
	ticker := time.NewTicker(time.Microsecond * table.pgConfig.TimeIntervalMilliSecond)
	defer ticker.Stop()

	deleteStmt := make([]string, len(table.columnInfo))
	for i := 0; i < len(table.columnInfo); i++ {
		deleteStmt[i] = DeleteTableStmtFmt
		deleteStmt[i] = fmt.Sprintf(deleteStmt[i], table.pgConfig.TargetTable, table.columnInfo[i])
	}
	for {
		select {
		case <-ticker.C:
			for index, col := range table.columnInfo {
				if col.IsSerial || col.Type == PgDataTypes[IntegerTypeIndex] {
					vid := table.condition[col.Name].Cond[0].(uint64)
					deleteStmt[index] = strings.Replace(deleteStmt[index], "?", fmt.Sprintf("%d", vid), -1)
				} else if col.Type == PgDataTypes[CharacterVaryingTypeIndex] || col.Type == PgDataTypes[CharacterTypeIndex] || col.Type == PgDataTypes[TextTypeIndex] {
					vvs := table.condition[col.Name].Cond[0].(string)
					deleteStmt[index] = strings.Replace(deleteStmt[index], "?", fmt.Sprintf("'%s'", vvs), -1)

				} else {

				}
				table.deleteCondition(col.Name)
			}
			for _, v := range deleteStmt {
				_, err := table.conn.Exec(v)
				if err == nil {
					atomic.AddUint64(&table.deleteCount, 1)
				}
			}
		default:
		}

	}
}
func (table *Table) Update() {
	defer table.wg.Done()
	ticker := time.NewTicker(time.Microsecond * table.pgConfig.TimeIntervalMilliSecond)
	defer ticker.Stop()
	updateStmt := make([]string, len(table.columnInfo))
	for i := 0; i < len(table.columnInfo); i++ {
		updateStmt[i] = UpdateTableStmtFmt
		updateStmt[i] = fmt.Sprintf(updateStmt[i], table.pgConfig.TargetTable, table.columnInfo[i].Name, table.columnInfo[i].Name)
	}
	for {
		select {
		case <-table.done:
			return
		case <-ticker.C:
			for index, col := range table.columnInfo {
				if col.IsSerial || col.Type == PgDataTypes[IntegerTypeIndex] {
					vid := table.condition[col.Name].Cond[0].(uint64)
					updateStmt[index] = strings.Replace(updateStmt[index], "?", fmt.Sprintf("%d", vid), -1)
				} else if col.Type == PgDataTypes[CharacterVaryingTypeIndex] || col.Type == PgDataTypes[CharacterTypeIndex] || col.Type == PgDataTypes[TextTypeIndex] {
					vvs := table.condition[col.Name].Cond[0].(string)
					updateStmt[index] = strings.Replace(updateStmt[index], "?", fmt.Sprintf("'%s'", vvs), -1)
				} else {

				}
				table.deleteCondition(col.Name)
			}
			for _, v := range updateStmt {
				_, err := table.conn.Exec(v)
				if err == nil {
					atomic.AddUint64(&table.deleteCount, 1)
				}
			}
		default:
		}
	}
}
func (table *Table) Select() {
	defer table.wg.Done()
	ticker := time.NewTicker(time.Microsecond * table.pgConfig.TimeIntervalMilliSecond)
	defer ticker.Stop()
	selectStmt := make([]string, len(table.columnInfo)+1)
	selectColumns := make([][]string, len(table.columnInfo)+1)
	for i := 0; i < len(table.columnInfo); i++ {
		if selectColumns[i] == nil {
			selectColumns[i] = make([]string, i+1)
		}
		selectColumns[i] = append(selectColumns[i], table.columnInfo[i].Name)
		if len(selectColumns[i]) > 1 {
			for _, v := range selectColumns[i] {
				selectColumns[i] = append(selectColumns[i], v)
			}
		}
		selectColumns[i] = append(selectColumns[i], table.columnInfo[i].Name)
	}
	for i := 0; i < len(table.columnInfo); i++ {
		selectStmt[i] = SelectTableStmtFmt
		selectStmt[i] = fmt.Sprintf(selectStmt[i], strings.Join(selectColumns[i], ","), table.pgConfig.TargetTable, table.columnInfo[i].Name)
	}

	for {
		select {
		case <-table.done:
			return
		case <-ticker.C:
			for index, col := range table.columnInfo {
				if col.IsSerial || col.Type == PgDataTypes[IntegerTypeIndex] {
					vid := table.condition[col.Name].Cond[0].(uint64)
					selectStmt[index] = strings.Replace(selectStmt[index], "?", fmt.Sprintf("%d", vid), -1)
				} else if col.Type == PgDataTypes[CharacterVaryingTypeIndex] || col.Type == PgDataTypes[CharacterTypeIndex] || col.Type == PgDataTypes[TextTypeIndex] {
					vvs := table.condition[col.Name].Cond[0].(string)
					selectStmt[index] = strings.Replace(selectStmt[index], "?", fmt.Sprintf("'%s'", vvs), -1)
				} else {

				}
				table.deleteCondition(col.Name)
			}
			for _, v := range selectStmt {
				_, err := table.conn.Exec(v)
				if err == nil {
					atomic.AddUint64(&table.selectCount, 1)
				}
			}
		default:
		}
	}
}
