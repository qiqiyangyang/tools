package pg

import (
	"bytes"
	"common"
	"conf"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type TypeIndex int

const (
	IntegerTypeIndex TypeIndex = iota
	CharacterTypeIndex
	TextTypeIndex
	DateTypeIndex
	BigIntTypeIndex
	SmallIntTypeIndex
	TimeStampTypeIndex
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
const (
	internalIdFmt = "%s_seq_id bigserial not null primary key"
)

var (
	PgDataTypes = []string{
		"integer",
		"character",
		"text",
		"date",
		"bigint",
		"smallint",
		"timestamp",
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
	SerialColNum int
	wg           *sync.WaitGroup
	stop         chan struct{}
}

func NewTable(config *conf.PgConfig) (*Table, error) {

	conn, err := NewPgConnection(config.ServerConfig)
	if err != nil {
		return nil, err
	}
	if config.MaxBatchSize > common.MaxBatchSize {
		log.Printf("convert config.MaxBatchSize %d to %d\n", config.MaxBatchSize, common.MaxBatchSize)
		config.MaxBatchSize = common.MaxBatchSize
	}
	dropTableStmt := PreDropTableStmtFmt
	if _, err := conn.Exec(strings.Replace(dropTableStmt, "?", config.TargetTable, -1)); err != nil {
		conn.Close()
		return nil, err
	}
	createSqlStmt := fmt.Sprintf(CreateTableStmtFmt, config.TargetTable)
	originFieldList := strings.SplitN(config.TargetTableFiledList, ",", -1)
	fieldList := make([]string, len(originFieldList)+1)
	fieldList[0] = fmt.Sprintf(internalIdFmt, config.TargetTable)
	for index, field := range originFieldList {
		fieldList[index+1] = field
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
		col := &Column{Name: name}
		if strings.Contains(buf.String(), PgDataTypes[TimeStampTypeIndex]) {
			col.Type = PgDataTypes[TimeStampTypeIndex]
		} else if strings.Contains(buf.String(), PgDataTypes[CharacterTypeIndex]) {
			col.Type = PgDataTypes[CharacterTypeIndex]
		} else {
			col.Type = buf.String()
		}
		log.Println("column info:", col)
		col.Len = uint32(clen.Int64)
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
func (table *Table) Run(done chan struct{}) {
	createStmt := table.createPrepareStateForInsert()
	table.wg.Add(common.OperationCount)
	table.stop = make(chan struct{})
	defer table.conn.Close()

	go table.Insert(createStmt)
	time.Sleep(time.Duration(100) * time.Millisecond)
	go table.Update()
	//go table.Delete()
	//go table.Select()
	for {
		select {
		case <-done:
			for i := 0; i < common.OperationCount; i++ {
				table.stop <- struct{}{}
			}
			log.Println("...stop current thread")
			return
		}
	}
}

func (table *Table) createPrepareStateForInsert() string {
	sqlStmt := fmt.Sprintf(InsertTableStmtFmt, table.pgConfig.TargetTable)
	log.Println("snum:", table.SerialColNum)
	tcolumns := make([]string, len(table.columnInfo)-table.SerialColNum)
	var index int
	for i := 0; i < len(table.columnInfo); i++ {
		if table.columnInfo[i].IsSerial {
			continue
		}
		tcolumns[index] = table.columnInfo[i].Name
		index = index + 1
	}
	return strings.Replace(sqlStmt, "?", strings.Join(tcolumns, ","), -1)
}
func (table *Table) Insert(prepareSqlStmt string) {
	log.Println("prestmt:", prepareSqlStmt)
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
				eachValue := make([]string, 0)
				var value string
				for _, v := range table.columnInfo {
					if !v.IsSerial {
						switch v.Type {
						case PgDataTypes[IntegerTypeIndex]:
							eachValue = append(eachValue, fmt.Sprintf("%d", common.GenInt32()))
							break
						case PgDataTypes[BigIntTypeIndex]:
							eachValue = append(eachValue, fmt.Sprintf("%d", common.GenInt64()))
							break
						case PgDataTypes[SmallIntTypeIndex]:
							eachValue = append(eachValue, fmt.Sprintf("%d", common.GenInt16()))
							break
						case PgDataTypes[CharacterTypeIndex]:
							eachValue = append(eachValue, fmt.Sprintf("'%s'", common.GenVarch(uint32(v.Len))))
							break
						case PgDataTypes[TextTypeIndex]:
							eachValue = append(eachValue, fmt.Sprintf("'%s'", common.GenVarch(uint32(v.Len))))
							break
						case PgDataTypes[TimeStampTypeIndex]:
							eachValue = append(eachValue, fmt.Sprintf("to_timestamp('%s','%s')", time.Now().Format("2006-01-02 15:04:05"), "yyyy-mm-dd hh24:mi:ss"))
							break
						case PgDataTypes[DateTypeIndex]:
							eachValue = append(eachValue, fmt.Sprintf("to_date('%s','%s')", time.Now().Format("2006-01-02 15:04:05"), "yyyy-mm-dd hh24:mi:ss"))
							break
						}

					}
				}

				if i == 0 {
					value = fmt.Sprintf("(%s)", strings.Join(eachValue, ","))
				} else {
					value = fmt.Sprintf(",(%s)", strings.Join(eachValue, ","))
				}
				sbuf.WriteString(value)
			}
			_, err := table.conn.Exec(sbuf.String())
			sbuf.Reset()
			if err != nil {
				panic(err)
				return
			}
		}

	}
}

func (table *Table) typeConvertion(colType string) interface{} {
	var v interface{}
	switch colType {
	case PgDataTypes[IntegerTypeIndex]:
		v = new(int32)
		break
	case PgDataTypes[BigIntTypeIndex]:
		v = new(int64)
		break
	case PgDataTypes[SmallIntTypeIndex]:
		v = new(int16)
		break
	case PgDataTypes[CharacterTypeIndex]:
		v = new(string)
		break
	case PgDataTypes[TextTypeIndex]:
		v = new(string)
		break
	case PgDataTypes[TimeStampTypeIndex]:
		v = new(time.Time)
		break
	case PgDataTypes[DateTypeIndex]:
		v = new(time.Time)
		break
	}
	return v
}
func (table *Table) Update() {
	table.wg.Done()
	ticker := time.NewTicker(time.Millisecond * table.pgConfig.TimeIntervalMilliSecond * 20)
	defer ticker.Stop()
	columnInfo := make([]string, len(table.columnInfo))

	for index, col := range table.columnInfo {
		columnInfo[index] = col.Name
	}
	originSelectStmt := fmt.Sprintf(QueryTableStmtFmt, strings.Join(columnInfo, ","), table.pgConfig.TargetTable, table.pgConfig.MaxBatchSize)
	log.Println("originSelectStmt:", originSelectStmt)
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			rows, err := table.conn.Query(originSelectStmt)
			if err != nil {
				panic(err)
				return
			}
			defer rows.Close()
			selectVal := make([][]interface{}, table.pgConfig.MaxBatchSize)
			for i := 0; i < table.pgConfig.MaxBatchSize; i++ {
				selectVal[i] = make([]interface{}, len(table.columnInfo))
				for j := 0; j < len(table.columnInfo); j++ {
					colType := table.columnInfo[j].Type
					selectVal[i][j] = table.typeConvertion(colType)
				}
			}
			eachUpdateStmt := make([]string, 0)
			index := 0

			for rows.Next() {
				err = rows.Scan(selectVal[index]...)
				if err != nil {
					log.Println(err)
					continue
				}
				fmt.Println(selectVal[index])
				index = index + 1
			}
			for i := 0; i < table.pgConfig.MaxBatchSize; i++ {
				updateIndex := rand.Intn(len(table.columnInfo))
				if updateIndex == 0 {
					updateIndex = 1
				}
				condIndex := rand.Intn(len(table.columnInfo))

				updateValue := table.columnInfo[updateIndex]

				condValue := table.columnInfo[condIndex]
				// 	UpdateTableStmtFmt       = "update %s set %s=? where %s = ?"
				execStmt := fmt.Sprintf(UpdateTableStmtFmt, table.pgConfig.TargetTable, updateValue.Name, condValue.Name)

				if !updateValue.IsSerial {
					switch updateValue.Type {
					case PgDataTypes[IntegerTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", common.GenInt32()), 1)
						break
					case PgDataTypes[BigIntTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", common.GenInt64()), 1)
						break
					case PgDataTypes[SmallIntTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", common.GenInt16()), 1)
						break
					case PgDataTypes[CharacterTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", common.GenVarch(uint32(updateValue.Len))), 1)
						break
					case PgDataTypes[TextTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", common.GenVarch(uint32(updateValue.Len))), 1)
						break
					case PgDataTypes[TimeStampTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("to_timestamp('%s','%s')", time.Now().Format("2006-01-02 15:04:05.000"), "yyyy-mm-dd hh24:mi:ms"), 1)
						break
					case PgDataTypes[DateTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("to_date('%s','%s')", time.Now().Format("2006-01-02 15:04:05.000"), "yyyy-mm-dd hh24:mi:ss:ms"), 1)
						break
					}
				}

				if selectVal[i][condIndex] != nil {
					switch condValue.Type {
					case PgDataTypes[IntegerTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", selectVal[i][condIndex].(*int32)), 1)
						break
					case PgDataTypes[BigIntTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", selectVal[i][condIndex].(*int64)), 1)
						break
					case PgDataTypes[SmallIntTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", selectVal[i][condIndex].(*int16)), 1)
						break
					case PgDataTypes[CharacterTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", *selectVal[i][condIndex].(*string)), 1)
						break
					case PgDataTypes[TextTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", *selectVal[i][condIndex].(*string)), 1)
						break
					case PgDataTypes[TimeStampTypeIndex]:
						dTime := selectVal[i][condIndex].(*time.Time)
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", dTime.Format("2006-01-02 15:04:05.000")), 1)
						break
					case PgDataTypes[DateTypeIndex]:
						dTime := selectVal[i][condIndex].(*time.Time)
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", dTime.Format("2006-01-02 15:04:05.000")), 1)
						break
					}
					//log.Println("updateStmt:", execStmt)
					eachUpdateStmt = append(eachUpdateStmt, execStmt)
				}
			}

			for _, stmt := range eachUpdateStmt {
				res, err := table.conn.Exec(stmt)
				if err != nil {
					fmt.Println(err)
				} else {
					n, _ := res.RowsAffected()
					log.Printf("exec stmt:%s,affect rows:%d\n", stmt, n)
				}
			}

		}

	}
	return
}
