package pg

import (
	"bytes"
	"common"
	"conf"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Sirupsen/logrus"
)

type TypeIndex int

const (
	MaxTextLen = 1024
)
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
	QueryTableStmtFmt   = "select %s from %s  ORDER BY random() limit %d"
	PreDropTableStmtFmt = "drop table if exists  ?"
	CreateTableStmtFmt  = "create table if not exists  %s(?)"
	DeleteTableStmtFmt  = "delete  from %s where %s = ?"
	UpdateTableStmtFmt  = "update %s set ? where %s = %d"
	SelectTableStmtFmt  = "select ? from %s where %s = %s"

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
		"common",
	}
)

type Column struct {
	Name     string
	Type     string
	Len      uint32
	IsSerial bool
}

type SelectColumn struct {
	Name   string
	Val    string
	IsInit bool
}
type Table struct {
	columnInfo       []*Column
	conn             common.Connection
	pgConfig         *conf.PgConfig
	mtx              sync.Mutex
	SerialColNum     int
	wg               *sync.WaitGroup
	mainWg           *sync.WaitGroup
	stop             chan struct{}
	selectCh         chan []SelectColumn
	operationCounter *common.OperationCounter
}

func initMeta(conn common.Connection, tableName string) ([]*Column, error) {
	metaStmt := strings.Replace(SelectMetaPrepareStmtFmt, "?", tableName, -1)
	rows, err := conn.Query(metaStmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	metas := make([]*Column, 0)
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
		if col.Type == PgDataTypes[TextTypeIndex] {
			col.Len = uint32(MaxTextLen)
		} else {
			col.Len = uint32(clen.Int64)
		}
		buf.Reset()
		if strings.Contains(strings.ToLower(isserial), SerialKeyWord) {
			col.IsSerial = true

		} else {
			col.IsSerial = false
		}
		metas = append(metas, col)
	}
	return metas, nil
}
func (column *Column) Compare(c *Column) bool {
	if !(column.IsSerial && c.IsSerial) || column.Len != c.Len {
		return false
	}
	if column.Name != c.Name || column.Type != c.Name {
		return false
	}
	return true
}
func NewTable(config *conf.PgConfig, operationCounter *common.OperationCounter, mainWg *sync.WaitGroup) (*Table, error) {

	conn, err := NewPgConnection(config.ServerConfig)
	if err != nil {
		return nil, err
	}
	if config.DeleteBatchSize > common.MaxBatchSize {
		log.Printf("convert config.MaxBatchSize %d to %d\n", config.DeleteBatchSize, common.MaxBatchSize)
		config.DeleteBatchSize = common.MaxBatchSize
	}
	if config.InsertBatchSize > common.MaxBatchSize {
		log.Printf("convert config.MaxBatchSize %d to %d\n", config.InsertBatchSize, common.MaxBatchSize)
		config.InsertBatchSize = common.MaxBatchSize
	}
	if config.QueryBatchSize > common.MaxBatchSize {
		log.Printf("convert config.MaxBatchSize %d to %d\n", config.QueryBatchSize, common.MaxBatchSize)
		config.QueryBatchSize = common.MaxBatchSize
	}
	if config.UpdateBatchSize > common.MaxBatchSize {
		log.Printf("convert config.MaxBatchSize %d to %d\n", config.UpdateBatchSize, common.MaxBatchSize)
		config.UpdateBatchSize = common.MaxBatchSize
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
		columnInfo:       make([]*Column, 0),
		conn:             conn,
		pgConfig:         config,
		mainWg:           mainWg,
		selectCh:         make(chan []SelectColumn, config.QueryBatchSize),
		operationCounter: operationCounter,
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
		if col.Type == PgDataTypes[TextTypeIndex] {
			col.Len = uint32(MaxTextLen)
		} else {
			col.Len = uint32(clen.Int64)
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

func (table *Table) Run(done chan struct{}) {

	table.wg.Add(common.OperationCount)
	defer table.conn.Close()
	defer table.wg.Wait()
	defer table.mainWg.Done()
	createStmt := table.createPrepareStateForInsert()
	table.stop = make(chan struct{}, common.OperationCount)
	go table.Insert(createStmt)
	go table.Update()
	go table.Delete()
	go table.Select()
	for {
		select {
		case <-done:
			for i := 0; i < common.OperationCount; i++ {
				table.stop <- struct{}{}
			}
			return
		}
	}
}

func (table *Table) createPrepareStateForInsert() string {
	sqlStmt := fmt.Sprintf(InsertTableStmtFmt, table.pgConfig.TargetTable)
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
	if table.pgConfig.InsertBatchSize <= 0 {
		return
	}
	defer table.wg.Done()
	var sbuf bytes.Buffer
	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	var selectCol []SelectColumn
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			sbuf.WriteString(prepareSqlStmt)
			selectN := table.pgConfig.QueryBatchSize

			if selectN > table.pgConfig.InsertBatchSize {
				selectN = table.pgConfig.InsertBatchSize
			}
			selectCol = make([]SelectColumn, selectN)
			for i := 0; i < table.pgConfig.InsertBatchSize; i++ {

				eachValue := make([]string, len(table.columnInfo))
				var value string
				for index, v := range table.columnInfo {
					if !v.IsSerial {
						switch v.Type {
						case PgDataTypes[IntegerTypeIndex]:
							eachValue[index] = fmt.Sprintf("%d", common.GenInt32())
							break
						case PgDataTypes[BigIntTypeIndex]:
							eachValue[index] = fmt.Sprintf("%d", common.GenInt64())
							break
						case PgDataTypes[SmallIntTypeIndex]:
							eachValue[index] = fmt.Sprintf("%d", common.GenInt16())
							break
						case PgDataTypes[CharacterTypeIndex]:
							eachValue[index] = fmt.Sprintf("'%s'", common.GenVarch(uint32(v.Len)))
							break
						case PgDataTypes[TextTypeIndex]:
							eachValue[index] = fmt.Sprintf("'%s'", common.GenVarch(uint32(v.Len)))
							break
						case PgDataTypes[TimeStampTypeIndex]:
							eachValue[index] = fmt.Sprintf("to_timestamp('%s','%s')", time.Now().Format("2006-01-02 15:04:05"), "yyyy-mm-dd hh24:mi:ss")
							break
						case PgDataTypes[DateTypeIndex]:
							eachValue[index] = fmt.Sprintf("to_date('%s','%s')", time.Now().Format("2006-01-02 15:04:05"), "yyyy-mm-dd hh24:mi:ss")
							break
						}
					}
				}
				n := rand.Int31n(int32(len(table.columnInfo)))
				if n == 0 {
					n = 1
				}
				if i < selectN {
					selectCol[i].IsInit = true
					selectCol[i].Name = table.columnInfo[n].Name
					selectCol[i].Val = eachValue[n]
				}
				eachValue = eachValue[1:len(table.columnInfo)]
				if i == 0 {
					value = fmt.Sprintf("(%s)", strings.Join(eachValue, ","))
				} else {
					value = fmt.Sprintf(",(%s)", strings.Join(eachValue, ","))
				}
				sbuf.WriteString(value)
			}
			start := time.Now()
			_, err := table.conn.Exec(sbuf.String())
			sbuf.Reset()
			if err != nil {
				log.Debugf("%s:%v\n", "insert", err)
				continue
			}
			atomic.AddUint64(&table.operationCounter.Duration, (uint64(time.Since(start).Nanoseconds() / 1000000)))
			atomic.AddUint64(&table.operationCounter.Count, uint64(table.pgConfig.InsertBatchSize))
			atomic.AddUint64(&table.operationCounter.InsertCount, uint64(table.pgConfig.InsertBatchSize))
		default:
		}

		select {
		case table.selectCh <- selectCol:
		default:
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
	defer table.wg.Done()
	if table.pgConfig.UpdateBatchSize <= 0 {
		return
	}
	if !table.ValidMeta() {
		return
	}
	columnInfo := make([]string, len(table.columnInfo))

	for index, col := range table.columnInfo {
		columnInfo[index] = col.Name
	}
	originSelectStmt := fmt.Sprintf(QueryTableStmtFmt, strings.Join(columnInfo, ","), table.pgConfig.TargetTable, table.pgConfig.UpdateBatchSize)
	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			start := time.Now()
			rows, err := table.conn.Query(originSelectStmt)
			if err == nil {
				atomic.AddUint64(&table.operationCounter.Duration, (uint64(time.Since(start).Nanoseconds() / 1000000)))
				atomic.AddUint64(&table.operationCounter.SelectCount, 1)
				atomic.AddUint64(&table.operationCounter.Count, 1)
				defer rows.Close()
				selectVal := make([][]interface{}, table.pgConfig.UpdateBatchSize)
				for i := 0; i < table.pgConfig.UpdateBatchSize; i++ {
					selectVal[i] = make([]interface{}, len(table.columnInfo))
					for j := 0; j < len(table.columnInfo); j++ {
						colType := table.columnInfo[j].Type
						selectVal[i][j] = table.typeConvertion(colType)
					}
				}
				index := 0
				for rows.Next() {
					err = rows.Scan(selectVal[index]...)
					if err != nil {
						log.Debugf("%s:%v\n", "update", err)
						continue
					}
					index = index + 1
				}
				if index <= 0 {
					continue
				}
				for i := 0; i < index; i++ {
					seqId := *selectVal[i][0].(*int64)
					execStmt := fmt.Sprintf(UpdateTableStmtFmt, table.pgConfig.TargetTable, table.columnInfo[0].Name, seqId)
					updateSet := make([]string, 0)
					for colIndex, col := range table.columnInfo {
						if !table.columnInfo[colIndex].IsSerial {
							switch table.columnInfo[colIndex].Type {
							case PgDataTypes[IntegerTypeIndex]:
								updateSet = append(updateSet, fmt.Sprintf("%s =%d", col.Name, common.GenInt32()))
								break
							case PgDataTypes[BigIntTypeIndex]:
								updateSet = append(updateSet, fmt.Sprintf("%s =%d", col.Name, common.GenInt64()))
								break
							case PgDataTypes[SmallIntTypeIndex]:
								updateSet = append(updateSet, fmt.Sprintf("%s =%d", col.Name, common.GenInt16()))
								break
							case PgDataTypes[CharacterTypeIndex]:
								updateSet = append(updateSet, fmt.Sprintf("%s ='%s'", col.Name, common.GenVarch(uint32(col.Len))))
								break
							case PgDataTypes[TextTypeIndex]:
								updateSet = append(updateSet, fmt.Sprintf("%s ='%s'", col.Name, common.GenVarch(uint32(col.Len))))
								break
							case PgDataTypes[TimeStampTypeIndex]:
								updateSet = append(updateSet, fmt.Sprintf("%s =to_timestamp('%s','%s')", col.Name, time.Now().Format("2006-01-02 15:04:05.000"), "yyyy-mm-dd hh24:mi:ms"))
								break
							case PgDataTypes[DateTypeIndex]:
								updateSet = append(updateSet, fmt.Sprintf("%s =to_timestamp('%s','%s')", col.Name, time.Now().Format("2006-01-02 15:04:05.000"), "yyyy-mm-dd hh24:mi:ms"))
								break
							}
						}
					}
					execStmt = strings.Replace(execStmt, "?", strings.Join(updateSet, ","), 1)
					start := time.Now()
					_, err := table.conn.Exec(execStmt)
					if err != nil {
						log.Debugf("%s:%v\n", "update", err)
						continue
					}
					atomic.AddUint64(&table.operationCounter.Duration, (uint64(time.Since(start).Nanoseconds() / 1000000)))
					atomic.AddUint64(&table.operationCounter.UpdateCount, 1)
					atomic.AddUint64(&table.operationCounter.Count, 1)
				}
			}
		}
	}

}
func (table *Table) ValidMeta() bool {
	if table.pgConfig.InsertBatchSize == 0 {
		cols, err := initMeta(table.conn, table.pgConfig.TargetTable)
		if err != nil {
			log.Debugln(err)
			return false
		}
		for index := 0; index < len(cols); index++ {
			if !table.columnInfo[index].Compare(cols[index]) {
				log.Debugf("target table not match!")
				return false
			}
		}
	}
	return true
}
func (table *Table) Delete() {
	defer table.wg.Done()
	if table.pgConfig.DeleteBatchSize <= 0 {
		return
	}
	if !table.ValidMeta() {
		return
	}
	columnInfo := make([]string, len(table.columnInfo))

	for index, col := range table.columnInfo {
		columnInfo[index] = col.Name
	}
	originSelectStmt := fmt.Sprintf(QueryTableStmtFmt, strings.Join(columnInfo, ","), table.pgConfig.TargetTable, table.pgConfig.DeleteBatchSize)
	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			start := time.Now()
			rows, err := table.conn.Query(originSelectStmt)
			if err != nil {
				log.Debugf("%s:%v:\n", "select", err)
				continue
			}

			if err == nil {
				atomic.AddUint64(&table.operationCounter.Duration, (uint64(time.Since(start).Nanoseconds() / 1000000)))
				atomic.AddUint64(&table.operationCounter.SelectCount, 1)
				atomic.AddUint64(&table.operationCounter.Count, 1)
				defer rows.Close()
				selectVal := make([][]interface{}, table.pgConfig.DeleteBatchSize)
				for i := 0; i < table.pgConfig.DeleteBatchSize; i++ {
					selectVal[i] = make([]interface{}, len(table.columnInfo))
					for j := 0; j < len(table.columnInfo); j++ {
						colType := table.columnInfo[j].Type
						selectVal[i][j] = table.typeConvertion(colType)
					}
				}
				index := 0
				for rows.Next() {
					err = rows.Scan(selectVal[index]...)
					if err != nil {
						log.Println(err)
						continue
					}
					index = index + 1
				}
				if index <= 0 {
					continue
				}
				for i := 0; i < index; i++ {
					colIndex := rand.Int31n(int32(len(table.columnInfo)))
					colInfo := table.columnInfo[colIndex]
					execStmt := fmt.Sprintf(DeleteTableStmtFmt, table.pgConfig.TargetTable, colInfo.Name)
					switch colInfo.Type {
					case PgDataTypes[IntegerTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", *selectVal[i][colIndex].(*int32)), 1)
						break
					case PgDataTypes[BigIntTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", *selectVal[i][colIndex].(*int64)), 1)
						break
					case PgDataTypes[SmallIntTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("%d", *selectVal[i][colIndex].(*int16)), 1)
						break
					case PgDataTypes[CharacterTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", *selectVal[i][colIndex].(*string)), 1)
						break
					case PgDataTypes[TextTypeIndex]:
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", *selectVal[i][colIndex].(*string)), 1)
						break
					case PgDataTypes[TimeStampTypeIndex]:
						t := selectVal[i][colIndex].(*time.Time)
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05.000")), 1)
						break
					case PgDataTypes[DateTypeIndex]:
						t := selectVal[i][colIndex].(*time.Time)
						execStmt = strings.Replace(execStmt, "?", fmt.Sprintf("'%s'", t.Format("2006-01-02 15:04:05.000")), 1)
						break
					}
					start := time.Now()
					_, err := table.conn.Exec(execStmt)
					if err != nil {
						log.Debugf("%s:%v\n", "delete", err)
						continue
					}
					atomic.AddUint64(&table.operationCounter.Duration, (uint64(time.Since(start).Nanoseconds() / 1000000)))
					atomic.AddUint64(&table.operationCounter.DeleteCount, 1)
					atomic.AddUint64(&table.operationCounter.Count, 1)
				}
			}
		}
	}
}

func (table *Table) Select() {
	defer table.wg.Done()
	if table.pgConfig.InsertBatchSize <= 0 {
		return
	}
	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case vs := <-table.selectCh:
			if len(vs) > 0 {
				for _, v := range vs {
					if v.IsInit {
						originSelectStmt := fmt.Sprintf(SelectTableStmtFmt, table.pgConfig.TargetTable, v.Name, v.Val)

						originSelectStmt = strings.Replace(originSelectStmt, "?", "*", 1)
						start := time.Now()
						rows, err := table.conn.Query(originSelectStmt)
						if err != nil {
							log.Debugf("%s:%v\n", "select", err)
							continue
						}
						rows.Close()
						atomic.AddUint64(&table.operationCounter.Duration, (uint64(time.Since(start).Nanoseconds() / 1000000)))
						atomic.AddUint64(&table.operationCounter.SelectCount, 1)
						atomic.AddUint64(&table.operationCounter.Count, 1)
					}
				}
			}
		}
	}
}
