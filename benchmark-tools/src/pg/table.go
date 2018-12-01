package pg

import (
	"bytes"
	"common"
	"conf"
	"database/sql"
	"fmt"
	"math/rand"
	"metric"
	"strings"
	"sync"
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
	QueryTableStmtFmt        = "select %s from %s  ORDER BY random() limit %d"
	PreDropTableStmtFmt      = "drop table if exists  ?"
	CreateTableStmtFmt       = "create table if not exists  %s(?)"
	DeleteTableStmtFmt       = "delete  from %s where %s = ?"
	UpdateTableStmtFmt       = "update %s set ? where %s=?"
	SelectTableStmtFmt       = "select ? from %s where %s = ?"
	InsertTableStmtFmt       = "insert into %s(?)values"
	SelectMetaPrepareStmtFmt = "select a.column_name,a.data_type,a.character_maximum_length,COALESCE(a.column_default,'nil') from information_schema.columns a WHERE a.table_schema = 'public' and a.table_name = '?'"
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

type Table struct {
	columnInfo       []*Column
	conn             common.Connection
	pgConfig         *conf.PgConfig
	mtx              sync.Mutex
	serialColNum     int
	wg               *sync.WaitGroup
	mainWg           *sync.WaitGroup
	stop             chan struct{}
	operationCounter *metric.OperationCounter
}

func selectColumnInfo(conn common.Connection, name string) ([]*Column, error) {
	metaStmt := strings.Replace(SelectMetaPrepareStmtFmt, "?", name, -1)
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
		err = rows.Scan(&name, &ctype, &clen, &isserial)
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
func (table *Table) initColumnValue(col *Column, b bool) string {
	var value string
	if !col.IsSerial {
		switch col.Name {
		case PgDataTypes[IntegerTypeIndex]:
			value = fmt.Sprintf("%d", common.GenInt32(b))
			break
		case PgDataTypes[BigIntTypeIndex]:
			value = fmt.Sprintf("%d", common.GenInt64(b))
			break
		case PgDataTypes[SmallIntTypeIndex]:
			value = fmt.Sprintf("%d", common.GenInt16(b))
			break
		case PgDataTypes[CharacterTypeIndex]:
			value = fmt.Sprintf("'%s'", common.GenVarch(uint32(col.Len)))
			break
		case PgDataTypes[TextTypeIndex]:
			value = fmt.Sprintf("'%s'", common.GenVarch(uint32(col.Len)))
			break
		case PgDataTypes[TimeStampTypeIndex]:
			value = fmt.Sprintf("to_timestamp('%s','%s')", time.Now().Format("2006-01-02 15:04:05"), "yyyy-mm-dd hh24:mi:ss")
			break
		case PgDataTypes[DateTypeIndex]:
			value = fmt.Sprintf("to_date('%s','%s')", time.Now().Format("2006-01-02 15:04:05"), "yyyy-mm-dd hh24:mi:ss")
			break
		}
	}
	return value

}
func (table *Table) initSelectStmt() []string {
	batchStmt := make([]string, 0)
	for i := 0; i < len(table.columnInfo); i++ {

	}
	return batchStmt
}
func NewTable(config *conf.PgConfig, operationCounter *metric.OperationCounter, mainWg *sync.WaitGroup) (*Table, error) {

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
	table := &Table{
		columnInfo:       make([]*Column, 0),
		conn:             conn,
		pgConfig:         config,
		mainWg:           mainWg,
		operationCounter: operationCounter,
	}
	cols, err := selectColumnInfo(conn, config.TargetTable)
	for _, col := range cols {
		if col.IsSerial {
			table.serialColNum = table.serialColNum + 1
		}
	}
	table.wg = &sync.WaitGroup{}
	table.columnInfo = cols
	return table, nil
}

func (table *Table) Run(done chan struct{}) {

	table.wg.Add(common.OperationCount)
	defer table.conn.Close()
	defer table.wg.Wait()
	defer table.mainWg.Done()
	table.stop = make(chan struct{}, common.OperationCount)
	go table.Insert()
	go table.Update()
	//go table.Delete()
	//go table.Select()
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

func (table *Table) createInsertPrepareStmt() string {
	sqlStmt := fmt.Sprintf(InsertTableStmtFmt, table.pgConfig.TargetTable)
	tcolumns := make([]string, len(table.columnInfo)-table.serialColNum)
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
func (table *Table) Insert() {
	if table.pgConfig.InsertBatchSize <= 0 {
		return
	}
	createStmt := table.createInsertPrepareStmt()
	defer table.wg.Done()
	var sbuf bytes.Buffer
	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			sbuf.WriteString(createStmt)
			for i := 0; i < table.pgConfig.InsertBatchSize; i++ {
				colValues := make([]string, len(table.columnInfo)-1)
				for j := 0; j < len(table.columnInfo); j++ {
					colValues[j] = table.initColumnValue(table.columnInfo[j], true)
				}
				if i == 0 {
					sbuf.WriteString(fmt.Sprintf("(%s)", strings.Join(colValues, ",")))
				} else {
					sbuf.WriteString(fmt.Sprintf(",(%s)", strings.Join(colValues, ",")))
				}
			}
			start := time.Now()
			_, err := table.conn.Exec(sbuf.String())
			sbuf.Reset()
			if err != nil {
				log.Debugf("%s:%v\n", "insert", err)
				continue
			}
			table.operationCounter.AddDuration((uint64(time.Since(start).Nanoseconds() / 1000000)))
			table.operationCounter.AddInsertCount(uint64(table.pgConfig.InsertBatchSize))
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
	if table.pgConfig.UpdateBatchSize == 0 {
		return
	}
	if !table.validMeta() {
		return
	}
	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < table.pgConfig.UpdateBatchSize; i++ {
				condIndex := r.Intn(len(table.columnInfo))
				updateCount := r.Intn(len(table.columnInfo))
				var st string
				colv := make([]string, updateCount)
				for j := 0; j < updateCount && !table.columnInfo[j].IsSerial; j++ {
					colv[j] = fmt.Sprintf("%s=%s", table.columnInfo[j].Name, table.initColumnValue(table.columnInfo[j], true))
				}
				st = strings.Replace(fmt.Sprintf(UpdateTableStmtFmt, table.pgConfig.TargetTable, table.columnInfo[condIndex].Name), "?", strings.Join(colv, ","), -1)
				if table.columnInfo[condIndex].IsSerial {
					n := rand.Int63n(int64(table.operationCounter.InsertCount))
					st = strings.Replace(st, "?", fmt.Sprintf("%d", n), -1)
				} else {
					st = strings.Replace(st, "?", table.initColumnValue(table.columnInfo[condIndex], false), -1)
				}
				start := time.Now()
				_, err := table.conn.Query(st)
				if err != nil {
					log.Debugln(err, st)
					continue
				}
				table.operationCounter.AddUpdateCount(1)
				table.operationCounter.AddDuration(uint64(time.Since(start).Nanoseconds() / 1000000))
			}
		}
	}

}
func (table *Table) validMeta() bool {
	if table.pgConfig.InsertBatchSize == 0 {
		cols, err := selectColumnInfo(table.conn, table.pgConfig.TargetTable)
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
	if !table.validMeta() {
		return
	}
	//	DeleteTableStmtFmt       = "delete  from %s where %s = ?"
	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < table.pgConfig.DeleteBatchSize; i++ {
				var st string
				condIndex := r.Intn(len(table.columnInfo))
				st = fmt.Sprintf(DeleteTableStmtFmt, table.pgConfig.TargetTable, table.columnInfo[condIndex].Name)
				if table.columnInfo[condIndex].IsSerial {
					n := rand.Int63n(int64(table.operationCounter.Count))
					st = strings.Replace(st, "?", fmt.Sprintf("%s", n), -1)
				} else {
					st = strings.Replace(st, "?", table.initColumnValue(table.columnInfo[condIndex], true), -1)
				}
				start := time.Now()
				_, err := table.conn.Query(st)
				if err != nil {
					log.Debugf("%s:%v:\n", "select", err)
					continue
				}

				table.operationCounter.AddDeleteCount(1)
				table.operationCounter.AddDuration(uint64(time.Since(start).Nanoseconds() / 1000000))
			}
		}
	}
}
func (table *Table) Select() {
	defer table.wg.Done()
	if table.pgConfig.QueryBatchSize <= 0 {
		return
	}
	if !table.validMeta() {
		return
	}

	// 	SelectTableStmtFmt       = "select * from %s where %s = ?"

	ticker := time.NewTicker(table.pgConfig.TimeIntervalMilliSecond * time.Microsecond)
	defer ticker.Stop()
	for {
		select {
		case <-table.stop:
			return
		case <-ticker.C:
			r := rand.New(rand.NewSource(time.Now().UnixNano()))
			for i := 0; i < table.pgConfig.DeleteBatchSize; i++ {
				var st string
				condIndex := r.Intn(len(table.columnInfo) - 1)
				st = fmt.Sprintf(DeleteTableStmtFmt, table.pgConfig.TargetTable, table.columnInfo[condIndex].Name)
				if table.columnInfo[condIndex].IsSerial {
					n := rand.Int63n(int64(table.operationCounter.Count - 1))
					st = strings.Replace(st, "?", fmt.Sprintf("%s", n), -1)
				} else {
					st = strings.Replace(st, "?", table.initColumnValue(table.columnInfo[condIndex], true), -1)
				}
				start := time.Now()
				_, err := table.conn.Query(st)
				if err != nil {
					log.Debugf("%s:%v:\n", "select", err)
					continue
				}
				table.operationCounter.AddSelectCount(1)
				table.operationCounter.AddDuration(uint64(time.Since(start).Nanoseconds() / 1000000))
			}
		default:
		}

	}
}
