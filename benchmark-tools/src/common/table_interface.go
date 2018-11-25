package common

type TableInterface interface {
	Insert(prepareSqlStmt string)
	Delete()
	Select()
	Update()
}
