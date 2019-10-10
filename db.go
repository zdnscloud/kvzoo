package kvzoo

type DB interface {
	Close() error
	//like path, create child table, will create all parent table too
	CreateOrGetTable(TableName) (Table, error)
	//delete parent table will delete all child table
	DeleteTable(TableName) error
	Destroy() error
}

type Table interface {
	Begin() (Transaction, error)
}

type Transaction interface {
	Commit() error
	Rollback() error

	Add(string, []byte) error
	//delete non-exist key returns nil
	Delete(string) error
	Update(string, []byte) error
	//get non-exist key return err
	Get(string) ([]byte, error)
	List() (map[string][]byte, error)
}
