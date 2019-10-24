package kvzoo

type DB interface {
	//footprint of the data, used to quickly
	//verfiy the data of two db is same
	Checksum() (string, error)
	//Close and Destroy are mutually exclusive
	//release the conn
	Close() error
	//clean all the data and release the conn
	Destroy() error

	//like path, create child table, will create all parent table too
	CreateOrGetTable(TableName) (Table, error)
	//delete parent table will delete all child table
	DeleteTable(TableName) error
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
