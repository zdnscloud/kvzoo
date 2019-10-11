package tests

import (
	"testing"

	ut "github.com/zdnscloud/cement/unittest"
	"github.com/zdnscloud/kvzoo"
	"github.com/zdnscloud/kvzoo/backend/bolt"
	"github.com/zdnscloud/kvzoo/client"
	"github.com/zdnscloud/kvzoo/server"
)

func TestDBReplication(t *testing.T) {
	db1, err := bolt.New("s1.db")
	ut.Equal(t, err, nil)
	saddr1 := "127.0.0.1:7777"
	rdb1, err := server.New(saddr1, db1)
	ut.Equal(t, err, nil)
	go rdb1.Start()

	db2, err := bolt.New("s2.db")
	ut.Equal(t, err, nil)
	saddr2 := "127.0.0.1:7778"
	rdb2, err := server.New(saddr2, db2)
	ut.Equal(t, err, nil)
	go rdb2.Start()

	ldb, err := client.New(saddr1, []string{saddr2})
	ut.Equal(t, err, nil)
	defer func() {
		ldb.Destroy()
		ldb.Close()
		rdb1.Stop()
		rdb2.Stop()
	}()

	//replication after add
	keyPrefix, valuePrefix := "key", "value"
	keys, values := genData(keyPrefix, valuePrefix, 1000)
	tableName, _ := kvzoo.NewTableName("/xxxx/xx")
	err = loadDataToTable(ldb, tableName, keys, values)
	ut.Equal(t, err, nil)

	ut.Assert(t, tableHasData(db1, tableName, keys, values), "")
	ut.Assert(t, tableHasData(db2, tableName, keys, values), "")

	err = ldb.DeleteTable(tableName)
	ut.Equal(t, err, nil)

	keyPrefix, valuePrefix = "k", "v"
	keys, values = genData(keyPrefix, valuePrefix, 1000)
	tableName, _ = kvzoo.NewTableName("/abcxx")
	err = loadDataToTableInParal(ldb, tableName, keys, values)
	ut.Equal(t, err, nil)

	ut.Assert(t, tableHasData(db1, tableName, keys, values), "")
	ut.Assert(t, tableHasData(db2, tableName, keys, values), "")

	//replication after update
	keyPrefix, valuePrefix = "k", "vvv"
	keys, values = genData(keyPrefix, valuePrefix, 1000)
	tableName, _ = kvzoo.NewTableName("/abcxx")
	err = updateDataInTableInParal(ldb, tableName, keys, values)
	ut.Equal(t, err, nil)

	ut.Assert(t, tableHasData(db1, tableName, keys, values), "")
	ut.Assert(t, tableHasData(db2, tableName, keys, values), "")

	//replication after delete
	err = deleteDataInTableInParal(ldb, tableName, keys, values)
	ut.Equal(t, err, nil)

	data, err := getTableData(db1, tableName)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(data), 0)
	data, err = getTableData(db2, tableName)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(data), 0)
}
