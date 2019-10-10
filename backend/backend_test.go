package backend

import (
	"fmt"
	"sync"
	"testing"

	ut "github.com/zdnscloud/cement/unittest"
	"github.com/zdnscloud/kvzoo"
	"github.com/zdnscloud/kvzoo/backend/bolt"
)

const (
	dbName = "teststorage.db"
)

func TestTable(t *testing.T) {
	db, err := bolt.New(dbName)
	defer func() {
		db.Close()
		db.Destroy()
	}()
	ut.Equal(t, err, nil)
	testTable(t, db)
}

func testTable(t *testing.T, db kvzoo.DB) {
	_, err := kvzoo.NewTableName("xxx")
	ut.Assert(t, err != nil, "")
	_, err = kvzoo.NewTableName("/xxx//")
	ut.Assert(t, err != nil, "")

	tn1, err := kvzoo.NewTableName("/xxx/good")
	ut.Assert(t, err == nil, "")
	tn2, err := kvzoo.NewTableName("/xxx/goodd")
	ut.Assert(t, err == nil, "")

	_, err = db.CreateOrGetTable(tn1)
	ut.Assert(t, err == nil, "")
	err = db.DeleteTable(tn2)
	ut.Assert(t, err != nil, "")
	err = db.DeleteTable(tn1)
	ut.Assert(t, err == nil, "")
}

func TestAddAndGet(t *testing.T) {
	db, err := bolt.New(dbName)
	defer func() {
		db.Close()
		db.Destroy()
	}()
	ut.Equal(t, err, nil)
	testAddAndGet(t, db)
}

func testAddAndGet(t *testing.T, db kvzoo.DB) {
	keyPrefix, valuePrefix := "key", "v"
	keys, values := genData(keyPrefix, valuePrefix, 1000)
	tableName, _ := kvzoo.NewTableName("/xxxx/xx")
	err := loadDataToTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	ut.Assert(t, tableHasData(db, tableName, keys, values), "")
	db.DeleteTable(tableName)

	keys, values = genData(keyPrefix, valuePrefix, 10)
	err = loadDataToTableInParal(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	ut.Assert(t, tableHasData(db, tableName, keys, values), "")
	ut.Assert(t, tableHasData(db, tableName, []string{"k1"}, []string{"v1"}) == false, "")
	db.DeleteTable(tableName)

	err = loadDataToTableInParal(db, tableName, []string{"k1", "k1"}, []string{"v1", "v2"})
	ut.Assert(t, err != nil, "")
	db.DeleteTable(tableName)

	err = loadDataToTable(db, tableName, []string{"k1", "k1"}, []string{"v1", "v2"})
	ut.Assert(t, err != nil, "")
	db.DeleteTable(tableName)
}

func genData(keyPrefix, valuePrefix string, count int) ([]string, []string) {
	keys := make([]string, 0, count)
	values := make([]string, 0, count)
	for i := 0; i < count; i++ {
		keys = append(keys, fmt.Sprintf("%s%d", keyPrefix, i))
		values = append(values, fmt.Sprintf("%s%d", valuePrefix, i))
	}
	return keys, values
}

func loadDataToTable(db kvzoo.DB, tableName kvzoo.TableName, keys, values []string) error {
	return applyToTable(db, tableName, func(tx kvzoo.Transaction) dbOp {
		return tx.Add
	}, keys, values)
}

func loadDataToTableInParal(db kvzoo.DB, tableName kvzoo.TableName, keys, values []string) error {
	return applyToTableInParal(db, tableName, func(tx kvzoo.Transaction) dbOp {
		return tx.Add
	}, keys, values)
}

type dbOpGen func(kvzoo.Transaction) dbOp
type dbOp func(string, []byte) error

func applyToTable(db kvzoo.DB, tableName kvzoo.TableName, opGen dbOpGen, keys, values []string) error {
	table, err := db.CreateOrGetTable(tableName)
	if err != nil {
		return err
	}

	tx, err := table.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	op := opGen(tx)
	for i := 0; i < len(keys); i++ {
		if err := op(keys[i], []byte(values[i])); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func applyToTableInParal(db kvzoo.DB, tableName kvzoo.TableName, opGen dbOpGen, keys, values []string) error {
	table, err := db.CreateOrGetTable(tableName)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	errors := make(chan error, len(keys))
	for i := 0; i < len(keys); i++ {
		wg.Add(1)
		go func(k, v string) {
			defer wg.Done()
			tx, err := table.Begin()
			if err != nil {
				errors <- err
				return
			}
			op := opGen(tx)
			if err := op(k, []byte(v)); err != nil {
				errors <- err
				tx.Rollback()
				return
			}
			if err := tx.Commit(); err != nil {
				errors <- err
				return
			}
		}(keys[i], values[i])
	}
	wg.Wait()
	if len(errors) != 0 {
		return <-errors
	} else {
		return nil
	}
}

func tableHasData(db kvzoo.DB, tableName kvzoo.TableName, keys, values []string) bool {
	table, err := db.CreateOrGetTable(tableName)
	if err != nil {
		return false
	}

	tx, err := table.Begin()
	if err != nil {
		return false
	}
	defer tx.Rollback()

	for i := 0; i < len(keys); i++ {
		value, err := tx.Get(keys[i])
		if err != nil {
			return false
		}

		if string(value) != values[i] {
			return false
		}
	}

	return true
}

func TestUpdate(t *testing.T) {
	db, err := bolt.New(dbName)
	ut.Equal(t, err, nil)
	defer func() {
		db.Close()
		db.Destroy()
	}()
	testUpdate(t, db)
}

func testUpdate(t *testing.T, db kvzoo.DB) {
	keys, values := genData("key", "value", 1000)
	tableName, _ := kvzoo.NewTableName("/xxxx")
	err := loadDataToTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	_, values = genData("key", "vv", 1000)
	err = updateDataInTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	ut.Assert(t, tableHasData(db, tableName, keys, values), "")
	db.DeleteTable(tableName)

	keys, values = genData("k", "value", 10)
	err = loadDataToTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	_, values = genData("k", "vvv", 10)
	err = updateDataInTableInParal(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	ut.Assert(t, tableHasData(db, tableName, keys, values), "")

	err = updateDataInTableInParal(db, tableName, []string{"nk1", "nk2"}, []string{"v1", "v2"})
	ut.Assert(t, err != nil, "")

	err = updateDataInTable(db, tableName, []string{"key1", "key2"}, []string{"v1", "v2"})
	ut.Assert(t, err != nil, "")

	ut.Assert(t, tableHasData(db, tableName, keys, values), "")
}

func updateDataInTable(db kvzoo.DB, tableName kvzoo.TableName, keys, values []string) error {
	return applyToTable(db, tableName, func(tx kvzoo.Transaction) dbOp {
		return tx.Update
	}, keys, values)
}

func updateDataInTableInParal(db kvzoo.DB, tableName kvzoo.TableName, keys, values []string) error {
	return applyToTableInParal(db, tableName, func(tx kvzoo.Transaction) dbOp {
		return tx.Update
	}, keys, values)
}

func TestDelete(t *testing.T) {
	db, err := bolt.New(dbName)
	ut.Equal(t, err, nil)
	defer func() {
		db.Close()
		db.Destroy()
	}()
	testDelete(t, db)
}

func testDelete(t *testing.T, db kvzoo.DB) {
	keys, values := genData("key", "value", 1000)
	tableName, _ := kvzoo.NewTableName("/xxxx/xxx/xxxxx")
	err := loadDataToTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	data, err := getTableData(db, tableName)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(data), 1000)

	keys, values = genData("key", "value", 500)
	err = deleteDataInTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	data, err = getTableData(db, tableName)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(data), 500)
	db.DeleteTable(tableName)

	keys, values = genData("key", "value", 100)
	err = loadDataToTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	err = deleteDataInTable(db, tableName, []string{"k1", "k2"}, []string{"v1", "v2"})
	ut.Assert(t, err == nil, "")
	err = deleteDataInTableInParal(db, tableName, []string{"kk1", "kk2"}, []string{"v1", "v2"})
	ut.Assert(t, err == nil, "")
	err = deleteDataInTableInParal(db, tableName, keys, values)
	data, err = getTableData(db, tableName)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(data), 0)
}

func deleteDataInTable(db kvzoo.DB, tableName kvzoo.TableName, keys, values []string) error {
	return applyToTable(db, tableName, func(tx kvzoo.Transaction) dbOp {
		return func(k string, v []byte) error {
			return tx.Delete(k)
		}
	}, keys, values)
}

func deleteDataInTableInParal(db kvzoo.DB, tableName kvzoo.TableName, keys, values []string) error {
	return applyToTableInParal(db, tableName, func(tx kvzoo.Transaction) dbOp {
		return func(k string, v []byte) error {
			return tx.Delete(k)
		}
	}, keys, values)
}

func getTableData(db kvzoo.DB, tableName kvzoo.TableName) (map[string][]byte, error) {
	table, err := db.CreateOrGetTable(tableName)
	if err != nil {
		return nil, err
	}

	tx, err := table.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return tx.List()
}

func TestList(t *testing.T) {
	db, err := bolt.New(dbName)
	ut.Equal(t, err, nil)
	defer func() {
		db.Close()
		db.Destroy()
	}()
	testList(t, db)
}

func testList(t *testing.T, db kvzoo.DB) {
	keys, values := genData("key", "value", 1000)
	tableName, _ := kvzoo.NewTableName("/xxxx/x/xxxxx")
	err := loadDataToTable(db, tableName, keys, values)
	ut.Equal(t, err, nil)
	data, err := getTableData(db, tableName)
	ut.Equal(t, err, nil)
	assertMapEqualsToSlices(t, data, keys, values)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			data, err := getTableData(db, tableName)
			ut.Equal(t, err, nil)
			assertMapEqualsToSlices(t, data, keys, values)
			wg.Done()
		}()
	}
	wg.Wait()
}

func assertMapEqualsToSlices(t *testing.T, m map[string][]byte, keys, values []string) {
	ut.Equal(t, len(m), len(keys))
	for i := 0; i < len(keys); i++ {
		ut.Equal(t, string(m[keys[i]]), values[i])
	}
}

func TestNestedTable(t *testing.T) {
	db, err := bolt.New(dbName)
	ut.Equal(t, err, nil)
	defer func() {
		db.Close()
		db.Destroy()
	}()
	testNestedTable(t, db)
}

func testNestedTable(t *testing.T, db kvzoo.DB) {
	t1, _ := kvzoo.NewTableName("/app/cd/ns1")
	keys, values := genData("key", "value", 1000)
	err := loadDataToTable(db, t1, keys, values)
	ut.Assert(t, err == nil, "")
	err = db.DeleteTable("/app/cd")
	ut.Assert(t, err == nil, "")
	data, err := getTableData(db, t1)
	ut.Equal(t, err, nil)
	ut.Equal(t, len(data), 0)

	loadDataToTable(db, t1, keys, values)
	data, _ = getTableData(db, t1)
	ut.Equal(t, len(data), 1000)

	t2, _ := kvzoo.NewTableName("/app/cd/ns2")
	loadDataToTable(db, t2, keys, values)
	data, _ = getTableData(db, t2)
	ut.Equal(t, len(data), 1000)

	tn, _ := kvzoo.NewTableName("/app")
	db.DeleteTable(tn)
	data, _ = getTableData(db, t1)
	ut.Equal(t, len(data), 0)
	data, _ = getTableData(db, t2)
	ut.Equal(t, len(data), 0)
}

func TestTxRollback(t *testing.T) {
	db, err := bolt.New(dbName)
	defer func() {
		db.Close()
		db.Destroy()
	}()
	ut.Equal(t, err, nil)
	testTxRollback(t, db)
}

func testTxRollback(t *testing.T, db kvzoo.DB) {
	tn, err := kvzoo.NewTableName("/good")
	ut.Assert(t, err == nil, "")

	table, err := db.CreateOrGetTable(tn)
	ut.Assert(t, err == nil, "")

	tx, err := table.Begin()
	ut.Assert(t, err == nil, "")
	keys, values := genData("k", "v", 10)
	for i := 0; i < 10; i++ {
		tx.Add(keys[i], []byte(values[i]))
	}
	tx.Rollback()
	data, err := getTableData(db, tn)
	ut.Assert(t, err == nil, "")
	ut.Equal(t, len(data), 0)

	loadDataToTable(db, tn, keys, values)
	tx, err = table.Begin()
	ut.Assert(t, err == nil, "")
	for _, key := range keys {
		tx.Delete(key)
	}
	tx.Rollback()
	ut.Assert(t, tableHasData(db, tn, keys, values), "")

	_, newValues := genData("k", "value", 10)
	tx, err = table.Begin()
	ut.Assert(t, err == nil, "")
	for i := 0; i < 10; i++ {
		tx.Update(keys[i], []byte(newValues[i]))
	}
	tx.Rollback()
	ut.Assert(t, tableHasData(db, tn, keys, values), "")
}
