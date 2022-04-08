package main

import (
	"fmt"
	"os"
	"path"

	"github.com/tecbot/gorocksdb"
)

const (
	workspace  = "workspace"
	checkpoint = "checkpoint"

	lruCacheSize    = 1024 * 1024
	writeBufferSize = 1024 * 1024
)

func showCheckpoint(cpDir string) {
	db, err := open(cpDir, lruCacheSize, writeBufferSize)
	if err != nil {
		fmt.Printf("Failed to open checkpoint [%s]: %v\n", cpDir, err)
		return
	}
	defer db.Close()
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := db.NewIterator(ro)
	fmt.Printf("  => [checkpoint]:\n")
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		val := string(iter.Value().Data())
		fmt.Printf("\t[%s] -> [%s]\n", key, val)
	}
	iter.Close()
}

func showDB(db *gorocksdb.DB) {
	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()

	iter := db.NewIterator(ro)
	fmt.Printf("  => [DB]:\n")
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key := string(iter.Key().Data())
		val := string(iter.Value().Data())
		fmt.Printf("\t[%s] -> [%s]\n", key, val)
	}
	iter.Close()
}

func open(dir string, lruCacheSize int, writeBufferSize int) (db *gorocksdb.DB, err error) {
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(lruCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(writeBufferSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)
	db, err = gorocksdb.OpenDb(opts, dir)
	return
}

func put(db *gorocksdb.DB, key, val interface{}) (err error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(true)
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()

	wb.Put([]byte(key.(string)), val.([]byte))
	if err = db.Write(wo, wb); err != nil {
		return err
	}

	return
}

func test_checkpoint(rootDir string) {
	fmt.Printf("================ start [test_checkpoint] ===================\n")
	// create db
	workDir := path.Join(rootDir, workspace)
	err := os.Mkdir(workDir, 0755)
	if err != nil {
		fmt.Printf("Failed to create directory [%s]: %v\n", workDir, err)
		return
	}
	db, err := open(workDir, lruCacheSize, writeBufferSize)
	if err != nil {
		fmt.Printf("Failed to open db [%s]: %v\n", workDir, err)
		return
	}
	defer db.Close()

	// save value in workspace
	key := "1"
	val := []byte("this value 1")
	fmt.Printf("Insert [%s] -> [%s]\n", key, val)
	err = put(db, key, val)
	if err != nil {
		fmt.Printf("Failed to put data: %v\n", err)
		return
	}

	// do checkpoint
	cpDir := path.Join(rootDir, checkpoint)
	cp, err := db.NewCheckpoint()
	if err != nil {
		fmt.Printf("Failed to new checkpoint: %v\n", err)
		return
	}
	err = cp.CreateCheckpoint(cpDir, 0)
	cp.Destroy()
	if err != nil {
		fmt.Printf("Failed to create checkpoint [%s]: %v\n", cpDir, err)
		return
	}

	// show DB and checkpoint
	showDB(db)
	showCheckpoint(cpDir)

	// save a new value in workspace
	key = "2"
	val = []byte("this value 2")
	fmt.Printf("Insert [%s] -> [%s]\n", key, val)
	err = put(db, key, val)
	if err != nil {
		fmt.Printf("Failed to put data: %v\n", err)
		return
	}

	// show DB and checkpoint
	showDB(db)
	showCheckpoint(cpDir)
	fmt.Printf("================ end [test_checkpoint] ===================\n\n")
}

func test_put_same_key(rootDir string) {
	fmt.Printf("================ start [test_put_same_key] ===================\n")
	// create db
	workDir := path.Join(rootDir, workspace)
	err := os.Mkdir(workDir, 0755)
	if err != nil && !os.IsExist(err) {
		fmt.Printf("Failed to create directory [%s]: %v\n", workDir, err)
		return
	}
	db, err := open(workDir, lruCacheSize, writeBufferSize)
	if err != nil {
		fmt.Printf("Failed to open db [%s]: %v\n", workDir, err)
		return
	}
	defer db.Close()

	showDB(db)

	// save value in workspace
	key := "1"
	val := []byte("this is new value of 1")
	fmt.Printf("Insert [%s] -> [%s]\n", key, val)
	err = put(db, key, val)
	if err != nil {
		fmt.Printf("Failed to put data: %v\n", err)
		return
	}

	showDB(db)

	// save a new value in workspace
	key = "1"
	val = []byte("this is the latest value of 1")
	fmt.Printf("Insert [%s] -> [%s]\n", key, val)
	err = put(db, key, val)
	if err != nil {
		fmt.Printf("Failed to put data: %v\n", err)
		return
	}

	// show DB and checkpoint
	showDB(db)
	fmt.Printf("================ end [test_put_same_key] ===================\n\n")
}

func main() {
	if len(os.Args) != 2 {
		fmt.Printf("Need one parameter to specify test directory\n")
		return
	}

	// prepare test root directory
	rootDir := os.Args[1]
	err := os.Mkdir(rootDir, 0755)
	if err != nil {
		fmt.Printf("Failed to create directory [%s]: %v\n", rootDir, err)
		return
	}
	defer os.RemoveAll(rootDir)

	test_checkpoint(rootDir)

	test_put_same_key(rootDir)
}
