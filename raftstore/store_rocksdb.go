// Copyright 2018 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package raftstore

import (
	"bytes"
	"fmt"
	"math"

	"os"

	"github.com/cubefs/cubefs/util/errors"
	"github.com/cubefs/cubefs/util/log"
	"github.com/tecbot/gorocksdb"
)

// RocksDBStore is a wrapper of the gorocksdb.DB
type RocksDBStore struct {
	dir string
	db  *gorocksdb.DB
}

// NewRocksDBStore returns a new RocksDB instance.
func NewRocksDBStore(dir string, lruCacheSize, writeBufferSize int) (store *RocksDBStore, err error) {
	if err = os.MkdirAll(dir, os.ModePerm); err != nil {
		return
	}
	store = &RocksDBStore{dir: dir}
	if err = store.Open(lruCacheSize, writeBufferSize); err != nil {
		return
	}
	return
}

// Open opens the RocksDB instance.
func (rs *RocksDBStore) Open(lruCacheSize, writeBufferSize int) error {
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(lruCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(writeBufferSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)
	db, err := gorocksdb.OpenDb(opts, rs.dir)
	if err != nil {
		err = fmt.Errorf("action[openRocksDB],err:%v", err)
		return err
	}
	rs.db = db
	return nil

}

func OpenWithColumnFamilies(
	dir string,
	cfNames []string,
	lruCacheSize,
	writeBufferSize int,
) (*RocksDBStore, []*gorocksdb.ColumnFamilyHandle, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, nil, err
	}
	rs := &RocksDBStore{dir: dir}

	// FIXME: need adjust options?
	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(lruCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(writeBufferSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)

	cfOpts := make([]*gorocksdb.Options, 0)
	for i := 0; i < len(cfNames); i++ {
		cfOpt := gorocksdb.NewDefaultOptions()
		cfOpt.SetCreateIfMissingColumnFamilies(true)
		cfOpts = append(cfOpts, cfOpt)
	}

	db, cfs, err := gorocksdb.OpenDbColumnFamilies(opts, dir, cfNames, cfOpts)
	if err != nil {
		err = fmt.Errorf("action[OpenWithColumnFamilies] dir[%v]:%v", dir, err)
		return nil, nil, err
	}
	rs.db = db
	return rs, cfs, nil
}

func (rs *RocksDBStore) Close() {
	// FIXME: need flush?
	rs.db.Close()
}

func (rs *RocksDBStore) Flush() error {
	opts := gorocksdb.NewDefaultFlushOptions()
	opts.SetWait(true)
	err := rs.db.Flush(opts)
	opts.Destroy()
	if err != nil {
		log.LogErrorf("DEBUG: failed to flush rocksdb: %v", err)
	}
	return err
}

// Del deletes a key-value pair.
func (rs *RocksDBStore) Del(key interface{}, isSync bool) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(isSync)
	defer func() {
		wo.Destroy()
		ro.Destroy()
		wb.Destroy()
	}()
	slice, err := rs.db.Get(ro, []byte(key.(string)))
	if err != nil {
		return
	}
	result = slice.Data()
	err = rs.db.Delete(wo, []byte(key.(string)))
	return
}

// Put adds a new key-value pair to the RocksDB.
func (rs *RocksDBStore) Put(key, value interface{}, isSync bool) (result interface{}, err error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(isSync)
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Put([]byte(key.(string)), value.([]byte))
	if err := rs.db.Write(wo, wb); err != nil {
		return nil, err
	}
	result = value
	return result, nil
}

// Get returns the value based on the given key.
func (rs *RocksDBStore) Get(key interface{}) (result interface{}, err error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	return rs.db.GetBytes(ro, []byte(key.(string)))
}

// DeleteKeyAndPutIndex deletes the key-value pair based on the given key and put other keys in the cmdMap to RocksDB.
// TODO explain
func (rs *RocksDBStore) DeleteKeyAndPutIndex(key string, cmdMap map[string][]byte, isSync bool) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	wb := gorocksdb.NewWriteBatch()
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Delete([]byte(key))
	for otherKey, value := range cmdMap {
		if otherKey == key {
			continue
		}
		wb.Put([]byte(otherKey), value)
	}

	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[deleteFromRocksDB],err:%v", err)
		return err
	}
	return nil
}

// Put adds a new key-value pair to the RocksDB.
func (rs *RocksDBStore) Replace(key string, value interface{}, isSync bool) (result interface{}, err error) {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	wo.SetSync(isSync)
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	wb.Delete([]byte(key))
	wb.Put([]byte(key), value.([]byte))
	if err := rs.db.Write(wo, wb); err != nil {
		return nil, err
	}
	result = value
	return result, nil
}

// BatchPut puts the key-value pairs in batch.
func (rs *RocksDBStore) BatchPut(cmdMap map[string][]byte, isSync bool) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	wb := gorocksdb.NewWriteBatch()
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()
	for key, value := range cmdMap {
		wb.Put([]byte(key), value)
	}
	if err := rs.db.Write(wo, wb); err != nil {
		err = fmt.Errorf("action[batchPutToRocksDB],err:%v", err)
		return err
	}
	return nil
}

// SeekForPrefix seeks for the place where the prefix is located in the snapshots.
func (rs *RocksDBStore) SeekForPrefix(prefix []byte) (result map[string][]byte, err error) {
	result = make(map[string][]byte)
	snapshot := rs.RocksDBSnapshot()
	it := rs.Iterator(snapshot)
	defer func() {
		it.Close()
		rs.ReleaseSnapshot(snapshot)
	}()
	it.Seek(prefix)
	for ; it.ValidForPrefix(prefix); it.Next() {
		key := it.Key().Data()
		value := it.Value().Data()
		valueByte := make([]byte, len(value))
		copy(valueByte, value)
		result[string(key)] = valueByte
		it.Key().Free()
		it.Value().Free()
	}
	if err := it.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// RocksDBSnapshot returns the RocksDB snapshot.
func (rs *RocksDBStore) RocksDBSnapshot() *gorocksdb.Snapshot {
	return rs.db.NewSnapshot()
}

// ReleaseSnapshot releases the snapshot and its resources.
func (rs *RocksDBStore) ReleaseSnapshot(snapshot *gorocksdb.Snapshot) {
	rs.db.ReleaseSnapshot(snapshot)
}

// Iterator returns the iterator of the snapshot.
func (rs *RocksDBStore) Iterator(snapshot *gorocksdb.Snapshot) *gorocksdb.Iterator {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	ro.SetSnapshot(snapshot)

	return rs.db.NewIterator(ro)
}

// Create a new column family
func (rs *RocksDBStore) CreateColumnFamily(name string) (*gorocksdb.ColumnFamilyHandle, error) {
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissingColumnFamilies(true)
	return rs.db.CreateColumnFamily(opts, name)
}

func (rs *RocksDBStore) GetCF(cf *gorocksdb.ColumnFamilyHandle, key []byte) (interface{}, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	defer ro.Destroy()
	slice, err := rs.db.GetCF(ro, cf, key)
	if err != nil {
		return nil, err
	}
	data := make([]byte, slice.Size())
	copy(data, slice.Data())
	slice.Free()
	return data, nil
}

func (rs *RocksDBStore) LoadNCF(
	cf *gorocksdb.ColumnFamilyHandle,
	nr uint64,
	loadFunc func(k, v *gorocksdb.Slice) error,
	skipKeys [][]byte,
) (cnt uint64, err error) {
	if nr == 0 {
		nr = math.MaxUint64
	}

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	iter := rs.db.NewIteratorCF(ro, cf)
	defer func() {
		iter.Close()
		ro.Destroy()
	}()

	iter.SeekToFirst()
	for nr != cnt {
		if !iter.Valid() {
			return
		}

		key := iter.Key()
		val := iter.Value()
		if skipKeys != nil {
			for _, k := range skipKeys {
				if bytes.Compare(key.Data(), k) == 0 {
					goto next
				}
			}
		}
		if err = loadFunc(iter.Key(), iter.Value()); err != nil {
			err = errors.NewErrorf("Load [%d/%d] from column family fail: %v", cnt, nr, err)
			return
		}
		cnt++
	next:
		key.Free()
		val.Free()

		iter.Next()
	}
	return
}

func (rs *RocksDBStore) PutCF(cf *gorocksdb.ColumnFamilyHandle, key, value []byte, isSync bool) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	defer wo.Destroy()
	return rs.db.PutCF(wo, cf, key, value)
}

func (rs *RocksDBStore) PutBatchCF(cf *gorocksdb.ColumnFamilyHandle, kvBatch map[string][]byte, isSync bool) error {
	wo := gorocksdb.NewDefaultWriteOptions()
	wb := gorocksdb.NewWriteBatch()
	defer func() {
		wo.Destroy()
		wb.Destroy()
	}()

	wo.SetSync(isSync)
	for key, value := range kvBatch {
		wb.PutCF(cf, []byte(key), value)
	}
	return rs.db.Write(wo, wb)
}

func (rs *RocksDBStore) DelCF(cf *gorocksdb.ColumnFamilyHandle, key []byte, isSync bool) (interface{}, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(isSync)
	defer func() {
		ro.Destroy()
		wo.Destroy()
	}()

	slice, err := rs.db.Get(ro, key)
	if err != nil {
		return nil, err
	}
	data := make([]byte, slice.Size())
	copy(data, slice.Data())
	slice.Free()

	err = rs.db.DeleteCF(wo, cf, key)
	return data, err
}

func (rs *RocksDBStore) NewCheckpoint() (*gorocksdb.Checkpoint, error) {
	return rs.db.NewCheckpoint()
}
