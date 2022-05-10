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

package metanode

import (
	"fmt"
	"os"
	"path"
	"time"

	"github.com/cubefs/cubefs/util"
	"github.com/cubefs/cubefs/util/errors"
	"github.com/tecbot/gorocksdb"
)

const (
	TxnDefault uint = iota
	TxnSnapshot

	DBLRUCacheSize    = 1000
	DBWriteBufferSize = 4 * util.MB
)

type MetaDataKey interface {
	Compare(than []byte) int
	Value() []byte
}

var (
	ro        *gorocksdb.ReadOptions // FIXME: direct-ro, cached-ro
	wo        *gorocksdb.WriteOptions
	otxno     *gorocksdb.OptimisticTransactionOptions
	otxnoSnap *gorocksdb.OptimisticTransactionOptions
)

func init() {
	ro = gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	wo = gorocksdb.NewDefaultWriteOptions()
	wo.SetSync(false)

	otxno = gorocksdb.NewDefaultOptimisticTransactionOptions()

	otxnoSnap = gorocksdb.NewDefaultOptimisticTransactionOptions()
	otxnoSnap.SetSetSnapshot(true)
}

type BTree struct {
	db     *gorocksdb.DB
	otxnDB *gorocksdb.OptimisticTransactionDB
	cfs    []*gorocksdb.ColumnFamilyHandle
	cfNr   int
	cnt    []int
}

func cleanupStaleDBData(dir string) error {
	rootDir := path.Dir(dir)
	victim := path.Base(dir)
	expName := fmt.Sprintf("%s_%s_%v", metaDBDirExpPrefix, victim, time.Now().UnixNano())
	expPath := path.Join(rootDir, expName)
	if err := os.Rename(dir, expPath); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func NewBTree(dir string, cfNames []string) (*BTree, error) {
	var (
		otxnDB *gorocksdb.OptimisticTransactionDB
		cfs    []*gorocksdb.ColumnFamilyHandle
		err    error
	)

	nr := len(cfNames)
	if nr == 0 {
		err = errors.New("Column Family Names are nil")
		return nil, err
	}

	if cfNames[0] != "default" {
		err = errors.New("The first Column Family Name must be default")
		return nil, err
	}

	err = cleanupStaleDBData(dir)
	if err != nil && !os.IsNotExist(err) {
		err = errors.NewErrorf("Failed to clean dir %v: %v", dir, err)
		return nil, err
	}

	basedTableOptions := gorocksdb.NewDefaultBlockBasedTableOptions()
	basedTableOptions.SetBlockCache(gorocksdb.NewLRUCache(DBLRUCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(basedTableOptions)
	opts.SetCreateIfMissing(true)
	opts.SetWriteBufferSize(DBWriteBufferSize)
	opts.SetMaxWriteBufferNumber(2)
	opts.SetCompression(gorocksdb.NoCompression)

	cfOpts := make([]*gorocksdb.Options, nr)
	for i := 0; i < nr; i++ {
		cfOpts[i] = gorocksdb.NewDefaultOptions()
		cfOpts[i].SetCreateIfMissingColumnFamilies(true)
	}

	otxnDB, cfs, err = gorocksdb.OptimisticTransactionDBOpenColumnFamily(opts, dir, nr, cfNames, cfOpts)
	if err != nil {
		err = errors.NewErrorf("Failed to create optimistic transaction db: %v", err)
		return nil, err
	}

	return &BTree{
		db:     otxnDB.OptimisticTransactionDBGetBaseDB(),
		otxnDB: otxnDB,
		cfs:    cfs,
		cfNr:   nr,
		// FIXME: initialize cnt
	}, nil
}

func (b *BTree) GetNoTxn(key MetaDataKey, which int) ([]byte, error) {
	if which >= b.cfNr || b.cfs[which] == nil {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, b.cfNr)
		return nil, err
	}

	slice, err := b.db.GetCF(ro, b.cfs[which], key.Value())
	if err != nil {
		err = errors.NewErrorf("Failed to get value from db: %v", err)
		return nil, err
	}
	defer slice.Free()

	if slice.Size() == 0 {
		return nil, nil
	}

	data := make([]byte, slice.Size())
	copy(data, slice.Data())
	return data, nil
}

func (b *BTree) TransactionBegin(flag uint) (txn *gorocksdb.Transaction, snap *gorocksdb.Snapshot) {
	switch flag {
	case TxnDefault:
		txn = b.otxnDB.OptimisticTransactionBegin(wo, otxno, nil)
	case TxnSnapshot:
		txn = b.otxnDB.OptimisticTransactionBegin(wo, otxnoSnap, nil)
		snap = txn.GetSnapshot()
	default:
		panic(fmt.Sprintf("Unknown transaction flag %v", flag))
	}
	return
}

func (b *BTree) TransactionCommit(txn *gorocksdb.Transaction) error {
	err := txn.Commit()
	if err != nil {
		err = errors.NewErrorf("Failed to commit transaction: %v", err)
		return err
	}
	return nil
}

func (b *BTree) TransactionEnd(txn *gorocksdb.Transaction, snap *gorocksdb.Snapshot) {
	if snap != nil {
		txn.ReleaseSnapshot(snap)
	}
	txn.Destroy()
}

func (b *BTree) Get(txn *gorocksdb.Transaction, key MetaDataKey, which int) ([]byte, error) {
	var (
		slice *gorocksdb.Slice
		err   error
	)

	if which >= b.cfNr || b.cfs[which] == nil {
		err = errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, b.cfNr)
		return nil, err
	}

	cf := b.cfs[which]
	slice, err = txn.GetCF(ro, cf, key.Value())
	if err != nil {
		err = errors.NewErrorf("Failed to get value from column family %v: %v", which, err)
		return nil, err
	}
	defer slice.Free()

	if slice.Size() == 0 {
		return nil, nil
	}

	data := make([]byte, slice.Size())
	copy(data, slice.Data())

	return data, nil
}

func (b *BTree) Put(txn *gorocksdb.Transaction, key MetaDataKey, value []byte, which int) (err error) {
	if which >= b.cfNr || b.cfs[which] == nil {
		err = errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, b.cfNr)
		return
	}

	cf := b.cfs[which]
	err = txn.PutCF(cf, key.Value(), value)
	if err != nil {
		err = errors.NewErrorf("Failed to put value from column family %v: %v", which, err)
		return
	}

	return
}

func (b *BTree) Find(txn *gorocksdb.Transaction, key MetaDataKey, which int, fn func(data []byte)) {
	data, err := b.Get(txn, key, which)
	if err != nil {
		panic(err)
	}
	if data == nil {
		return
	}
	fn(data)
}

func (b *BTree) Has(key MetaDataKey, which int) bool {
	data, err := b.GetNoTxn(key, which)
	return data != nil && err != nil
}

func (b *BTree) Delete(txn *gorocksdb.Transaction, key MetaDataKey, which int) []byte {
	data, err := b.Get(txn, key, which)
	if err != nil {
		panic(err)
		return nil
	}

	if data == nil {
		return nil
	}

	// b.Get already checked which
	cf := b.cfs[which]
	err = txn.DeleteCF(cf, key.Value())
	if err != nil {
		err = errors.NewErrorf("Failed to delete value from column family %v: %v", which, err)
		panic(err)
		return nil
	}

	return data
}

func (b *BTree) ReplaceOrInsert(tnx *gorocksdb.Transaction, key MetaDataKey, val []byte, which int, replace bool) ([]byte, bool) {
	var (
		data []byte
		ok   bool
		err  error
	)

	if replace {
		data, err = b.Get(tnx, key, which)
		if err != nil {
			panic(err)
			return nil, ok
		}

		err = b.Put(tnx, key, val, which)
		if err != nil {
			err = errors.NewErrorf("Failed to put value to column family %v: %v", which, err)
			panic(err)
			return nil, ok
		}

		ok = true
		return data, ok
	}

	data, err = b.Get(tnx, key, which)
	if err != nil {
		panic(err)
		return nil, ok
	}
	if data == nil {
		err = b.Put(tnx, key, val, which)
		if err != nil {
			err = errors.NewErrorf("Failed to put value to column family %v: %v", which, err)
			panic(err)
			return nil, ok
		}

		ok = true
		return nil, ok
	}

	ok = false
	return data, ok
}

func (b *BTree) Traverse(
	txn *gorocksdb.Transaction,
	snap *gorocksdb.Snapshot,
	which int,
	fn func(data []byte) bool,
) {
	var opts *gorocksdb.ReadOptions

	if snap != nil {
		opts = gorocksdb.NewDefaultReadOptions()
		defer opts.Destroy()
		opts.SetSnapshot(snap)
	} else {
		opts = ro
	}

	if which >= b.cfNr || b.cfs[which] == nil {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, b.cfNr)
		panic(err)
		return
	}

	cf := b.cfs[which]
	iter := txn.NewIteratorCF(opts, cf)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if !fn(iter.Value().Data()) {
			break
		}
	}
	iter.Close()
}

func (b *BTree) TraverseRange(
	txn *gorocksdb.Transaction,
	snap *gorocksdb.Snapshot,
	which int,
	start, end MetaDataKey,
	fn func(data []byte) bool,
) {
	var opts *gorocksdb.ReadOptions

	filter := func(i *gorocksdb.Iterator) bool {
		key := i.Key().Data()
		// key < start || key >= end
		if start.Compare(key) < 0 || end.Compare(key) >= 0 {
			return true
		}
		data := i.Value().Data()
		if !fn(data) {
			return false
		}
		return true
	}

	if snap != nil {
		opts = gorocksdb.NewDefaultReadOptions()
		defer opts.Destroy()
		opts.SetSnapshot(snap)
	} else {
		opts = ro
	}

	if which >= b.cfNr || b.cfs[which] == nil {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, b.cfNr)
		panic(err)
		return
	}

	cf := b.cfs[which]
	iter := txn.NewIteratorCF(opts, cf)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if !filter(iter) {
			break
		}
	}
	iter.Close()
}

func (b *BTree) TraverseGreaterOrEqual(
	txn *gorocksdb.Transaction,
	snap *gorocksdb.Snapshot,
	which int,
	start MetaDataKey,
	fn func(data []byte) bool,
) {
	var opts *gorocksdb.ReadOptions

	filter := func(i *gorocksdb.Iterator) bool {
		key := i.Key().Data()
		// key >= start
		if start.Compare(key) > 0 {
			return true
		}
		data := i.Value().Data()
		if !fn(data) {
			return false
		}
		return true
	}

	if snap != nil {
		opts = gorocksdb.NewDefaultReadOptions()
		defer opts.Destroy()
		opts.SetSnapshot(snap)
	} else {
		opts = ro
	}

	if which >= b.cfNr || b.cfs[which] == nil {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, b.cfNr)
		panic(err)
		return
	}

	cf := b.cfs[which]
	iter := txn.NewIteratorCF(opts, cf)
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if !filter(iter) {
			break
		}
	}
	iter.Close()
}

type BTreeSnapshotIterators struct {
	snap   *gorocksdb.Snapshot
	iters  []*gorocksdb.Iterator
	totals []int
}

func (b *BTree) NewSnapshot() *gorocksdb.Snapshot {
	snap := b.db.NewSnapshot()
	return snap
}

func (b *BTree) NewSnapshotIterators() *BTreeSnapshotIterators {
	btreeSnap := &BTreeSnapshotIterators{
		snap:   b.db.NewSnapshot(),
		iters:  make([]*gorocksdb.Iterator, b.cfNr),
		totals: make([]int, b.cfNr),
	}

	ro := gorocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	ro.SetFillCache(false)
	ro.SetSnapshot(btreeSnap.snap)

	// skip default
	for i, cf := range b.cfs[1:] {
		btreeSnap.iters[i] = b.db.NewIteratorCF(ro, cf)
		btreeSnap.totals[i] = b.Len(i)
	}

	return btreeSnap
}

func (b *BTree) ReleaseSnapshotIterators(btreeSnap *BTreeSnapshotIterators) {
	for _, iter := range btreeSnap.iters {
		iter.Close()
	}
	b.db.ReleaseSnapshot(btreeSnap.snap)
}

func (si *BTreeSnapshotIterators) SnapshotTraverse(which int, fn func(item interface{}) bool) {
	if which >= len(si.iters) {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, len(si.iters))
		panic(err)
	}
	iter := si.iters[which]

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		fn(iter.Value().Data())
	}
}

func (si *BTreeSnapshotIterators) SnapshotTraverseRange(
	which int,
	start, end MetaDataKey,
	fn func(item interface{}) bool,
) {
	if which >= len(si.iters) {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, len(si.iters))
		panic(err)
	}
	iter := si.iters[which]

	filter := func(i *gorocksdb.Iterator) bool {
		key := i.Key().Data()
		// key < start || key >= end
		if start.Compare(key) < 0 || end.Compare(key) >= 0 {
			return true
		}
		data := i.Value().Data()
		if !fn(data) {
			return false
		}
		return true
	}

	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if !filter(iter) {
			break
		}
	}
}

func (si *BTreeSnapshotIterators) Len(which int) int {
	if which >= len(si.totals) {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, len(si.iters))
		panic(err)
	}

	return si.totals[which]
}

func (b *BTree) Reset() {
	// FIXME: how to avoid race between reset and access, add a lock to wait?
	// FIXME: remove all data or rename column families???
}

func (b *BTree) Len(which int) int {
	if which >= b.cfNr || b.cfs[which] == nil {
		err := errors.NewErrorf("Invalid 'which' parameter: which[%v] max[%v]", which, b.cfNr)
		panic(err)
		return 0
	}

	return b.cnt[which]
}
