package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kvPath string
	kvDB   *badger.DB
	config *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// create kvDataBase
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	kvDB := engine_util.CreateDB(kvPath, false)

	return &StandAloneStorage{kvPath: kvPath, kvDB: kvDB, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// do nothing.
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Close kvDataBase.
	if err := s.kvDB.Close(); err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.kvDB.NewTransaction(false)
	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.kvDB.NewTransaction(true)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			if err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value); err != nil {
				return err
			}
		case storage.Delete:
			del := m.Data.(storage.Delete)
			if err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key)); err != nil {
				return err
			}
		}
	}
	if err := txn.Commit(); err != nil {
		return err
	}
	return nil
}

// referencing raft_storage.RegionReader
type StandAloneReader struct {
	kvTxn *badger.Txn
}

func NewStandAloneReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{kvTxn: txn}
}

func (reader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	txn := reader.kvTxn
	val, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (reader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.kvTxn)
}

func (reader *StandAloneReader) Close() {
	reader.kvTxn.Discard()
}
