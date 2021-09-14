package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer.
// It is aware of the implementation of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	// Your Data Here (4C).
	txn  *MvccTxn
	iter engine_util.DBIterator
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	// Your Code Here (4C).
	scanner := &Scanner{
		txn:  txn,
		iter: txn.Reader.IterCF(engine_util.CfWrite),
	}
	scanner.iter.Seek(EncodeKey(startKey, scanner.txn.StartTS))
	return scanner
}

func (scan *Scanner) Close() {
	// Your Code Here (4C).
	scan.iter.Close()
}

// Next returns the next key/value pair from the scanner.
//// If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	// Your Code Here (4C).
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item := scan.iter.Item()
	gotKey := item.KeyCopy(nil)
	userKey := DecodeUserKey(gotKey)

	scan.iter.Seek(EncodeKey(userKey, scan.txn.StartTS))
	if !scan.iter.Valid() {
		return nil, nil, nil
	}
	item = scan.iter.Item()
	gotKey = item.KeyCopy(nil)
	key := DecodeUserKey(gotKey)
	writeVal, err := item.ValueCopy(nil)
	if err != nil {
		return userKey, nil, err
	}
	write, err := ParseWrite(writeVal)
	if err != nil {
		return userKey, nil, err
	}
	var value []byte
	if write.Kind == WriteKindDelete {
		value = nil
	}
	value, err = scan.txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(userKey, write.StartTS))
	if !bytes.Equal(key, userKey) {
		return scan.Next()
	}
	for {
		if !bytes.Equal(key, userKey) {
			break
		}
		scan.iter.Next()
		if !scan.iter.Valid() {
			break
		}
		tempItem := scan.iter.Item()
		gotKey = tempItem.KeyCopy(nil)
		key = DecodeUserKey(gotKey)
	}
	return userKey, value, err
}
