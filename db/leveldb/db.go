package leveldb

import (
	"errors"
	"fmt"
	"os"
	"sync"

	abstract "github.com/senarukana/rldb/db/interface"

	log "github.com/golang/glog"
	"github.com/jmhodges/levigo"
)

const (
	LEVELDB_CACHE_SIZE        = 1024 * 1024 * 16 // 16MB
	LEVELDB_BLOCK_SIZE        = 256 * 1024
	LEVELDB_BLOOM_FILTER_BITS = 64
)

type LevelDBEngine struct {
	*levigo.DB
	sync.Mutex
	dbPath       string
	snapshotPath string
}

func NewLevelDBEngine() abstract.StoreEngine {
	leveldbEngine := &LevelDBEngine{}
	return leveldbEngine
}

func newLevelDB(dataPath string, clone bool) (*levigo.DB, error) {
	var err error
	if clone {
		if err = os.RemoveAll(dataPath); err != nil {
			return nil, err
		}
	}
	opts := levigo.NewOptions()
	opts.SetCache(levigo.NewLRUCache(LEVELDB_CACHE_SIZE))
	opts.SetCreateIfMissing(true)
	opts.SetBlockSize(LEVELDB_BLOCK_SIZE)
	filter := levigo.NewBloomFilter(LEVELDB_BLOOM_FILTER_BITS)
	opts.SetFilterPolicy(filter)
	return levigo.Open(dataPath, opts)
}

func (self *LevelDBEngine) Init(dataPath string) (err error) {
	self.DB, err = newLevelDB(dataPath, false)
	if err == nil {
		self.snapshotPath = fmt.Sprintf("SNAPSHOT_%s", self.dbPath)
	}
	return err
}

func (self *LevelDBEngine) Close() error {
	self.DB.Close()
	return nil
}

func (self *LevelDBEngine) Set(key, val []byte) error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	return self.Put(wo, key, val)
}

func (self *LevelDBEngine) Get(key []byte) ([]byte, error) {
	ro := levigo.NewReadOptions()
	defer ro.Close()
	return self.DB.Get(ro, key)
}

func (self *LevelDBEngine) Delete(key []byte) error {
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	return self.DB.Delete(wo, key)
}

func (self *LevelDBEngine) Recovery(b []byte) (err error) {
	self.DB.Close()
	self.DB, err = newLevelDB(self.snapshotPath, false)
	if err != nil {
		log.Errorf("RECOVERY: Open clone DB error: %s", err.Error())
	}
	return err
}

func (self *LevelDBEngine) Snapshot() (b []byte, err error) {
	self.Lock()
	defer self.Unlock()

	opts := levigo.NewReadOptions()
	opts.SetSnapshot(self.NewSnapshot())
	defer opts.Close()
	iter := self.NewIterator(opts)
	defer iter.Close()
	cloneDB, err := newLevelDB(self.snapshotPath, true)
	if err != nil {
		log.Errorf(fmt.Sprintf("SNAPSHOT: new db error: %s", err.Error()))
		return nil, errors.New(fmt.Sprintf("SNAPSHOT: new db error: %s", err.Error()))
	}
	wo := levigo.NewWriteOptions()
	defer wo.Close()
	wb := levigo.NewWriteBatch()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		wb.Put(iter.Key(), iter.Value())
	}
	err = cloneDB.Write(wo, wb)
	if err != nil {
		log.Errorf(fmt.Sprintf("SNAPSHOT: CLONE ERROR: %s", err.Error()))
		return nil, err
	}
	cloneDB.Close()
	return nil, nil
}
