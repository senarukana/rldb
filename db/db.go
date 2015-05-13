package db

import (
	"os"
	"strings"

	abstract "github.com/senarukana/rldb/db/interface"
	"github.com/senarukana/rldb/db/leveldb"
)

var (
	defaultDirPerm os.FileMode = 0755
)

type engineNewFunc func() abstract.StoreEngine

var engineImpls = make(map[string]engineNewFunc)

func register(name string, engineInit engineNewFunc) {
	engineImpls[name] = engineInit
}

type DB struct {
	abstract.StoreEngine
	engineName string
	dataPath   string
}

func NewDB(engineName, dataPath string) (*DB, error) {
	if engineNew, ok := engineImpls[strings.ToLower(engineName)]; ok {
		if _, err := os.Stat(dataPath); err != nil && !os.IsExist(err) {
			if err = os.Mkdir(dataPath, defaultDirPerm); err != nil {
				return nil, err
			}
		}
		eng := engineNew()
		if err := eng.Init(dataPath); err != nil {
			return nil, err
		}
		engine := &DB{
			StoreEngine: eng,
			engineName:  engineName,
			dataPath:    dataPath,
		}
		return engine, nil
	} else {
		return nil, ErrUnknownDBEngineName
	}
}

func (engine *DB) EngineName() string {
	return engine.engineName
}

func (engine *DB) DataPath() string {
	return engine.dataPath
}

func (engine *DB) Close() error {
	return engine.Close()
}

func init() {
	register("leveldb", leveldb.NewLevelDBEngine)
}
