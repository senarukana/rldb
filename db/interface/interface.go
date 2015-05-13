package engine

type StoreEngine interface {
	Init(dataPath string) error
	Set(key, val []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Snapshot() ([]byte, error)
	Recovery(b []byte) error
	Close() error
}
