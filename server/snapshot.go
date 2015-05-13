package server

import (
	"github.com/senarukana/rldb/db"

	log "github.com/golang/glog"
)

// DbStateMachine contains the DB path.
type DbStateMachine struct {
	dbpath string
	db     *db.DB
}

// NewDbStateMachine returns a StateMachine for capturing and restoring
// the state of an sqlite database.
func NewDbStateMachine(path string, db *db.DB) *DbStateMachine {
	d := &DbStateMachine{
		dbpath: path,
		db:     db,
	}
	log.V(1).Infof("New DB state machine created with path: %s", path)
	return d
}

// Save captures the state of the database. The caller must ensure that
// no transaction is taking place during this call.
//
// http://sqlite.org/howtocorrupt.html states it is safe to do this
// as long as no transaction is in progress.
func (d *DbStateMachine) Save() ([]byte, error) {
	log.V(2).Infof("Capturing database state from path: %s", d.dbpath)
	if _, err := d.db.Snapshot(); err != nil {
		log.Errorf("Failed to save state: %s", err.Error())
		return nil, err
	}
	log.V(2).Infof("Database state successfully saved to %s", d.dbpath)
	return nil, nil
}

// Recovery restores the state of the database using the given data.
func (d *DbStateMachine) Recovery(b []byte) error {
	log.V(2).Infof("Restoring database state to path: %s", d.dbpath)
	if err := d.db.Recovery(b); err != nil {
		log.Errorf("Failed to recover state: %s", err.Error())
		return err
	}
	log.V(2).Infof("Database restored successfully to %s", d.dbpath)
	return nil
}
