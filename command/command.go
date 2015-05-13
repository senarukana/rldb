package command

import (
	"github.com/senarukana/rldb/db"

	log "github.com/golang/glog"
	"github.com/goraft/raft"
)

// SetCommand encapsulates a sqlite statement.
type SetCommand struct {
	Key []byte `json:"key"`
	Val []byte `json:"val"`
}

// NewSetCommand creates a new SetCommand.
func NewSetCommand(key, val []byte) *SetCommand {
	return &SetCommand{
		Key: key,
		Val: val,
	}
}

// CommandName of the SetCommand in the log.
func (c *SetCommand) CommandName() string {
	return "set"
}

// Apply executes an sqlite statement.
func (c *SetCommand) Apply(server raft.Server) (interface{}, error) {
	log.V(5).Infof("Applying SetCommand: key='%s', val='%s'", string(c.Key), string(c.Val))
	db := server.Context().(*db.DB)
	return nil, db.Set(c.Key, c.Val)
}

// DeleteCommand encapsulates a sqlite statement.
type DeleteCommand struct {
	Key []byte `json:"key"`
}

// NewSetCommand creates a new SetCommand.
func NewDeleteCommand(key []byte) *DeleteCommand {
	return &DeleteCommand{
		Key: key,
	}
}

// CommandName of the DeleteCommand in the log.
func (c *DeleteCommand) CommandName() string {
	return "delete"
}

// Apply executes an sqlite statement.
func (c *DeleteCommand) Apply(server raft.Server) (interface{}, error) {
	log.V(5).Infof("Applying DeleteCommand: '%s'", string(c.Key))
	db := server.Context().(*db.DB)
	return nil, db.Delete(c.Key)
}
