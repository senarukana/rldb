package db

import (
	"errors"
)

var (
	ErrUnknownDBEngineName = errors.New("unknown db engine")
	ErrDbInitError         = errors.New("init db engine error")
)
