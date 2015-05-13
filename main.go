package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime/pprof"
	"strings"

	"github.com/senarukana/rldb/server"

	log "github.com/golang/glog"
)

var host string
var port int
var join string
var dbfile string
var dbpath string
var cpuprofile string
var logFile string
var logLevel string
var snapAfter int
var disableReporting bool

func init() {
	flag.StringVar(&host, "h", "localhost", "hostname")
	flag.IntVar(&port, "p", 4001, "port")
	flag.StringVar(&join, "join", "", "host:port of leader to join")
	flag.StringVar(&dbfile, "dbfile", "rldb", "sqlite filename")
	flag.StringVar(&cpuprofile, "cpuprofile", "", "write CPU profile to file")
	flag.IntVar(&snapAfter, "s", 100, "Snapshot and compact after this number of new log entries")
	flag.StringVar(&dbpath, "dbpath", "data_rldb", "dbpath")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [arguments] <data-path> \n", os.Args[0])
		flag.PrintDefaults()
	}
}

func main() {
	flag.Parse()

	// Set up profiling, if requested.
	if cpuprofile != "" {
		log.Info("Profiling enabled")
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Errorf("Unable to create path: %s", err.Error())
		}
		defer closeFile(f)

		err = pprof.StartCPUProfile(f)
		if err != nil {
			log.Errorf("Unable to start CPU Profile: %s", err.Error())
		}

		defer pprof.StopCPUProfile()
	}

	createDir(dbpath)

	s := server.NewServer(dbpath, dbfile, snapAfter, host, port)
	go func() {
		log.Error(s.ListenAndServe(join).Error())
	}()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
	log.Info("rqlite server stopped")
}

func closeFile(f *os.File) {
	if err := f.Close(); err != nil {
		log.Errorf("Unable to close file: %s", err.Error())
		os.Exit(1)
	}
}

func createFile(path string) *os.File {
	usr, _ := user.Current()
	dir := usr.HomeDir

	// Check in case of paths like "/something/~/something/"
	if path[:2] == "~/" {
		path = strings.Replace(path, "~/", dir+"/", 1)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0744); err != nil {
		log.Errorf("Unable to create path: %s", err.Error())
		os.Exit(1)
	}

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil && !strings.Contains(err.Error(), "is a directory") {
		log.Errorf("Unable to open file: %s", err.Error())
		os.Exit(1)
	}

	return f
}

func createDir(path string) {
	if err := os.MkdirAll(path, 0744); err != nil {
		log.Errorf("Unable to create path: %s", err.Error())
		os.Exit(1)
	}
}
