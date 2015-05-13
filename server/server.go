package server

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/senarukana/rldb/command"
	"github.com/senarukana/rldb/db"

	log "github.com/golang/glog"
	"github.com/goraft/raft"
	"github.com/gorilla/mux"
)

var (
	DEFAULT_ENGINE = "LEVELDB"
)

type QueryResponse struct {
	Error string `json:"error"`
	Value []byte `json:"value"`
}

type SnapshotConf struct {
	// The index when the last snapshot happened
	lastIndex uint64

	// If the incremental number of index entries since the last
	// snapshot exceeds snapshotAfter rqlite will do a snapshot
	snapshotAfter uint64
}

// Server is is a combination of the Raft server and an HTTP
// server which acts as the transport.
type Server struct {
	name       string
	host       string
	port       int
	path       string
	router     *mux.Router
	raftServer raft.Server
	httpServer *http.Server
	dbPath     string
	db         *db.DB
	snapConf   *SnapshotConf
	inSnapshot bool
	sync.Mutex
}

// NewServer creates a new server.
func NewServer(dataDir string, dbfile string, snapAfter int, host string, port int) *Server {
	dbPath := path.Join(dataDir, dbfile)

	// Raft requires randomness.
	rand.Seed(time.Now().UnixNano())
	log.Info("Raft random seed initialized")

	// Setup commands.
	raft.RegisterCommand(&command.SetCommand{})
	raft.RegisterCommand(&command.DeleteCommand{})
	log.Info("Raft commands registered")
	d, err := db.NewDB(DEFAULT_ENGINE, dbPath)
	if err != nil {
		log.Fatalf("INIT DB ERROR: %s", err.Error())
	}
	s := &Server{
		host:     host,
		port:     port,
		path:     dataDir,
		dbPath:   dbPath,
		snapConf: &SnapshotConf{snapshotAfter: uint64(snapAfter)},
		db:       d,
		router:   mux.NewRouter(),
	}

	// Read existing name or generate a new one.
	if b, err := ioutil.ReadFile(filepath.Join(dataDir, "name")); err == nil {
		s.name = string(b)
	} else {
		s.name = fmt.Sprintf("%07x", rand.Int())[0:7]
		if err = ioutil.WriteFile(filepath.Join(dataDir, "name"), []byte(s.name), 0644); err != nil {
			panic(err)
		}
	}

	return s
}

func (s *Server) ListenAndServe(leader string) error {
	var err error

	log.V(1).Infof("Initializing Raft Server: %s", s.path)

	// Initialize and start Raft server.
	transporter := raft.NewHTTPTransporter("/raft", 200*time.Millisecond)
	dbStateMachine := NewDbStateMachine(s.dbPath, s.db)
	s.raftServer, err = raft.NewServer(s.name, s.path, transporter, dbStateMachine, s.db, "")
	if err != nil {
		log.Errorf("Failed to create new Raft server: %s", err.Error())
		return err
	}

	log.Info("Loading latest snapshot, if any, from disk")
	if err := s.raftServer.LoadSnapshot(); err != nil && os.IsNotExist(err) {
		log.V(1).Info("no snapshot found")
	} else if err != nil {
		log.Errorf("Error loading snapshot: %s", err.Error())
	}

	transporter.Install(s.raftServer, s)
	if err := s.raftServer.Start(); err != nil {
		log.Errorf("Error starting raft server: %s", err.Error())
	}

	if leader != "" {
		// Join to leader if specified.

		log.V(1).Infof("Attempting to join leader at %s", leader)

		// if !s.raftServer.IsLogEmpty() {
		// 	log.Error("Cannot join with an existing log")
		// 	return errors.New("Cannot join with an existing log")
		// }
		if err := s.Join(leader); err != nil {
			log.Errorf("Failed to join leader: %s", err.Error())
			return err
		}

	} else if s.raftServer.IsLogEmpty() {
		// Initialize the server by joining itself.

		log.V(1).Info("Initializing new cluster")

		_, err := s.raftServer.Do(&raft.DefaultJoinCommand{
			Name:             s.raftServer.Name(),
			ConnectionString: s.connectionString(),
		})
		if err != nil {
			log.Errorf("Failed to join to self: %s", err.Error())
		}

	} else {
		log.Info("Recovered from log")
	}

	log.V(1).Info("Initializing HTTP server")

	// Initialize and start HTTP server.
	s.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.router,
	}

	s.router.HandleFunc("/raft", s.serveRaftInfo).Methods("GET")
	s.router.HandleFunc("/get", s.getHandler).Methods("GET")
	s.router.HandleFunc("/set", s.setHandler).Methods("GET")
	s.router.HandleFunc("/delete", s.deleteHandler).Methods("GET")
	s.router.HandleFunc("/join", s.joinHandler).Methods("POST")

	log.V(1).Infof("Listening at %s", s.connectionString())

	return s.httpServer.ListenAndServe()
}

// ensurePrettyPrint returns a JSON representation of the object o. If
// the HTTP request requested pretty-printing, it ensures that happens.
func ensurePrettyPrint(req *http.Request, o map[string]interface{}) []byte {
	var b []byte
	b, _ = json.MarshalIndent(o, "", "    ")
	return b
}

// connectionString returns the string used to connect to this server.
func (s *Server) connectionString() string {
	return fmt.Sprintf("http://%s:%d", s.host, s.port)
}

// serveRaftInfo returns information about the underlying Raft server
func (s *Server) serveRaftInfo(w http.ResponseWriter, req *http.Request) {
	var peers []interface{}
	for _, v := range s.raftServer.Peers() {
		peers = append(peers, v)
	}

	info := make(map[string]interface{})
	info["name"] = s.raftServer.Name()
	info["state"] = s.raftServer.State()
	info["leader"] = s.raftServer.Leader()
	info["peers"] = peers

	_, err := w.Write(ensurePrettyPrint(req, info))
	if err != nil {
		log.Error("failed to serve raft info")
		http.Error(w, "failed to serve raft info", http.StatusInternalServerError)
		return
	}
}

// leaderRedirect returns a 307 Temporary Redirect, with the full path
// to the leader.
func (s *Server) leaderRedirect(w http.ResponseWriter, r *http.Request) {
	peers := s.raftServer.Peers()
	leader := peers[s.raftServer.Leader()]

	if leader == nil {
		// No leader available, give up.
		log.Error("attempted leader redirection, but no leader available")
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("no leader available"))
		return
	}

	var u string
	for _, p := range peers {
		if p.Name == leader.Name {
			u = p.ConnectionString
			break
		}
	}
	http.Redirect(w, r, u+r.URL.Path, http.StatusTemporaryRedirect)
}

// HandleFunc is a interface provided by raft http transporter
func (s *Server) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.router.HandleFunc(pattern, handler)
}

// Join joins to the leader of an existing cluster.
func (s *Server) Join(leader string) error {
	command := &raft.DefaultJoinCommand{
		Name:             s.raftServer.Name(),
		ConnectionString: s.connectionString(),
	}

	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(command); err != nil {
		return nil
	}

	resp, err := http.Post(fmt.Sprintf("http://%s/join", leader), "application/json", &b)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	// Look for redirect.
	if resp.StatusCode == http.StatusTemporaryRedirect {
		leader := resp.Header.Get("Location")
		if leader == "" {
			return errors.New("Redirect requested, but no location header supplied")
		}
		u, err := url.Parse(leader)
		if err != nil {
			return errors.New("Failed to parse redirect location")
		}
		log.Infof("Redirecting to leader at %s", u.Host)
		return s.Join(u.Host)
	}

	return nil
}

func (s *Server) joinHandler(w http.ResponseWriter, req *http.Request) {
	s.Lock()
	defer s.Unlock()

	if s.raftServer.State() != "leader" {
		s.leaderRedirect(w, req)
		return
	}

	command := &raft.DefaultJoinCommand{}

	if err := json.NewDecoder(req.Body).Decode(&command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if _, err := s.raftServer.Do(command); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (s *Server) getHandler(w http.ResponseWriter, req *http.Request) {
	log.V(5).Infof("getHandler for URL: %s", req.URL)
	var (
		err        error
		val        []byte
		badRequest bool
	)
	defer func() {
		if badRequest {
			return
		}
		q := QueryResponse{}
		if err != nil {
			q.Error = err.Error()
		} else {
			q.Value = val
		}
		b, err := json.Marshal(q)
		if err != nil {
			log.Errorf("Failed to marshal JSON data: %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
		} else {
			_, err = w.Write([]byte(b))
			if err != nil {
				log.Errorf("Error writting JSON data: %s", err.Error())
			}
		}
	}()

	// Get the query statement
	q := req.URL.Query()
	key := strings.TrimSpace(q.Get("key"))
	if key == "" {
		log.V(5).Infof("Bad HTTP request: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		badRequest = true
		return
	}

	val, err = s.db.Get([]byte(key))
	return
}

func (s *Server) checkSnapshot() {
	if s.inSnapshot {
		return
	}
	currentIndex := s.raftServer.CommitIndex()
	count := currentIndex - s.snapConf.lastIndex
	if uint64(count) > s.snapConf.snapshotAfter {
		s.Lock()
		s.inSnapshot = true
		// recheck the index, if it was done by other
		count := currentIndex - s.snapConf.lastIndex
		if uint64(count) > s.snapConf.snapshotAfter {
			log.V(2).Infof("Committed log entries snapshot threshold reached, starting snapshot")
			err := s.raftServer.TakeSnapshot()
			s.logSnapshot(err, currentIndex, count)
			s.snapConf.lastIndex = currentIndex
		}
		s.inSnapshot = false
		s.Unlock()
	}
}

// logSnapshot logs about the snapshot that was taken.
func (s *Server) logSnapshot(err error, currentIndex, count uint64) {
	info := fmt.Sprintf("%s: snapshot of %d events at index %d", s.connectionString(), count, currentIndex)
	if err != nil {
		log.Infof("%s attempted and failed: %v", info, err)
	} else {
		log.Infof("%s completed", info)
	}
}

func (s *Server) setHandler(w http.ResponseWriter, req *http.Request) {
	log.V(5).Infof("setHandler for URL: %s", req.URL)
	if s.raftServer.State() != "leader" {
		s.leaderRedirect(w, req)
		return
	}
	var (
		err        error
		badRequest bool
	)
	defer func() {
		if badRequest {
			return
		}
		q := QueryResponse{}
		if err != nil {
			q.Error = err.Error()
		}
		b, err := json.Marshal(q)
		if err != nil {
			log.Errorf("Failed to marshal JSON data: %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
		} else {
			_, err = w.Write([]byte(b))
			if err != nil {
				log.Errorf("Error writting JSON data: %s", err.Error())
			}
		}
	}()
	s.checkSnapshot()
	q := req.URL.Query()
	key := strings.TrimSpace(q.Get("key"))
	val := strings.TrimSpace(q.Get("val"))
	if key == "" || val == "" {
		log.V(5).Infof("Bad HTTP request: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		badRequest = true
		return
	}

	_, err = s.raftServer.Do(command.NewSetCommand([]byte(key), []byte(val)))
	return
}

func (s *Server) deleteHandler(w http.ResponseWriter, req *http.Request) {
	log.V(5).Infof("deleteHandler for URL: %s", req.URL)
	if s.raftServer.State() != "leader" {
		s.leaderRedirect(w, req)
		return
	}
	var (
		err        error
		badRequest bool
	)
	defer func() {
		if badRequest {
			return
		}
		q := QueryResponse{}
		if err != nil {
			q.Error = err.Error()
		}
		b, err := json.Marshal(q)
		if err != nil {
			log.Errorf("Failed to marshal JSON data: %s", err.Error())
			http.Error(w, err.Error(), http.StatusBadRequest) // Internal error actually
		} else {
			_, err = w.Write([]byte(b))
			if err != nil {
				log.Errorf("Error writting JSON data: %s", err.Error())
			}
		}
	}()
	s.checkSnapshot()
	// Get the query statement
	q := req.URL.Query()
	key := strings.TrimSpace(q.Get("key"))
	if key == "" {
		log.V(5).Infof("Bad HTTP request: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		badRequest = true
		return
	}

	_, err = s.raftServer.Do(command.NewDeleteCommand([]byte(key)))
	return
}
