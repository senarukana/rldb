#rldb
rldb is a strong consistent distributed k/v storage system based on raft.
It is just a demonstration of raft protocol usage.

# Getting started
1. Make sure you have leveldb installed.
	
	git clone https://github.com/google/leveldb
	cd leveldb
	make
	cp libleveldb.* /usr/local/lib & cp -r include/leveldb /usr/local/include/
2. install rldb

	go get github.com/senarukana/rldb
	go install github.com/senarukana/rldb

#Usage
1. Launch multiple rldb instance

	./rldb --logtostderr=1 --v=5 --dbfile='rldb1' -p=4001
	./rldb --join='localhost:4001' --logtostderr=1 --v=5 --dbfile='rldb2' -p=4002
	./rldb --join='localhost:4001' --logtostderr=1 --v=5 --dbfile='rldb3' -p=4003

2. fire http request
At current, it only support SET, GET, DELETE. Response is of json format: {Value:"", Error:""}

	 curl http://localhost:4001/set\?key\='t1'\&val\='t1'
 	 curl http://localhost:4001/set\?key\='t2'\&val\='t2'
 	 curl http://localhost:4001/get\?key\='t1'
 	 curl http://localhost:4001/delete\?key\='t1'

You can close any rldb instances at any given time to test the result. Enjoy!