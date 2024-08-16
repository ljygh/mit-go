# Mit-go
This is a repository for labs of mit 6.824. The original repository is: [6.824-golabs-2022
](https://github.com/keenJoe/6.824-golabs-2022)

## Map reduce
### Build
Init:
```
cd src
go mod init 6.824
```

Build mr apps:
```
cd src/main
go build -race -buildmode=plugin ../mrapps/wc.go
```

### Run
Run sequential map reduce: (-> mr-out-*)
```
go run -race mrsequential.go wc.so pg*.txt 
```

Run distributed map reduce:
```
rm mr-out*
go run -race mrcoordinator.go pg-*.txt
go run -race mrworker.go wc.so
```

### Implementation
main/mrcoordinator.go and main/mrworker.go;  
mr/coordinator.go, mr/worker.go, and mr/rpc.go.


## Raft
Raft algorithm is a consensus algorithm used to ensure distributed servers have the same command logs. Based on the same log, servers apply commands to their state machines in the same order, so that all servers will reach in the same state. The algorithm is introduced in this paper: [1] Ongaro, D. , &  Ousterhout, J. K. . (2014). In search of an understandable consensus algorithm. draft of october.

### Algorithm
1. Leader election: Followers will timeout and become candidate if they don't achieve heartbeat from leader after some time. If the candidate can get votes from a majority of servers, it will become leader. It sends heartbeats frequently to remain the state of leader.
2. Log replication: Leader receices command requests from clients. It adds it to its own log. It uses rpc to append new commands to other servers. If a majority of servers have the command, they commit the command and apply it to state machines.
3. The details of the algorithm is shown in Figure 2 of the paper.

### Implementation and test
The algorithm is implemented with go. Servers are run in a simulated network with presence of failures such as server crashes, network partitions, and message loss. The way to test the code is shown below:
```
cd src/raft
go test -run 2A (for leader election)
go test -run 2B (for log replication)
```