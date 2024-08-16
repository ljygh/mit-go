package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Define states of a raft server
type State int

const (
	Follower State = iota
	Candidate
	Leader
)

// Define time interval for heart beat.
const heartbeatInterval float32 = 0.1

// Define Entry for logs
type Entry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 2A
	currentTerm int
	votedFor    int
	timeout     float32
	timer       float32
	state       State

	// 2B
	log         []Entry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	applyCh     chan ApplyMsg

	// Loggers
	tickerLogger      *log.Logger
	requestVoteLogger *log.Logger
	appendEntryLogger *log.Logger
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	rf.requestVoteLogger.Println()
	rf.requestVoteLogger.Println("Get vote request from server", args.CandidateID)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.requestVoteLogger.Println("Reject: Term is lower than current term.")
	} else if args.Term == rf.currentTerm && rf.votedFor != -1 {
		reply.Success = false
		rf.requestVoteLogger.Println("Reject: Same term but already voted.")
	} else if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
		reply.Success = false
		rf.requestVoteLogger.Println("Reject: This server holds more up-to-date log according to term.")
	} else if args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1 {
		reply.Success = false
		rf.requestVoteLogger.Println("Reject: This server holds more up-to-date log according to last index.")
	} else {
		reply.Success = true
		rf.votedFor = args.CandidateID
		rf.currentTerm = args.Term
		rf.timer = 0.0
		rf.timeout = newTimeout()
		rf.requestVoteLogger.Println("Vote for server", args.CandidateID)
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Rpc args for AppendEntry
type AppendEntryArgs struct {
	// 2A
	Term     int
	LeaderID int

	// 2B
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

// Rpc reply for AppendEntry
type AppendEntryReply struct {
	Term    int
	Success bool
}

// AppendEntry RPC handler
func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	rf.appendEntryLogger.Println()
	rf.appendEntryLogger.Println("Get append entry request from leader", args.LeaderID)
	reply.Term = rf.currentTerm

	// Old leader.
	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.appendEntryLogger.Println("Reject: Leader of past term.")
		rf.mu.Unlock()
		return
	}

	// Correct leader, update timer.
	if rf.currentTerm < args.Term {
		rf.timeout = newTimeout()
		rf.currentTerm = args.Term
	}
	rf.timer = 0.0
	if rf.state == Leader {
		rf.state = Follower
	}
	rf.appendEntryLogger.Println("Correct heartbeat, reset timer.")

	// Previous entry is not in the log.
	if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		rf.appendEntryLogger.Println("Reject: PrevLogIndex not found in log.")
		rf.mu.Unlock()
		return
	}

	// Previous entry is in the log, but terms don't match.
	if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		rf.appendEntryLogger.Println("Reject: Term not matched in the log.")
		rf.mu.Unlock()
		return
	}

	// Otherwise, append entries.
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	rf.appendEntryLogger.Println("Success: Append", len(args.Entries), " entries in the log.")
	rf.printLog(rf.appendEntryLogger)

	// Update commitIndex based on the leader.
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		rf.appendEntryLogger.Println("Update commitIndex to:", rf.commitIndex)
	}
	rf.mu.Unlock()
}

// Send a AppendEntry RPC to a server.
func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = (rf.state == Leader)
	term = rf.currentTerm
	if isLeader {
		index = len(rf.log)
		newEntry := Entry{}
		newEntry.Command = command
		newEntry.Term = term
		rf.log = append(rf.log, newEntry)
		rf.tickerLogger.Println("Get new client command:", command)
	}
	rf.mu.Unlock()

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker(tickerLogFile *os.File, requestVoteLogFile *os.File, appendEntryLogFile *os.File) {
	defer tickerLogFile.Close()
	defer requestVoteLogFile.Close()
	defer appendEntryLogFile.Close()
	rf.tickerLogger.Println("Ticker of server", rf.me, "started")
	rf.tickerLogger.Println("timeout:", rf.timeout)
	rf.tickerLogger.Println()

	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(time.Millisecond)
		rf.mu.Lock()
		rf.tickerLogger.Println("State:", rf.state, ", Term:", rf.currentTerm, ", VotedFor:", rf.votedFor, ", timer:", rf.timer)
		rf.timer += 0.001
		if rf.state == Follower && rf.timer >= rf.timeout { // Follower timeout.
			rf.tickerLogger.Println("Raft server", rf.me, "follower timeout")
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.state = Candidate
			rf.timer = 0.0
			rf.timeout = newTimeout()

			// Request votes.
			rf.tickerLogger.Println("Request votes to all servers")
			inTime := time.Now()
			var wg sync.WaitGroup
			var votes int = 1
			var votesLock sync.Mutex
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					wg.Add(1)
					go func(votes *int) {
						args := RequestVoteArgs{}
						args.Term = rf.currentTerm
						args.CandidateID = rf.me
						args.LastLogIndex = len(rf.log) - 1
						args.LastLogTerm = rf.log[len(rf.log)-1].Term
						reply := RequestVoteReply{}
						ok := rf.sendRequestVote(i, &args, &reply)
						if ok {
							if reply.Success {
								votesLock.Lock()
								*votes++
								votesLock.Unlock()
							}
						}
						wg.Done()
					}(&votes)
				}
			}

			done := make(chan bool)
			go func() {
				defer close(done)
				wg.Wait()
				done <- true
			}()

			voteSuccess := false
			select {
			case <-done:
				if votes > (len(rf.peers) / 2) {
					voteSuccess = true
				}
			case <-time.After(2 * time.Millisecond):
				if votes > (len(rf.peers) / 2) {
					voteSuccess = true
				}
			}
			outTime := time.Now()
			rf.tickerLogger.Println("Time cost of requesting votes:", outTime.Sub(inTime))

			// Make decision based on result of votes.
			rf.tickerLogger.Println("Get number of votes:", votes)
			rf.tickerLogger.Println("Number of servers:", len(rf.peers))
			if voteSuccess {
				rf.state = Leader
				rf.timer = 0.1
				rf.tickerLogger.Println("Become leader")

				// Update nextIndex and matchIndex
				nextIndex := len(rf.log)
				rf.tickerLogger.Println("Initial nextIndex:", nextIndex)
				rf.tickerLogger.Println("Initial matchIndex:", 0)
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = nextIndex
					rf.matchIndex[i] = 0
				}
			} else {
				rf.state = Follower
				rf.currentTerm--
				rf.tickerLogger.Println("Lose election, convert back to follower")
			}
		} else if rf.state == Leader && rf.timer >= heartbeatInterval { // Leader
			rf.timer = 0.0
			rf.tickerLogger.Println("Send heartbeat to servers")
			inTime := time.Now()
			var term int = 0
			var termLock sync.Mutex
			var rfLock sync.Mutex
			var wg sync.WaitGroup
			for i := 0; i < len(rf.peers); i++ {
				if i != rf.me {
					wg.Add(1)
					go func(term *int) {
						args := AppendEntryArgs{}
						args.Term = rf.currentTerm
						args.LeaderID = rf.me
						args.PrevLogIndex = rf.nextIndex[i] - 1
						rf.tickerLogger.Println("Server, nextIndex, PrevLogIndex:", i, rf.nextIndex[i], args.PrevLogIndex)
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
						args.Entries = rf.log[rf.nextIndex[i]:]
						rf.tickerLogger.Println(i, "length of entries:", len(args.Entries))
						args.LeaderCommit = rf.commitIndex

						reply := AppendEntryReply{}
						ok := rf.sendAppendEntry(i, &args, &reply)

						if ok {
							if reply.Success { // Success: heartbeat or append entries.
								if len(args.Entries) > 0 {
									rfLock.Lock()
									rf.nextIndex[i] = len(rf.log)
									rf.matchIndex[i] = rf.nextIndex[i] - 1
									rf.tickerLogger.Println(i, "Success, update nextIndex to", rf.nextIndex[i])
									rfLock.Unlock()
								} else {
									rf.tickerLogger.Println(i, "Success, no entries, nextIndex:", rf.nextIndex[i])
								}
							} else if reply.Term > rf.currentTerm { // Obsolete leader.
								termLock.Lock()
								if reply.Term > *term {
									*term = reply.Term
								}
								termLock.Unlock()
								rf.tickerLogger.Println(i, "Obsolete leader")
							} else { // Fail to append entries.
								rfLock.Lock()
								rf.nextIndex[i]--
								rfLock.Unlock()
								rf.tickerLogger.Println(i, "Fail, decrease nextIndex to", rf.nextIndex[i])
							}
						}
						wg.Done()
					}(&term)
				} else {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = len(rf.log) - 1
				}
			}

			done := make(chan bool)
			go func() {
				defer close(done)
				wg.Wait()
				done <- true
			}()

			isObsolete := false
			select {
			case <-done:
				if term > 0 {
					isObsolete = true
				}
			case <-time.After(2 * time.Millisecond):
				if term > 0 {
					isObsolete = true
				}
			}

			outTime := time.Now()
			rf.tickerLogger.Println("Finish heartbeat, cost time:", outTime.Sub(inTime))
			if isObsolete {
				rf.tickerLogger.Println("Obsolete leader, convert back to follower, term:", term)
				rf.currentTerm = term
				rf.votedFor = -1
				rf.state = Follower
				rf.timeout = newTimeout()
			} else { // Update commitedIndex.
				index := rf.commitIndex + 1
				count := 0
				for i := 0; i < len(rf.peers); i++ {
					if rf.matchIndex[i] >= index {
						count++
					}
				}

				if count > len(rf.peers)/2 {
					rf.commitIndex = index
					rf.tickerLogger.Println("Leader: set commitIndex to:", rf.commitIndex)
				}
			}
			rf.printLog(rf.tickerLogger)
		}

		// Apply command no matter it is leader or follower.
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			applyMsg := ApplyMsg{}
			applyMsg.CommandValid = true
			applyMsg.Command = rf.log[rf.lastApplied].Command
			applyMsg.CommandIndex = rf.lastApplied
			rf.applyCh <- applyMsg
			rf.tickerLogger.Println("Apply command with index:", rf.lastApplied)
			rf.printLog(rf.tickerLogger)
		}
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Set ticker logger
	tickerLogFile, err := os.OpenFile("./log/ticker_log_"+strconv.Itoa(rf.me)+".txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	rf.tickerLogger = log.New(tickerLogFile, "Raft server "+strconv.Itoa(rf.me), log.LstdFlags|log.Lmicroseconds)

	// Set request vote logger
	requestVoteLogFile, err := os.OpenFile("./log/requestVote_log_"+strconv.Itoa(rf.me)+".txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	rf.requestVoteLogger = log.New(requestVoteLogFile, "Raft server "+strconv.Itoa(rf.me), log.LstdFlags|log.Lmicroseconds)

	// Set append entry logger
	appendEntryLogFile, err := os.OpenFile("./log/appendEntry_log_"+strconv.Itoa(rf.me)+".txt", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	rf.appendEntryLogger = log.New(appendEntryLogFile, "Raft server "+strconv.Itoa(rf.me), log.LstdFlags|log.Lmicroseconds)

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.timer = 0.0
	rf.timeout = newTimeout()

	// 2B
	rf.commitIndex = 0
	rf.lastApplied = 0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}
	dummyEntry := Entry{}
	dummyEntry.Term = 0
	rf.log = append(rf.log, dummyEntry)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker(tickerLogFile, requestVoteLogFile, appendEntryLogFile)

	return rf
}

// Generate a random timeout.
func newTimeout() float32 {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	const min float32 = 0.4
	const max float32 = 0.6
	return min + random.Float32()*(max-min)
}

// Print info of log.
func (rf *Raft) printLog(logger *log.Logger) {
	logger.Print("Logs (index, term, command): ")
	for i := 1; i < len(rf.log); i++ {
		logger.Print("(", i, ", ", rf.log[i].Term, ", ", rf.log[i].Command, ")")
	}
	logger.Println("Commit index:", rf.commitIndex)
	logger.Println("Applied index:", rf.lastApplied)
}
