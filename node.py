# node.py

import grpc
from concurrent import futures
import threading
import time
import random
import sys

import raft_pb2
import raft_pb2_grpc

# State constants
FOLLOWER = 0
CANDIDATE = 1
LEADER = 2

class RaftNode(raft_pb2_grpc.RaftNodeServicer):
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers  # Dictionary of peer node IDs and their addresses
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []  # List of LogEntry
        self.commit_index = -1
        self.last_applied = -1
        self.next_index = {}  # For each server, index of the next log entry to send
        self.match_index = {}  # For each server, index of highest log entry known to be replicated
        self.leader_id = None  # Current known leader

        self.lock = threading.Lock()

        # Election timeout between 150ms and 300ms
        self.reset_election_timeout()

        # Start background threads
        threading.Thread(target=self.election_timer, daemon=True).start()
        threading.Thread(target=self.apply_entries, daemon=True).start()

    def reset_election_timeout(self):
        self.election_timeout = random.uniform(0.15, 0.3)
        self.last_heartbeat_time = time.time()

    # RPC implementations
    def RequestVote(self, request, context):
        with self.lock:
            # Print send/receive messages
            print(f"Process {self.node_id} receives RPC RequestVote from Process {request.candidateId}")

            # Update term if necessary
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = FOLLOWER
                self.voted_for = None

            vote_granted = False

            if request.term == self.current_term:
                if (self.voted_for is None or self.voted_for == request.candidateId) and \
                   self.is_log_up_to_date(request.lastLogIndex, request.lastLogTerm):
                    self.voted_for = request.candidateId
                    vote_granted = True
                    # Reset election timer
                    self.reset_election_timeout()

            # Prepare response
            response = raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=vote_granted)

            print(f"Process {self.node_id} sends RPC RequestVoteResponse to Process {request.candidateId}")

            return response

    def AppendEntries(self, request, context):
        with self.lock:
            # Print send/receive messages
            print(f"Process {self.node_id} receives RPC AppendEntries from Process {request.leaderId}")

            success = False

            if request.term >= self.current_term:
                self.current_term = request.term
                self.state = FOLLOWER
                self.voted_for = None
                self.leader_id = request.leaderId
                # Reset election timer
                self.reset_election_timeout()
            else:
                response = raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)
                print(f"Process {self.node_id} sends RPC AppendEntriesResponse to Process {request.leaderId}")
                return response

            # Check log consistency
            if request.prevLogIndex == -1 or \
               (request.prevLogIndex < len(self.log) and \
                self.log[request.prevLogIndex].term == request.prevLogTerm):
                success = True
                # Delete any conflicting entries
                index = request.prevLogIndex + 1
                self.log = self.log[:index]
                # Append any new entries not already in the log
                for entry in request.entries:
                    log_entry = raft_pb2.LogEntry(term=entry.term, index=entry.index, operation=entry.operation)
                    self.log.append(log_entry)
                # Update commit index
                if request.leaderCommit > self.commit_index:
                    self.commit_index = min(request.leaderCommit, len(self.log) - 1)
            else:
                success = False

            response = raft_pb2.AppendEntriesResponse(term=self.current_term, success=success)

            print(f"Process {self.node_id} sends RPC AppendEntriesResponse to Process {request.leaderId}")

            return response

    def ClientRequest(self, request, context):
        with self.lock:
            print(f"Process {self.node_id} received client request: {request.operation}")

            if self.state != LEADER:
                # Forward to leader
                leader_address = self.peers.get(self.leader_id)
                if leader_address:
                    print(f"Process {self.node_id} is not leader, forwarding to leader {self.leader_id}")
                    with grpc.insecure_channel(leader_address) as channel:
                        stub = raft_pb2_grpc.RaftNodeStub(channel)
                        return stub.ClientRequest(request)
                else:
                    # Leader unknown
                    print(f"Process {self.node_id} does not know the current leader.")
                    return raft_pb2.ClientResponseMessage(result="Leader unknown", success=False)
            else:
                # Append to log
                entry = raft_pb2.LogEntry(term=self.current_term, index=len(self.log), operation=request.operation)
                self.log.append(entry)
                # Update own state
                self.match_index[self.node_id] = entry.index
                self.next_index[self.node_id] = entry.index + 1
                # Start replication
                threading.Thread(target=self.replicate_log, daemon=True).start()
                # Wait until the entry is committed
                while self.commit_index < entry.index:
                    time.sleep(0.01)
                # Return result to client
                return raft_pb2.ClientResponseMessage(result=f"Operation '{request.operation}' executed.", success=True)

    # Helper methods
    def election_timer(self):
        while True:
            time.sleep(0.01)
            with self.lock:
                if self.state != LEADER and (time.time() - self.last_heartbeat_time) >= self.election_timeout:
                    # Start election
                    self.state = CANDIDATE
                    self.current_term += 1
                    self.voted_for = self.node_id
                    self.leader_id = None
                    votes_received = set([self.node_id])  # Vote for self

                    print(f"Process {self.node_id} starts election for term {self.current_term}")

                    # Send RequestVote RPCs to all other nodes
                    for peer_id, address in self.peers.items():
                        if peer_id == self.node_id:
                            continue
                        threading.Thread(target=self.send_request_vote, args=(peer_id, address, votes_received), daemon=True).start()

                    # Wait for election result
                    election_end_time = time.time() + self.election_timeout
                    while time.time() < election_end_time and self.state == CANDIDATE:
                        with self.lock:
                            if len(votes_received) > len(self.peers) // 2:
                                # Become leader
                                self.state = LEADER
                                self.leader_id = self.node_id
                                for peer_id in self.peers.keys():
                                    self.next_index[peer_id] = len(self.log)
                                    self.match_index[peer_id] = -1
                                print(f"Process {self.node_id} becomes leader for term {self.current_term}")
                                threading.Thread(target=self.heartbeat_timer, daemon=True).start()
                                break
                        time.sleep(0.05)
                    else:
                        # Election failed, start over
                        self.state = FOLLOWER
                        self.voted_for = None
                        self.reset_election_timeout()

    def send_request_vote(self, peer_id, address, votes_received):
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            last_log_index = len(self.log) - 1
            last_log_term = self.log[last_log_index].term if last_log_index >= 0 else -1
            request = raft_pb2.RequestVoteRequest(
                term=self.current_term,
                candidateId=self.node_id,
                lastLogIndex=last_log_index,
                lastLogTerm=last_log_term
            )
            print(f"Process {self.node_id} sends RPC RequestVote to Process {peer_id}")
            try:
                response = stub.RequestVote(request)
                print(f"Process {self.node_id} receives RPC RequestVoteResponse from Process {peer_id}")
                with self.lock:
                    if response.term > self.current_term:
                        self.current_term = response.term
                        self.state = FOLLOWER
                        self.voted_for = None
                        self.leader_id = None
                        return
                    if self.state == CANDIDATE and response.voteGranted:
                        votes_received.add(peer_id)
            except Exception as e:
                print(f"Process {self.node_id} failed to send RequestVote to Process {peer_id}: {e}")

    def heartbeat_timer(self):
        while self.state == LEADER:
            time.sleep(0.1)  # Heartbeat interval of 100ms
            self.send_heartbeats()

    def send_heartbeats(self):
        for peer_id, address in self.peers.items():
            if peer_id == self.node_id:
                continue
            threading.Thread(target=self.send_append_entries, args=(peer_id, address), daemon=True).start()

    def send_append_entries(self, peer_id, address):
        with grpc.insecure_channel(address) as channel:
            stub = raft_pb2_grpc.RaftNodeStub(channel)
            prev_log_index = self.next_index.get(peer_id, 0) - 1
            prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 and prev_log_index < len(self.log) else -1
            entries = []
            if len(self.log) > prev_log_index + 1:
                entries = self.log[prev_log_index + 1:]

            request = raft_pb2.AppendEntriesRequest(
                term=self.current_term,
                leaderId=self.node_id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                entries=entries,
                leaderCommit=self.commit_index
            )
            print(f"Process {self.node_id} sends RPC AppendEntries to Process {peer_id}")
            try:
                response = stub.AppendEntries(request)
                print(f"Process {self.node_id} receives RPC AppendEntriesResponse from Process {peer_id}")
                with self.lock:
                    if response.term > self.current_term:
                        self.current_term = response.term
                        self.state = FOLLOWER
                        self.voted_for = None
                        self.leader_id = None
                        return
                    if response.success:
                        # Update next_index and match_index
                        self.next_index[peer_id] = prev_log_index + len(entries) + 1
                        self.match_index[peer_id] = self.next_index[peer_id] - 1
                        # Advance commit index if possible
                        self.update_commit_index()
                    else:
                        # Decrement next_index and retry
                        self.next_index[peer_id] = max(0, self.next_index.get(peer_id, 0) - 1)
            except Exception as e:
                print(f"Process {self.node_id} failed to send AppendEntries to Process {peer_id}: {e}")

    def update_commit_index(self):
        for N in range(len(self.log) - 1, self.commit_index, -1):
            count = 1  # Include self
            for peer_id in self.peers.keys():
                if peer_id != self.node_id and self.match_index.get(peer_id, -1) >= N:
                    count += 1
            if count > len(self.peers) // 2 and self.log[N].term == self.current_term:
                self.commit_index = N
                break

    def apply_entries(self):
        while True:
            time.sleep(0.01)
            with self.lock:
                while self.last_applied < self.commit_index:
                    self.last_applied += 1
                    entry = self.log[self.last_applied]
                    # Execute the operation (for simplicity, just print it)
                    print(f"Process {self.node_id} applies log at index {self.last_applied}: {entry.operation}")

    def is_log_up_to_date(self, lastLogIndex, lastLogTerm):
        if len(self.log) == 0:
            return True
        last_term = self.log[-1].term
        if lastLogTerm != last_term:
            return lastLogTerm > last_term
        else:
            return lastLogIndex >= len(self.log) - 1

    def replicate_log(self):
        # This function ensures log entries are replicated to followers
        self.send_heartbeats()

def serve(node_id, port, peers):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_node = RaftNode(node_id, port, peers)
    raft_pb2_grpc.add_RaftNodeServicer_to_server(raft_node, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f"Process {node_id} started and listening on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: python node.py [node_id] [port] [peer_id:address,...]")
        sys.exit(1)

    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    peers_arg = sys.argv[3] if len(sys.argv) > 3 else ""

    # Parse peers
    peers = {}
    for peer in peers_arg.split(','):
        if peer.strip() == "":
            continue
        peer_id_str, address = peer.strip().split(':', 1)  # Split only on the first colon
        peer_id = int(peer_id_str)
        peers[peer_id] = address

    # Remove self from peers if present
    if node_id in peers:
        del peers[node_id]

    serve(node_id, port, peers)
