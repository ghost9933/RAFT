import grpc
from concurrent import futures
import threading
import time
import raft_pb2
import raft_pb2_grpc
import logging
import os
import sys
import random

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, peers):
        self.id = node_id
        self.peers = peers  # List of (host, port, id)
        self.current_term = 0
        self.voted_for = None
        self.state = 'follower'
        self.commitIndex = 0
        self.lastApplied = 0
        self.log = []
        self.nextIndex = {}
        self.matchIndex = {}
        self.leader_id = None
        self.lock = threading.Lock()
        self.election_timer = None
        self.heartbeat_timer = None
        self.election_timeout = random.uniform(0.15, 0.3)  # Increased timeout
        self.heartbeat_timeout = 0.1 # Increased heartbeat interval
        self.reset_election_timer()

    # RPC Methods
    def RequestVote(self, request, context):
        with self.lock:
            term = request.term
            candidateId = request.candidateId
            response = raft_pb2.RequestVoteResponse()
            response.term = self.current_term
            response.voteGranted = False

            if term < self.current_term:
                logging.info(f"Process {self.id} rejects vote for Process {candidateId} (term {term} < current term {self.current_term})")
                return response

            if term > self.current_term:
                # Update term and reset state
                self.current_term = term
                self.voted_for = None
                self.state = 'follower'
                self.reset_election_timer()
                logging.info(f"Process {self.id} updates term to {self.current_term} and becomes follower")

            # Grant vote if not voted yet in this term or already voted for this candidate
            if self.voted_for is None or self.voted_for == candidateId:
                self.voted_for = candidateId
                response.voteGranted = True
                logging.info(f"Process {self.id} grants vote to Process {candidateId} for term {self.current_term}")
                self.reset_election_timer()
            else:
                # Vote has already been cast for another candidate in this term
                logging.info(f"Process {self.id} denies vote to Process {candidateId} for term {self.current_term} (already voted for Process {self.voted_for})")

            return response

    def AppendEntries(self, request, context):
        with self.lock:
            term = request.term
            leaderId = request.leaderId
            response = raft_pb2.AppendEntriesResponse()
            response.term = self.current_term
            response.success = False

            if term < self.current_term:
                return response

            self.leader_id = leaderId
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
            self.state = 'follower'
            self.reset_election_timer()

            # Log consistency check
            if request.prevLogIndex > 0:
                if len(self.log) < request.prevLogIndex or self.log[request.prevLogIndex - 1].term != request.prevLogTerm:
                    return response

            # Append new entries
            index = request.prevLogIndex
            for entry in request.entries:
                index += 1
                if len(self.log) >= index:
                    if self.log[index - 1].term != entry.term:
                        self.log = self.log[:index - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)

            # Update commitIndex
            if request.leaderCommit > self.commitIndex:
                self.commitIndex = min(request.leaderCommit, len(self.log))
                self.apply_logs()

            response.success = True
            return response

    def ClientRequest(self, request, context):
        with self.lock:
            if self.state != 'leader':
                # Forward to the leader
                leader_stub = self.get_leader_stub()
                if leader_stub:
                    logging.info(f"Process {self.id} forwards client request to Leader {self.leader_id}")
                    return leader_stub.ClientRequest(request)
                else:
                    return raft_pb2.ClientResponseMessage(result="No leader available")
            else:
                # Leader processes the client request
                operation = request.operation
                self.append_log(operation)
                # Start replicating logs
                self.replicate_log()
                # Wait for logs to be committed (simplified)
                while self.commitIndex < len(self.log):
                    time.sleep(0.1)
                return raft_pb2.ClientResponseMessage(result=f"Operation '{operation}' executed")

    # Helper Methods
    def reset_election_timer(self):
        if self.election_timer and self.election_timer.is_alive():
            self.election_timer.cancel()
        self.election_timeout = random.uniform(1.0, 2.0)  # Increased timeout
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()

    def start_election(self):
        with self.lock:
            self.state = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            logging.info(f"Process {self.id} becomes Candidate for term {self.current_term}")
            votes = 1  # Vote for self
            responses = []
            for peer in self.peers:
                threading.Thread(target=self.send_request_vote, args=(peer, responses)).start()
            time.sleep(1)  # Wait for votes
            if self.state != 'candidate':
                return
            votes += len([r for r in responses if r.voteGranted])
            if votes > (len(self.peers) + 1) // 2:
                self.state = 'leader'
                logging.info(f"Process {self.id} becomes Leader for term {self.current_term}")
                self.nextIndex = {peer[2]: len(self.log) + 1 for peer in self.peers}
                self.matchIndex = {peer[2]: 0 for peer in self.peers}
                self.start_heartbeat()
            else:
                self.reset_election_timer()

    def send_request_vote(self, peer, responses):
        channel = grpc.insecure_channel(f"{peer[0]}:{peer[1]}")
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        request = raft_pb2.RequestVoteRequest(term=self.current_term, candidateId=self.id)
        logging.info(f"Process {self.id} sends RPC RequestVote to Process {peer[2]}")
        try:
            response = stub.RequestVote(request)
            responses.append(response)
        except Exception as e:
            logging.error(f"Process {self.id} failed to send RequestVote to Process {peer[2]}: {e}")

    def start_heartbeat(self):
        if self.heartbeat_timer and self.heartbeat_timer.is_alive():
            self.heartbeat_timer.cancel()
        self.heartbeat_timer = threading.Timer(self.heartbeat_timeout, self.send_heartbeats)
        self.heartbeat_timer.start()

    def send_heartbeats(self):
        if self.state != 'leader':
            return
        for peer in self.peers:
            threading.Thread(target=self.send_append_entries, args=(peer,)).start()
        self.start_heartbeat()

    def send_append_entries(self, peer):
        channel = grpc.insecure_channel(f"{peer[0]}:{peer[1]}")
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        # Prepare AppendEntriesRequest with log entries
        next_idx = self.nextIndex.get(peer[2], len(self.log) + 1)
        prevLogIndex = next_idx - 1
        prevLogTerm = self.log[prevLogIndex - 1].term if prevLogIndex > 0 and prevLogIndex <= len(self.log) else 0
        entries = self.log
        request = raft_pb2.AppendEntriesRequest(
            term=self.current_term,
            leaderId=self.id,
            prevLogIndex=prevLogIndex,
            prevLogTerm=prevLogTerm,
            entries=entries,
            leaderCommit=self.commitIndex
        )
        logging.info(f"Process {self.id} sends RPC AppendEntries to Process {peer[2]}")
        try:
            response = stub.AppendEntries(request)
            if response.success:
                self.nextIndex[peer[2]] = prevLogIndex + len(entries) + 1
                self.matchIndex[peer[2]] = self.nextIndex[peer[2]] - 1
                # Check if any entries can be committed
                for i in range(self.commitIndex + 1, len(self.log) + 1):
                    count = 1  # Leader has the entry
                    for p in self.peers:
                        if self.matchIndex.get(p[2], 0) >= i:
                            count += 1
                    if count > (len(self.peers) + 1) // 2:
                        self.commitIndex = i
                        self.apply_logs()
            else:
                self.nextIndex[peer[2]] = max(1, self.nextIndex[peer[2]] - 1)
        except Exception as e:
            logging.error(f"Process {self.id} failed to send AppendEntries to Process {peer[2]}: {e}")

    def append_log(self, operation):
        entry = raft_pb2.LogEntry(term=self.current_term, index=len(self.log) + 1, operation=operation)
        self.log.append(entry)
        logging.info(f"Process {self.id} appends operation '{operation}' to log at index {entry.index}")

    def replicate_log(self):
        for peer in self.peers:
            threading.Thread(target=self.send_append_entries, args=(peer,)).start()

    def apply_logs(self):
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            entry = self.log[self.lastApplied - 1]
            # Execute the operation (placeholder)
            logging.info(f"Process {self.id} executes operation '{entry.operation}' from log index {entry.index}")

    def get_leader_stub(self):
        if self.leader_id:
            for peer in self.peers:
                if peer[2] == self.leader_id:
                    channel = grpc.insecure_channel(f"{peer[0]}:{peer[1]}")
                    stub = raft_pb2_grpc.RaftServiceStub(channel)
                    return stub
        return None

    # Start the gRPC server
    def serve(self, host, port):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        server.add_insecure_port(f"0.0.0.0:{port}")  # Listen on all interfaces
        server.start()
        logging.info(f"Process {self.id} started at {host}:{port}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            server.stop(0)

    def GetState(self, request, context):
        with self.lock:
            response = raft_pb2.StateResponse(
                id=self.id,
                term=self.current_term,
                state=self.state,
                commitIndex=self.commitIndex,
                lastApplied=self.lastApplied,
                log=self.log
            )
            return response

    # Main Execution
if __name__ == '__main__':
    # Read environment variables or command-line arguments
    node_id = int(os.environ.get('ID'))
    host = os.environ.get('HOST')
    port = os.environ.get('PORT')
    peers = []
    for peer in os.environ.get('PEERS').split(','):
        p_id, p_host, p_port = peer.split(':')
        if int(p_id) != node_id:
            peers.append((p_host, p_port, int(p_id)))
    node = RaftNode(node_id, peers)
    node.serve(host, port)
