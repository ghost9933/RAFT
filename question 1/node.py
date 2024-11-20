import grpc
from concurrent import futures
import threading
import time
import random
import raft_pb2
import raft_pb2_grpc
import sys
import os

# Import logging for output
import logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, peers):
        self.id = node_id
        self.peers = peers  # List of (host, port) tuples
        self.state = 'follower'
        self.current_term = 0
        self.voted_for = None
        self.lock = threading.Lock()
        self.heartbeat_timeout = 0.1  # 100 ms
        self.election_timeout = random.uniform(0.15, 0.3)  # 150-300 ms
        self.last_heartbeat = time.time()
        self.election_timer = threading.Timer(self.election_timeout, self.start_election)
        self.election_timer.start()
        self.servers = {}  # gRPC channels to other servers

    # RPC Handlers
    def RequestVote(self, request, context):
        with self.lock:
            logging.info(f"Process {self.id} receives RPC RequestVote from Process {request.candidateId}")
            term = request.term
            candidateId = request.candidateId
            response = raft_pb2.RequestVoteResponse(term=self.current_term, voteGranted=False)
            if term > self.current_term:
                self.current_term = term
                self.voted_for = None
                self.state = 'follower'
            if self.voted_for in [None, candidateId] and term == self.current_term:
                self.voted_for = candidateId
                response.voteGranted = True
            return response

    def AppendEntries(self, request, context):
        with self.lock:
            logging.info(f"Process {self.id} receives RPC AppendEntries from Process {request.leaderId}")
            term = request.term
            leaderId = request.leaderId
            response = raft_pb2.AppendEntriesResponse(term=self.current_term, success=False)
            if term >= self.current_term:
                self.current_term = term
                self.state = 'follower'
                self.voted_for = None
                self.reset_election_timer()
                response.success = True
            return response

    # Helper Methods
    def reset_election_timer(self):
        if self.election_timer.is_alive():
            self.election_timer.cancel()
        self.election_timeout = random.uniform(0.15, 0.3)
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
            if votes + len([r for r in responses if r.voteGranted]) > len(self.peers) / 2:
                self.state = 'leader'
                logging.info(f"Process {self.id} becomes Leader for term {self.current_term}")
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
        request = raft_pb2.AppendEntriesRequest(term=self.current_term, leaderId=self.id)
        logging.info(f"Process {self.id} sends RPC AppendEntries to Process {peer[2]}")
        try:
            response = stub.AppendEntries(request)
        except Exception as e:
            logging.error(f"Process {self.id} failed to send AppendEntries to Process {peer[2]}: {e}")

    # Start the gRPC server
    def serve(self, host, port):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        server.add_insecure_port(f"{host}:{port}")
        server.start()
        logging.info(f"Process {self.id} started at {host}:{port}")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            server.stop(0)

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
