import unittest
import grpc
import raft_pb2
import raft_pb2_grpc
import time
import subprocess
import re

class TestRaftAlgorithm(unittest.TestCase):

    def setUp(self):
        # List of node addresses
        self.nodes = [
            ('localhost', 50051),
            ('localhost', 50052),
            ('localhost', 50053),
            ('localhost', 50054),
            ('localhost', 50055),
        ]

    def get_stub(self, host, port):
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        return stub

    def get_container_name(self, node_id):
        # Get the list of all containers (running and stopped)
        result = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True, text=True)
        container_names = result.stdout.strip().split('\n')
        # Pattern to match the container name
        pattern = re.compile(f".*node{node_id}_1$")
        for name in container_names:
            if pattern.match(name):
                return name
        return None

    def test_leader_election(self):
        """Test that a leader is elected in the cluster."""
        leader_id = None
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state == 'leader':
                    leader_id = f"{host}:{port}"
                    print(f"Leader is at {leader_id}")
                    break
            except Exception as e:
                continue
        self.assertIsNotNone(leader_id, "No leader found in the cluster")

    def test_log_replication(self):
        """Test that logs are replicated across all nodes."""
        # Find the leader
        leader_stub = None
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state == 'leader':
                    leader_stub = stub
                    break
            except Exception as e:
                continue
        self.assertIsNotNone(leader_stub, "No leader found in the cluster")

        # Send a command to the leader
        operation = "set x=10"
        request = raft_pb2.ClientRequestMessage(operation=operation)
        response = leader_stub.ClientRequest(request)
        self.assertIn("executed", response.result)

        # Wait for replication
        time.sleep(2)

        # Verify that all nodes have the entry in their logs
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                log_operations = [entry.operation for entry in state.log]
                self.assertIn(operation, log_operations, f"Operation not found in log of node {host}:{port}")
            except Exception as e:
                self.fail(f"Failed to get state from node {host}:{port}: {e}")

    def test_node_failure_and_recovery(self):
        """Test cluster behavior when a leader node fails and recovers."""
        # Find the leader
        leader_host = None
        leader_port = None
        leader_id = None
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state == 'leader':
                    leader_host = host
                    leader_port = port
                    leader_id = state.id
                    break
            except Exception as e:
                continue
        self.assertIsNotNone(leader_host, "No leader found in the cluster")
        self.assertIsNotNone(leader_id, "Leader ID not found")

        # Get the container name
        container_name = self.get_container_name(leader_id)
        self.assertIsNotNone(container_name, f"Container for leader node {leader_id} not found")
        print(f"Stopping leader node {container_name}")

        # Stop the leader node
        subprocess.run(["docker", "stop", container_name])

        # Wait for re-election
        time.sleep(10)

        # Check that a new leader is elected
        new_leader_found = False
        for host, port in self.nodes:
            if host == leader_host and port == leader_port:
                continue  # Skip the stopped node
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state == 'leader':
                    new_leader_found = True
                    print(f"New leader is at {host}:{port}")
                    break
            except Exception as e:
                continue
        self.assertTrue(new_leader_found, "No new leader found after stopping the original leader")

        # Send a command to the new leader
        operation = "set x=20"
        request = raft_pb2.ClientRequestMessage(operation=operation)
        response = stub.ClientRequest(request)
        self.assertIn("executed", response.result)

        # Start the original leader node
        print(f"Starting leader node {container_name}")
        subprocess.run(["docker", "start", container_name])

        # Wait for the node to catch up
        time.sleep(10)

        # Check that the restarted node has the latest logs
        stub = self.get_stub(leader_host, leader_port)
        state = stub.GetState(raft_pb2.Empty())
        log_operations = [entry.operation for entry in state.log]
        self.assertIn(operation, log_operations, f"Operation not found in log of restarted node {leader_host}:{leader_port}")

    def test_client_interaction(self):
        """Test that the client can interact with the cluster."""
        # Find the leader
        leader_stub = None
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state == 'leader':
                    leader_stub = stub
                    break
            except Exception as e:
                continue
        self.assertIsNotNone(leader_stub, "No leader found in the cluster")

        # Send a set operation
        operation = "set y=30"
        request = raft_pb2.ClientRequestMessage(operation=operation)
        response = leader_stub.ClientRequest(request)
        self.assertIn("executed", response.result)

        # Wait for replication
        time.sleep(2)

        # Verify that all nodes have the entry in their logs
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                log_operations = [entry.operation for entry in state.log]
                self.assertIn(operation, log_operations, f"Operation not found in log of node {host}:{port}")
            except Exception as e:
                self.fail(f"Failed to get state from node {host}:{port}: {e}")

if __name__ == '__main__':
    unittest.main()
