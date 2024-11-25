import unittest
import grpc
import raft_pb2
import raft_pb2_grpc
import time
import subprocess
import re
import threading

class TestRaftAlgorithm(unittest.TestCase):

    def setUp(self):
        # List of node addresses (host, port)
        self.nodes = [
            ('localhost', 50051),
            ('localhost', 50052),
            ('localhost', 50053),
            ('localhost', 50054),
            ('localhost', 50055),
        ]

    def get_stub(self, host, port):
        """Create a gRPC stub for the given host and port."""
        channel = grpc.insecure_channel(f"{host}:{port}")
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        return stub

    def get_container_name(self, node_id):
        # Get the list of all containers (running and stopped)
        result = subprocess.run(["docker", "ps", "-a", "--format", "{{.Names}}"], capture_output=True, text=True)
        container_names = result.stdout.strip().split('\n')
        # Updated pattern to match hyphen instead of underscore
        pattern = re.compile(f".*node{node_id}-1$")
        for name in container_names:
            if pattern.match(name):
                return name
        # Debugging: Print all container names if not found
        print(f"Available containers: {container_names}")
        return None

    def find_leader(self):
        """Find and return the leader's stub and ID."""
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state == 'leader':
                    return stub, state.id
            except Exception:
                continue
        return None, None

    def test_leader_election(self):
        """Test that a leader is elected in the cluster."""
        leader_stub, leader_id = self.find_leader()
        self.assertIsNotNone(leader_stub, "No leader found in the cluster")
        print(f"Leader elected: Process {leader_id}")

    def test_basic_log_replication(self):
        """Test that a single client request is replicated to all followers."""
        leader_stub, leader_id = self.find_leader()
        self.assertIsNotNone(leader_stub, "No leader found in the cluster")

        # Send a client request to the leader
        operation = "set a=1"
        request = raft_pb2.ClientRequestMessage(operation=operation)
        response = leader_stub.ClientRequest(request)
        self.assertIn("executed", response.result)
        print(f"Client request '{operation}' executed by Leader {leader_id}")

        # Wait for replication
        time.sleep(2)

        # Verify that all nodes have the operation in their logs
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                log_operations = [entry.operation for entry in state.log]
                self.assertIn(operation, log_operations, f"Operation '{operation}' not found in node {host}:{port}")
                print(f"Node {state.id} has operation '{operation}' in its log.")
            except Exception as e:
                self.fail(f"Failed to get state from node {host}:{port}: {e}")

 
    def test_concurrent_client_requests(self):
        """Test that multiple client requests are correctly replicated and committed."""
        leader_stub, leader_id = self.find_leader()
        self.assertIsNotNone(leader_stub, "No leader found in the cluster")

        # Define multiple operations
        operations = ["set c=3", "set d=4", "set e=5", "set f=6", "set g=7"]

        # Function to send a client request
        def send_request(op):
            request = raft_pb2.ClientRequestMessage(operation=op)
            try:
                response = leader_stub.ClientRequest(request)
                self.assertIn("executed", response.result)
                print(f"Client request '{op}' executed by Leader {leader_id}")
            except Exception as e:
                self.fail(f"Failed to send client request '{op}': {e}")

        # Send operations concurrently
        threads = []
        for op in operations:
            thread = threading.Thread(target=send_request, args=(op,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Wait for replication
        time.sleep(3)

        # Verify all operations are present in all logs
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                log_operations = [entry.operation for entry in state.log]
                for op in operations:
                    self.assertIn(op, log_operations, f"Operation '{op}' not found in node {host}:{port}")
                print(f"All operations {operations} are present in node {state.id}'s log.")
            except Exception as e:
                self.fail(f"Failed to get state from node {host}:{port}: {e}")

    def test_node_recovery_after_log_divergence(self):
        """Test that a node with a divergent log correctly synchronizes upon recovery."""
        leader_stub, leader_id = self.find_leader()
        self.assertIsNotNone(leader_stub, "No leader found in the cluster")

        # Find a follower to stop
        follower_stub = None
        follower_id = None
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state != 'leader':
                    follower_stub = stub
                    follower_id = state.id
                    break
            except Exception as e:
                continue
        self.assertIsNotNone(follower_stub, "No follower found in the cluster")

        # Get the container name
        container_name = self.get_container_name(follower_id)
        self.assertIsNotNone(container_name, f"Container for follower node {follower_id} not found")
        
        # Stop the follower node
        print(f"Stopping follower node {container_name}")
        subprocess.run(["docker", "stop", container_name], check=True)

        # Send client requests to create log divergence
        divergent_operations = ["set h=8", "set i=9"]
        for op in divergent_operations:
            request = raft_pb2.ClientRequestMessage(operation=op)
            leader_stub.ClientRequest(request)
            print(f"Client request '{op}' executed by Leader {leader_id}")

        # Wait for replication
        time.sleep(3)

        # Restart the follower node
        print(f"Starting follower node {container_name}")
        subprocess.run(["docker", "start", container_name], check=True)

        # Wait for synchronization
        time.sleep(5)

        # Verify that the follower's log matches the leader's log
        try:
            restarted_stub = self.get_stub("localhost", 50051)  # Adjust based on node mapping
            restarted_state = restarted_stub.GetState(raft_pb2.Empty())
            current_leader_stub, current_leader_id = self.find_leader()
            self.assertIsNotNone(current_leader_stub, "No current leader found after follower recovery")
            leader_state = current_leader_stub.GetState(raft_pb2.Empty())

            # Check log length
            self.assertEqual(len(restarted_state.log), len(leader_state.log),
                             "Log length mismatch between recovered follower and current leader")
            
            # Check each operation
            for r_entry, l_entry in zip(restarted_state.log, leader_state.log):
                self.assertEqual(r_entry.operation, l_entry.operation,
                                 "Log operation mismatch between recovered follower and current leader")
            print(f"Recovered follower {follower_id} successfully synchronized with Leader {current_leader_id}.")
        except Exception as e:
            self.fail(f"Failed to verify log consistency after follower recovery: {e}")

    def test_client_request_forwarding(self):
        """Test that a client can send a request to any node and it gets forwarded to the leader."""
        # Choose a non-leader node
        non_leader_stub = None
        non_leader_id = None
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                if state.state != 'leader':
                    non_leader_stub = stub
                    non_leader_id = state.id
                    break
            except Exception:
                continue
        self.assertIsNotNone(non_leader_stub, "No non-leader node found to test forwarding")

        # Send a client request to the non-leader node
        operation = "set j=10"
        request = raft_pb2.ClientRequestMessage(operation=operation)
        response = non_leader_stub.ClientRequest(request)
        self.assertIn("executed", response.result)
        print(f"Client request '{operation}' sent to non-leader {non_leader_id} and executed successfully.")

        # Wait for replication
        time.sleep(2)

        # Verify that all nodes have the operation in their logs
        for host, port in self.nodes:
            try:
                stub = self.get_stub(host, port)
                state = stub.GetState(raft_pb2.Empty())
                log_operations = [entry.operation for entry in state.log]
                self.assertIn(operation, log_operations, f"Operation '{operation}' not found in node {host}:{port}")
                print(f"Node {state.id} has operation '{operation}' in its log.")
            except Exception as e:
                self.fail(f"Failed to get state from node {host}:{port}: {e}")

if __name__ == '__main__':
    unittest.main()
