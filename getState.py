import grpc
import raft_pb2
import raft_pb2_grpc

def get_node_state(host, port):
    """Connect to a Raft node and print its state."""
    # Create a gRPC channel to the given host and port
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = raft_pb2_grpc.RaftServiceStub(channel)
    
    try:
        # Request the state of the node
        response = stub.GetState(raft_pb2.Empty())
        print(f"Node ID: {response.id}")
        print(f"Term: {response.term}")
        print(f"State: {response.state}")
        print(f"Commit Index: {response.commitIndex}")
        print(f"Last Applied: {response.lastApplied}")
        print("Log Entries:")
        for entry in response.log:
            print(f"  - Index: {entry.index}, Term: {entry.term}, Operation: {entry.operation}")
    except grpc.RpcError as e:
        print(f"Failed to get state from node: {e}")

if __name__ == '__main__':
    # Define the target node's host and port
    host = 'localhost'
    port = [50051,50052,50053,50054,50055]

    # Get and print the state of the node
    for p in port:
        get_node_state(host, p)
        print('=='*8)
