import grpc
import raft_pb2
import raft_pb2_grpc
import sys

def main():
    if len(sys.argv) != 4:
        print("Usage: python client.py <host> <port> <operation>")
        return
    host = 'localhost'
    port = sys.argv[2]
    operation = sys.argv[3]
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = raft_pb2_grpc.RaftServiceStub(channel)
    request = raft_pb2.ClientRequestMessage(operation=operation)
    response = stub.ClientRequest(request)
    print(f"Client received response: {response.result}")

if __name__ == '__main__':
    main()
