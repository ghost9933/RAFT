import grpc
import sys
import raft_pb2
import raft_pb2_grpc

def run():
    if len(sys.argv) != 3:
        print("Usage: python client.py [node address] [operation]")
        return

    address = sys.argv[1]
    operation = sys.argv[2]

    channel = grpc.insecure_channel(address)
    stub = raft_pb2_grpc.RaftNodeStub(channel)
    response = stub.ClientRequest(raft_pb2.ClientRequestMessage(operation=operation))
    print(f"Client received: {response.result}")

if __name__ == '__main__':
    run()
