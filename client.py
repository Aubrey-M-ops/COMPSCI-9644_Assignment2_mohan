"""
Client submits a request to the primary
"""

import grpc
import replication_pb2
import replication_pb2_grpc


def log_to_file(filename, key, value):
    with open(filename, "log") as log_file:
        log_file.write(f"{key} {value}\n")


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = replication_pb2_grpc.SequenceStub(channel)

        while True:
            key = input("Enter key (or type 'exit' to quit): ")
            if key.lower() == 'exit':
                print("Byebye")
                break

            value = input("Enter value: ")

            # Send request to Primary
            request = replication_pb2.WriteRequest(key=key, value=value)
            response = stub.Write(request)

            log_to_file("log/client.txt", key, value)
            print(f"[Client] Response from Primary: {response.ack}")


if __name__ == '__main__':
    run()
