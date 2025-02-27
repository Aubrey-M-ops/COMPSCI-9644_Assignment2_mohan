"""
Primary Server: 
1. Receive client request
2. Send request to backup
3. Receice ACK from backup
4. If ACK is received, store data and log to file
5. Send ACK and result to the client.
"""

import grpc
import replication_pb2
import replication_pb2_grpc
from concurrent import futures

primary_store = {}


def log_to_file(filename, key, value):
    with open(filename, "a") as log_file:
        log_file.write(f"{key} {value}\n")


class PrimaryService(replication_pb2_grpc.SequenceServicer):
    def Write(self, request, context):
        try:
            # connect to Backup server
            with grpc.insecure_channel('localhost:50052') as channel:
                stub = replication_pb2_grpc.SequenceStub(channel)

                # Send request to Backup
                backup_response = stub.Write(request)
                # Receive ACK from Backup
                if backup_response.ack == "ACK":
                    # If ACK is received, store data and log to file
                    primary_store[request.key] = request.value
                    log_to_file("primary.txt", request.key, request.value)
                    print(
                        f"[Primary] Stored: {request.key} -> {request.value}")
                    # Send ACK and result to the client
                    return replication_pb2.WriteResponse(ack="Write successful")
                else:
                    return replication_pb2.WriteResponse(ack="Write failed")

        except Exception as e:
            print(f"[Primary] Error communicating with Backup: {e}")
            return replication_pb2.WriteResponse(ack="Write failed")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_SequenceServicer_to_server(
        PrimaryService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Primary server is running on port 50051...")
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
