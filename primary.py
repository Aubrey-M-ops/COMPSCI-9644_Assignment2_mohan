"""
Primary Server: 
1. Receive client request
2. Send request to backup
3. Receice ACK from backup
4. If ACK is received, store data and log to file
5. Send ACK and result to the client.
"""

import grpc
import threading
import time
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
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
                    log_to_file("log/primary.txt", request.key, request.value)
                    print(
                        f"[Primary] Stored: {request.key} -> {request.value}")
                    # Send ACK and result to the client
                    return replication_pb2.WriteResponse(ack=f"Write successful: key={request.key}, value={request.value}")
                else:
                    return replication_pb2.WriteResponse(ack="Write failed")

        except Exception as e:
            print(f"[Primary] Error communicating with Backup: {e}")
            return replication_pb2.WriteResponse(ack="Write failed")

# Heartbeat service thread


def send_heartbeat():
    with grpc.insecure_channel("localhost:50053") as channel:
        stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
        while True:
            request = heartbeat_service_pb2.HeartbeatRequest(
                service_identifier="primary")
            try:
                stub.Heartbeat(request)
            except grpc.RpcError:
                print("[Primary] Failed to send heartbeat")
            time.sleep(5)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_SequenceServicer_to_server(
        PrimaryService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Primary server is running on port 50051...")

    # start a thread to send heartbeat
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

    server.wait_for_termination()


if __name__ == '__main__':
    serve()
