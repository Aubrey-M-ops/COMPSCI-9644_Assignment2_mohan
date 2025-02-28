"""
Backup Server:
1. Receive request from Primary
2. Apply the write operation to the backup store
3. Send ACK to the primary
"""
import grpc
import threading
import time
from concurrent import futures
import replication_pb2
import replication_pb2_grpc
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc

backup_store = {}


def log_to_file(filename, key, value):
    with open(filename, "log") as log_file:
        log_file.write(f"{key} {value}\n")


class BackupService(replication_pb2_grpc.SequenceServicer):
    def Write(self, request, context):
        # Apply the write operation to the backup store
        backup_store[request.key] = request.value
        log_to_file("log/backup.txt", request.key, request.value)
        print(f"[Backup] Stored: {request.key} -> {request.value}")

        # Send ACK to the client
        return replication_pb2.WriteResponse(ack="ACK")


def send_heartbeat():
    with grpc.insecure_channel("localhost:50053") as channel:
        stub = heartbeat_service_pb2_grpc.ViewServiceStub(channel)
        while True:
            request = heartbeat_service_pb2.HeartbeatRequest(
                service_identifier="backup")
            try:
                stub.Heartbeat(request)
            except grpc.RpcError:
                print("[Backup] Failed to send heartbeat")
            time.sleep(5)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_SequenceServicer_to_server(
        BackupService(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Backup server is running on port 50052...")

    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    heartbeat_thread.start()

    server.wait_for_termination()


if __name__ == '__main__':
    serve()
