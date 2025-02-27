"""
Backup Server:
1. Receive request from Primary
2. Apply the write operation to the backup store
3. Send ACK to the primary
"""
import grpc
from concurrent import futures
import replication_pb2
import replication_pb2_grpc

backup_store = {}

def log_to_file(filename, key, value):
    with open(filename, "a") as log_file:
        log_file.write(f"{key} {value}\n")

class BackupService(replication_pb2_grpc.SequenceServicer):
    def Write(self, request, context):
        # Apply the write operation to the backup store
        backup_store[request.key] = request.value
        log_to_file("log/backup.txt", request.key, request.value)
        print(f"[Backup] Stored: {request.key} -> {request.value}")
        
        # Send ACK to the client
        return replication_pb2.WriteResponse(ack="ACK")

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    replication_pb2_grpc.add_SequenceServicer_to_server(BackupService(), server)
    server.add_insecure_port('[::]:50052')
    server.start()
    print("Backup server is running on port 50052...")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
