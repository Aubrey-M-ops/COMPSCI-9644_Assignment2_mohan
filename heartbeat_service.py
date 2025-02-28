import grpc
from concurrent import futures
import time
import threading
from datetime import datetime
import heartbeat_service_pb2
import heartbeat_service_pb2_grpc
from google.protobuf.empty_pb2 import Empty

# keep record of the latest heartbeat received from each service
heartbeat_records = {
    "primary": None,
    "backup": None
}


def log_to_file(message):
    with open("heartbeat.txt", "log") as log_file:
        log_file.write(f"{message}\n")


class ViewService(heartbeat_service_pb2_grpc.ViewServiceServicer):
    # Receive heartbeat from primary and backup
    def Heartbeat(self, request, context):
        service = request.service_identifier
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        heartbeat_records[service] = time.time()
        log_to_file(
            f"{service.capitalize()} is alive. Latest heartbeat received at {current_time}")
        return Empty()


def monitor_heartbeats():
    while True:
        # Every 5 seconds, check if the primary and backup services have sent a heartbeat
        time.sleep(5)
        current_time = time.time()
        for service in ["primary", "backup"]:
            last_heartbeat = heartbeat_records.get(service)
            # Assume that if the last heartbeat was received more than 10 seconds ago, the service is down
            if last_heartbeat and current_time - last_heartbeat > 10:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                log_to_file(
                    f"{service.capitalize()} might be down. Latest heartbeat received at {timestamp}")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    heartbeat_service_pb2_grpc.add_ViewServiceServicer_to_server(
        ViewService(), server)
    server.add_insecure_port("[::]:50053")
    server.start()
    print("Heartbeat Server is running on port 50053...")

    # start a thread to monitor the heartbeats
    monitoring_thread = threading.Thread(
        target=monitor_heartbeats, daemon=True)
    monitoring_thread.start()

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
