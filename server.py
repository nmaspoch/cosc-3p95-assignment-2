import socket, os, struct, concurrent.futures, time

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)

from opentelemetry.sdk.resources import Resource

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, TraceIdRatioBased

resource = Resource.create({"service.name": "file-transfer-client"}) 
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)
# Sets the global default tracer provider
trace.set_tracer_provider(provider)
# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("server.tracer")

metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
provider = MeterProvider(metric_readers=[metric_reader])
# Sets the global default meter provider
metrics.set_meter_provider(provider)
# Creates a meter from the global meter provider
meter = metrics.get_meter("server.meter")

files_received_counter = meter.create_counter("files.received", "Number of files received")
file_size_histogram = meter.create_histogram(name="files.size", unit="bytes", description="Distribution of file sizes")

tracer_provider = TracerProvider(sampler=ALWAYS_ON)
# tracer_provider = TracerProvider(sampler=TraceIdRatioBased(0.25))


def receive_file_size(sck: socket.socket):
    # This funcion makes sure that the bytes which indicate the size of the file that will be sent are received.
    # The file is packed by the client via struct.pack(), 
    # a function that generates a bytes sequence that represents the file size.
    fmt = "<Q"
    expected_bytes = struct.calcsize(fmt)
    received_bytes = 0
    stream = bytes()
    while received_bytes < expected_bytes:
        chunk = sck.recv(expected_bytes - received_bytes)
        stream += chunk
        received_bytes += len(chunk)
    filesize = struct.unpack(fmt, stream)[0]
    return filesize


def receive_file(sck: socket.socket, filename, index):
    with tracer.start_as_current_span("file_span") as file_span:
        file_span.add_event(f'Received file {index}')
        file_span.set_attribute("name", filename)
        file_span.set_attribute("index", index)

        filesize = receive_file_size(sck)
        file_span.set_attribute("filesize", filesize)
        file_size_histogram.record(filesize)
        with open(filename, "wb") as f:
            received_bytes = 0
            while received_bytes < filesize:
                bytes_to_receive = min(1024, filesize - received_bytes)
                chunk = sck.recv(bytes_to_receive)
                if not chunk:
                    raise ConnectionError(f"Connection closed. Received {received_bytes}/{filesize} bytes")
                f.write(chunk)
                received_bytes += len(chunk)
        file_span.add_event(f'Received {received_bytes} bytes out of {filesize} total bytes')
        files_received_counter.add(1)

def handle_client(conn, address):
    with tracer.start_as_current_span("client_span") as client_span:
        client_span.set_attribute("client.address", f"{address[0]}:{address[1]}")

        num_files = receive_file_size(conn)
        client_span.set_attribute("num_files_expected", num_files)

        client_span.add_event(f"Client connected, expecting {num_files} files")
        for i in range(num_files):
            client_span.add_event(f"Receiving file {i}...")
            filepath = os.path.join(download_folder, f"received-{time.time()}-{i}.bin")
            receive_file(conn, filepath, i)
            client_span.add_event(f"File {i} received.")
        conn.close()
        client_span.add_event("Client disconnected.")

download_folder = "./downloads"
os.makedirs(download_folder, exist_ok=True)

# Delete all files in downloads folder
for filename in os.listdir(download_folder):
        file_path = os.path.join(download_folder, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except OSError as e:
                print(f"Error deleting {file_path}: {e}")

with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    with socket.create_server(("localhost", 3000)) as server:
        while True:
            print("Waiting for the client...")
            conn, address = server.accept()
            executor.submit(handle_client, conn, address)