import socket, os, struct, random, time, zlib

from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
)
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

from opentelemetry.sdk.resources import Resource

from opentelemetry import metrics
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, TraceIdRatioBased

from cryptography.fernet import Fernet

sampling_rate = float(os.environ.get("SAMPLING_RATE", "1.0"))  
tracing_enabled = os.environ.get("TRACING_ENABLED", "true").lower() == "true"

if tracing_enabled:
    # Existing tracer setup
    resource = Resource.create({"service.name": "file-transfer-client"}) 
    sampling_rate = float(os.environ.get("SAMPLING_RATE", "1.0"))  
    if sampling_rate >= 1.0:
        tracerProvider = TracerProvider(sampler=ALWAYS_ON, resource=resource)
    else:
        tracerProvider = TracerProvider(sampler=TraceIdRatioBased(sampling_rate), resource=resource)
        
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4318/v1/traces"))
    tracerProvider.add_span_processor(processor)
    trace.set_tracer_provider(tracerProvider)
    tracer = trace.get_tracer("client.tracer")

    reader = PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint="http://localhost:4318/v1/metrics"),
        export_interval_millis=5000  
    )
    meterProvider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(meterProvider) 
    meter = metrics.get_meter("client.meter")
else:
    # Use no-op providers (no telemetry collected)
    from opentelemetry.sdk.trace import TracerProvider as NoOpTracerProvider
    from opentelemetry.sdk.metrics import MeterProvider as NoOpMeterProvider
    
    trace.set_tracer_provider(NoOpTracerProvider())
    tracer = trace.get_tracer("client.tracer")
    
    metrics.set_meter_provider(NoOpMeterProvider())
    meter = metrics.get_meter("client.meter")

files_generated_counter = meter.create_counter("files.generated", "Number of files generated")
compressed_size_histogram = meter.create_histogram(name="files.compressed_size", unit="bytes", description="Distribution of compressed file sizes")
encrypted_size_histogram = meter.create_histogram(name="files.encrypted_size", unit="bytes", description="Distribution of encrypted file sizes")
original_size_histogram = meter.create_histogram(name="files.original_size", unit="bytes", description="Distribution of original file sizes")

key = b'nyY7ownDE1chVYqULAOrHQ7BYOlDc_FL-7XMEI4yihI='
cipher = Fernet(key)

def list_files(dir_path):
    # list to store files
    res = []
    try:
        for file_path in os.listdir(dir_path):
            if os.path.isfile(os.path.join(dir_path, file_path)):
                res.append(file_path)
    except FileNotFoundError:
        print(f"The directory {dir_path} does not exist")
    except PermissionError:
        print(f"Permission denied to access the directory {dir_path}")
    except OSError as e:
        print(f"An OS error occurred: {e}")
    return res

def send_file(sck: socket.socket, filename, index):
    with tracer.start_as_current_span("sent_file") as sent_file:
        sent_file.add_event(f"Sending file {index}")
        sent_file.set_attribute("file_name", filename)
        sent_file.set_attribute("index", index)

        with open(filename, "rb") as f:
            original_file = f.read()
        sent_file.add_event("Compressing file")
        compressed_file = zlib.compress(original_file, zlib.Z_BEST_COMPRESSION)
        sent_file.add_event("Compressed file")

        original_size = len(original_file)
        original_size_histogram.record(original_size)
        compressed_size = len(compressed_file)
        compressed_size_histogram.record(compressed_size)

        sent_file.set_attribute("original_size", original_size)
        sent_file.set_attribute("compressed_size", compressed_size)

        compress_ratio = (original_size - compressed_size) / original_size
        sent_file.set_attribute("compression_ratio", compress_ratio)

        sent_file.add_event("Encrypting file")
        encrypted_file = cipher.encrypt(compressed_file)
        sent_file.add_event("File encrypted")

        encrypted_size = len(encrypted_file)
        encrypted_size_histogram.record(encrypted_size)
        sent_file.set_attribute("encrypted_size", encrypted_size)

        # First inform the server the amount of bytes that will be sent.
        sck.sendall(struct.pack("<Q", encrypted_size))
        # Send the file in 1024-bytes chunks.
        offset = 0
        while offset < encrypted_size:
            chunk = encrypted_file[offset:offset + 1024]
            sck.sendall(chunk)
            offset += len(chunk)
        sent_file.add_event(f"File {index} uploaded")

# Must avoid conflicts between clients
client_id = int(time.time() * 1000)  
upload_folder = f"./upload_{client_id}"
os.makedirs(upload_folder, exist_ok=True)  # Create folder if it doesn't exist

# Delete all files in upload folder
for filename in os.listdir(upload_folder):
        file_path = os.path.join(upload_folder, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
            except OSError as e:
                print(f"Error deleting {file_path}: {e}")

# Choose between 20 and 50 files to generate
num_files = random.randint(20, 50)

# Generate files
with tracer.start_as_current_span("file_generation_span") as file_generation_span:
    for i in range(num_files):
        file_generation_span.add_event(f"Generating file #{i}")
        size_in_bytes = random.randint(5120, 104857600) # 5KB to 100MB
        total_bytes += size_in_bytes
        filepath = os.path.join(upload_folder, f'file_{i}.bin') 
        with open(filepath, "wb") as f:
            f.write(b'\0' * size_in_bytes)
        files_generated_counter.add(1)
    file_generation_span.set_attribute("num_files_generated", num_files)
    file_generation_span.set_attribute("total_size_generated", total_bytes)
    file_generation_span.add_event(f"Generated {num_files} files")

files = sorted(list_files(upload_folder))

with tracer.start_as_current_span("client_span") as client_span:
    client_span.set_attribute("client_id", client_id)
    client_span.set_attribute("total_files", num_files)
    client_span.set_attribute("total_bytes", total_bytes)

    with socket.create_connection(("localhost", 3000)) as conn:
        conn.sendall(struct.pack("<Q", len(files)))
        client_span.add_event(f"Client {client_id} has connected to server")
        for i in range(len(files)):
            client_span.add_event(f'Sending file {files[i]}')
            send_file(conn, os.path.join(upload_folder, files[i]), i)  
            client_span.add_event("Sent")

    client_span.add_event("Connection closed.")

if tracing_enabled:
    tracerProvider.shutdown()  
    meterProvider.shutdown()
