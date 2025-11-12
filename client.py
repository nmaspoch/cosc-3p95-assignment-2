import os, socket, struct, random, time

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

total_bytes = 0

resource = Resource.create({"service.name": "file-transfer-server"}) 
provider = TracerProvider(resource=resource)
processor = BatchSpanProcessor(ConsoleSpanExporter())
provider.add_span_processor(processor)
# Sets the global default tracer provider
trace.set_tracer_provider(provider)
# Creates a tracer from the global tracer provider
tracer = trace.get_tracer("client.tracer")

metric_reader = PeriodicExportingMetricReader(ConsoleMetricExporter())
provider = MeterProvider(metric_readers=[metric_reader])
# Sets the global default meter provider
metrics.set_meter_provider(provider)
# Creates a meter from the global meter provider
meter = metrics.get_meter("client.meter")

files_generated_counter = meter.create_counter("files.generated", "Number of files generated")

tracer_provider = TracerProvider(sampler=ALWAYS_ON)
# tracer_provider = TracerProvider(sampler=TraceIdRatioBased(0.25))

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
        # Get the size of the outgoing file.
        filesize = os.path.getsize(filename)
        sent_file.set_attribute("file_size", filesize)
        # First inform the server the amount of bytes that will be sent.
        sck.sendall(struct.pack("<Q", filesize))
        # Send the file in 1024-bytes chunks.
        with open(filename, "rb") as f:
            while read_bytes := f.read(1024):
                sck.sendall(read_bytes)
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
