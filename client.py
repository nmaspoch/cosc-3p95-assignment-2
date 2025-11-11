import os, socket, struct, random, time

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

def send_file(sck: socket.socket, filename):
    # Get the size of the outgoing file.
    filesize = os.path.getsize(filename)
    # First inform the server the amount of bytes that will be sent.
    sck.sendall(struct.pack("<Q", filesize))
    # Send the file in 1024-bytes chunks.
    with open(filename, "rb") as f:
        while read_bytes := f.read(1024):
            sck.sendall(read_bytes)

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
for i in range(num_files):
    size_in_bytes = random.randint(5120, 104857600) # 5KB to 100MB
    filepath = os.path.join(upload_folder, f'file_{i}.bin') 
    with open(filepath, "wb") as f:
        f.write(b'\0' * size_in_bytes)

files = sorted(list_files(upload_folder))
print('All Files:', files)



with socket.create_connection(("localhost", 3000)) as conn:
    conn.sendall(struct.pack("<Q", len(files)))
    print("Connected to the server.")
    for file in files:
        print(f'Sending file {file}')
        send_file(conn, os.path.join(upload_folder, file))  
        print("Sent.")

print("Connection closed.")
