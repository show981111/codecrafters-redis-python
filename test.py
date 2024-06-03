# Define the binary content you want to write to the file
binary_content = b"REDIS0003\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfe\x00\xfb\x01\x00\x00\traspberry\x06orange\xff|5S\xbf\xdb\x01\xcd\x8a\n"

# Define the path to the file
file_path = "binary_file.bin"

# Open the file in write-binary mode and write the binary content to it
with open(file_path, "wb") as file:
    file.write(binary_content)

print(f"Binary content written to {file_path}")
