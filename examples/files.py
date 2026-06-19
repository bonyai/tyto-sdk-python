"""Demonstrates file upload and download."""
import os
from tyto import Tyto

tyto = Tyto(api_key=os.environ.get("TYTO_API_KEY"))

nest = tyto.create(name="files-demo", template="ubuntu-24-dev")
print(f"Nest {nest.id} is {nest.status}")

# Upload a file
nest.put("./demo.txt", "demo.txt")
print("Uploaded file")

# Download the file
nest.get("demo.txt", "./demo.downloaded.txt")
print("Downloaded file")

# Upload a directory
nest.put("./mydir", "mydir")
print("Uploaded directory")

# Download a directory
nest.get("mydir", "./mydir-downloaded")
print("Downloaded directory")

nest.stop()
nest.delete()
print("Done")
