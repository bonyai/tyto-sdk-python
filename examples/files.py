"""Demonstrates file upload and download."""
import os
from tyto import Tyto

tyto = Tyto(api_key=os.environ.get("TYTO_API_KEY"))

nest = tyto.nests.create(name="files-demo", template="ubuntu-24-dev")
print(f"Nest {nest.id} is {nest.status}")

# Upload a file
content = b"Hello, Tyto!\n"
nest.fs.write("demo.txt", content, kind="file")
print("Uploaded file")

# Download the file
result = nest.fs.read("demo.txt")
print(f"Downloaded {result.kind}: {result.data.decode()}")

# Upload a directory as a tar archive
# import tarfile, io
# buf = io.BytesIO()
# with tarfile.open(fileobj=buf, mode="w") as tar:
#     info = tarfile.TarInfo(name="hello.txt")
#     data = b"hi\n"
#     info.size = len(data)
#     tar.addfile(info, io.BytesIO(data))
# nest.fs.write("mydir", buf.getvalue(), kind="dir")
# print("Uploaded directory")

nest.stop()
nest.delete()
print("Done")
