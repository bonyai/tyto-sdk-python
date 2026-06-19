import os
from tyto import Tyto

tyto = Tyto(api_key=os.environ.get("TYTO_API_KEY"))

me = tyto.me()
print(f"Signed in as {me.email}")

print("Creating nest…")
nest = tyto.create(name="quickstart-demo", template="ubuntu-24-dev")
print(f"Nest {nest.id} is {nest.status}")

print("Uploading file…")
nest.put("./hello.txt", "hello.txt")

print("Running command…")
output = nest.run(["bash", "-lc", "cat ~/hello.txt"])
print(f"Output: {output.strip()}")

print("Downloading file…")
nest.get("hello.txt", "./hello.downloaded.txt")

print("Stopping nest…")
nest.stop()
print(f"Nest status: {nest.status}")

print("Deleting nest…")
nest.delete()
print(f"Nest {nest.id} deleted")
