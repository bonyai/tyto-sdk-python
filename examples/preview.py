"""Uploads a web page to a nest, starts a server, and creates a live preview URL."""
import os
import time
from pathlib import Path
from tyto import Tyto

tyto = Tyto(api_key=os.environ.get("TYTO_API_KEY"))

nest = tyto.create(name="preview-demo")
print(f"Nest {nest.id} is {nest.status}")

# Upload the HTML page to the nest
html = Path(__file__).parent / "assets" / "index.html"
nest.fs.write("index.html", html.read_bytes(), kind="file")
print("Uploaded index.html")

# Start a Python HTTP server on port 3000 serving /home/tyto
nest.create_session(
    argv=["python3", "-m", "http.server", "3000"],
    tty=True,
    cwd="/home/tyto",
    cols=80,
    rows=24,
)
print("Web server started on port 3000")

# Give the server a moment to bind
time.sleep(1.5)

# Create a public preview (no auth token needed to open in browser)
preview = nest.previews.create(port=3000, auth="public", name="my-app")

print()
print("┌─────────────────────────────────────────┐")
print("│  🌐 Open in browser:                    │")
print(f"│  {preview.url}")
print("└─────────────────────────────────────────┘")
print()

# Inspect via top-level resource
if preview.id:
    inspected = tyto.previews.get(preview.id)
    print(f"Preview ID: {inspected.id}  expires: {inspected.expires_at}")

# The nest is left running so you can view the preview.
# When done, stop and clean up:
# nest.stop()
# nest.delete()
