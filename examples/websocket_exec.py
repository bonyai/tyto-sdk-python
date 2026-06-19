"""
Demonstrates interactive WebSocket connections.

exec() opens a raw command WebSocket (/nest/{id}/exec).
console() opens an interactive shell WebSocket (/nest/{id}/console).
session.attach() streams output from a managed session.
"""
import os
import sys
from tyto import Tyto

tyto = Tyto(api_key=os.environ.get("TYTO_API_KEY"))

nest = tyto.nests.create(name="ws-demo", template="ubuntu-24-dev")
print(f"Nest {nest.id} is {nest.status}")

# --- Raw exec WebSocket ---
print("Opening exec WebSocket…")
with nest.exec() as ws:
    print("exec: connected")
    try:
        for message in ws:
            sys.stdout.write(str(message))
            sys.stdout.flush()
    except Exception:
        pass
    print("\nexec: closed")

# --- Managed session + attach ---
print("Creating managed session…")
session = nest.sessions.create(
    argv=["bash", "-lc", "for i in 1 2 3; do echo $i; sleep 0.2; done"],
    tty=True,
    cols=80,
    rows=24,
)

print(f"Attaching to session {session.id}…")
with session.attach() as ws:
    print("attach: connected")
    try:
        for message in ws:
            sys.stdout.write(str(message))
            sys.stdout.flush()
    except Exception:
        pass
    print("\nattach: closed")

nest.stop()
nest.delete()
print("Done")
