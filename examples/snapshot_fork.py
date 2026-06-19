"""Demonstrates snapshots: create, list, fork, restore, and delete."""
import os
from tyto import Tyto

tyto = Tyto(api_key=os.environ.get("TYTO_API_KEY"))

nest = tyto.create(name="snapshot-demo", template="ubuntu-24-dev")
print(f"Nest {nest.id} is {nest.status}")

# Create a snapshot
print("Creating snapshot…")
snap = nest.create_snapshot(name="my-snapshot", description="Before refactoring")
print(f"Snapshot {snap.id} state: {snap.state}")

# List snapshots
snap_list = nest.snapshots.list()
print(f"Snapshots: {len(snap_list.snapshots or [])}")

# Fork the nest
print("Forking nest…")
fork = nest.fork(name="snapshot-demo-fork", stop_if_running=False, restart_source=True)
print(f"Forked → {fork.id} ({fork.status})")

# Restore (requires nest to be stopped first)
# nest.stop()
# restored = nest.restore(snap.id)
# print(f"Restored from {restored.restored_from}: {restored.status}")

# Delete the snapshot (dry run first)
if snap.id:
    dry = nest.delete_snapshot(snap.id, dry_run=True)
    print(f"Would free {dry.would_free_bytes or 0} bytes, can_delete={dry.can_delete}")

    if dry.can_delete:
        result = nest.delete_snapshot(snap.id)
        print(f"Deleted: {result.deleted}")

nest.stop()
nest.delete()
print("Done")
