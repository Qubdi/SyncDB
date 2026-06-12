"""Concurrency tests for the watermark file store.

save_watermark serialises its read-modify-write under an OS-level lock on a
.lock sidecar plus an in-process mutex.  Without that lock, concurrent writers
interleave read-modify-write cycles and silently drop each other's keys — so
these tests hammer one store file from many threads and many PROCESSES and
assert that every written key survives.
"""

import json
import subprocess
import sys
import threading
from pathlib import Path

import pytest
from syncdb.sync.watermark import read_watermark_file, save_watermark

# Each worker writes its keys through the real save_watermark path (lock,
# read-modify-write, temp file + os.replace).  Distinct keys per worker means
# any lost update is visible as a missing key in the final store.
_WORKER_SCRIPT = """
import sys
from pathlib import Path
from syncdb.sync.watermark import save_watermark

store = Path(sys.argv[1])
worker = sys.argv[2]
for i in range(int(sys.argv[3])):
    save_watermark({"path": store, "key": f"w{worker}_{i}"}, i)
"""


def test_threaded_writers_do_not_lose_keys(tmp_path):
    store = tmp_path / "wm.json"
    workers, writes = 8, 10

    def write_keys(worker: int) -> None:
        for i in range(writes):
            save_watermark({"path": store, "key": f"t{worker}_{i}"}, i)

    threads = [threading.Thread(target=write_keys, args=(w,)) for w in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    values = read_watermark_file(store)
    assert len(values) == workers * writes
    assert values["t0_0"] == 0


@pytest.mark.slow
def test_cross_process_writers_do_not_lose_keys(tmp_path):
    """The OS-level lock must serialise SEPARATE processes, not just threads —
    the in-process mutex cannot help here, so this exercises msvcrt/flock."""
    store = tmp_path / "wm.json"
    workers, writes = 4, 25

    procs = [
        subprocess.Popen(
            [sys.executable, "-c", _WORKER_SCRIPT, str(store), str(w), str(writes)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for w in range(workers)
    ]
    for proc in procs:
        _, stderr = proc.communicate(timeout=120)
        assert proc.returncode == 0, stderr.decode(errors="replace")

    values = read_watermark_file(store)
    assert len(values) == workers * writes
    # The store must also still be valid, pretty-printed JSON (atomic replace).
    assert isinstance(json.loads(Path(store).read_text(encoding="utf-8")), dict)
