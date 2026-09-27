from __future__ import annotations

import sys
import time

from fleet_watch.probe_runner import run_isolated


def test_isolated_probe_reaps_process_group_after_timeout():
    # The child creates a grandchild inheriting stdout/stderr. A plain
    # subprocess.run timeout can wait on the inherited pipe; the isolated
    # runner must terminate only this command's process group and return.
    script = (
        "import subprocess,sys,time; "
        "subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); "
        "time.sleep(30)"
    )
    started = time.monotonic()
    result = run_isolated([sys.executable, "-c", script], timeout_seconds=0.15)
    elapsed = time.monotonic() - started
    assert result.timed_out is True
    assert result.returncode is not None
    assert elapsed < 2.0
