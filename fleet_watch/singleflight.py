"""Bounded, cross-process single-flight coordination for Fleet Watch cycles.

A timeout must not create a second overlapping collector.  This module uses a
process-local lock for threads in one interpreter and an advisory ``flock`` for
separate launchd/CLI processes.  It never starts work and never waits past the
caller's deadline.
"""

from __future__ import annotations

import errno
import fcntl
import os
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class SingleFlightBusy(RuntimeError):
    """Raised when another caller owns the named cycle."""


_THREAD_LOCKS: dict[str, threading.Lock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


def _thread_lock(path: Path) -> threading.Lock:
    key = str(path)
    with _THREAD_LOCKS_GUARD:
        lock = _THREAD_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _THREAD_LOCKS[key] = lock
        return lock


class SingleFlight:
    """A named, bounded mutual-exclusion lease.

    The file descriptor is opened only after the thread lock is acquired and is
    always closed on both success and failure.  A failed non-blocking flock is
    retried only until ``timeout_seconds``; callers receive ``SingleFlightBusy``
    and can report UNKNOWN rather than starting duplicate work.
    """

    def __init__(self, path: Path | str, *, timeout_seconds: float = 1.0):
        if timeout_seconds < 0:
            raise ValueError("timeout_seconds must be non-negative")
        self.path = Path(path).expanduser()
        self.timeout_seconds = float(timeout_seconds)
        self._thread_lock = _thread_lock(self.path)
        self._local = threading.local()

    @contextmanager
    def acquire(self) -> Iterator[None]:
        if not self._thread_lock.acquire(timeout=self.timeout_seconds):
            raise SingleFlightBusy(f"single-flight busy: {self.path}")

        fd: int | None = None
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(str(self.path), os.O_RDWR | os.O_CREAT, 0o600)
            deadline = time.monotonic() + self.timeout_seconds
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError as exc:
                    if time.monotonic() >= deadline:
                        raise SingleFlightBusy(
                            f"single-flight busy: {self.path}"
                        ) from exc
                    time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
                except OSError as exc:
                    if exc.errno not in (errno.EACCES, errno.EAGAIN):
                        raise
                    if time.monotonic() >= deadline:
                        raise SingleFlightBusy(
                            f"single-flight busy: {self.path}"
                        ) from exc
                    time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
            yield
        finally:
            if fd is not None:
                try:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                except OSError:
                    pass
                try:
                    os.close(fd)
                except OSError:
                    pass
            self._thread_lock.release()

    def __enter__(self) -> "SingleFlight":
        context = self.acquire()
        self._local.context = context
        context.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        context = getattr(self._local, "context", None)
        if context is None:
            return
        try:
            context.__exit__(exc_type, exc, tb)
        finally:
            self._local.context = None
