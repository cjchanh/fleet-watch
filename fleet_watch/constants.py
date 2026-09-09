"""Pinned local binaries.

Process inspection must not honor a caller-controlled PATH. A ``ps`` shim
on PATH can fake create-times and release a live exclusive lease.
"""

PS_BIN = "/bin/ps"
