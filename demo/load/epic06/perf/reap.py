#!/usr/bin/env python3
# Runs a command as a child subreaper: the sandbox's PID 1 (`sleep infinity`) never reaps orphans, so the
# processes the stand's samplers leave behind (curl, node, docker) stay zombies and use up pids.max. Every
# orphan of the command is reparented to this process instead and reaped here. Exit code: the command's.
# Usage: reap.py <command> [args…]
import ctypes
import os
import sys

PR_SET_CHILD_SUBREAPER = 36
if ctypes.CDLL(None, use_errno=True).prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
    sys.exit(f"reap: prctl failed: {os.strerror(ctypes.get_errno())}")
child = os.fork()
if child == 0:
    os.execvp(sys.argv[1], sys.argv[1:])
import time

status = None
deadline = None
while True:
    try:
        pid, raw = os.waitpid(-1, 0 if deadline is None else os.WNOHANG)
    except ChildProcessError:
        break
    if pid == child:
        status = os.waitstatus_to_exitcode(raw)
        deadline = time.monotonic() + 30  # orphans still running get 30 s to end, then PID 1 has them
    elif pid == 0:
        if time.monotonic() > deadline:
            break
        time.sleep(0.2)
sys.exit(status if status >= 0 else 128 - status)
