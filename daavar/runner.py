"""Child processes (DRIVER-DESIGN §5 "Lock, processes", §8 "No interaction").

Every child runs in its own session (so its process group holds every descendant that does not
leave it), with stdin closed and stdout/stderr written to files. A timeout sends SIGTERM to the
group and SIGKILL `kill_grace_s` (10 s) later. When the child ends by itself, whatever it left
running in its group is killed too, so no grandchild outlives its step.

The result carries the exit code (negative: killed by that signal), whether the driver timed the
child out, the wall time and the child's peak RSS (`ru_maxrss` from `wait4`; the direct child
only, as the tools report their own `peak_rss_kb`).
"""

from __future__ import annotations

import os
import signal
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path

KILL_GRACE_S = 10.0
_POLL_S = 0.02


@dataclass
class Result:
    argv: list
    cwd: str
    env_delta: dict
    exit: int  # the exit code, or -N when signal N ended the child
    timed_out: bool  # the driver sent SIGTERM because the timeout passed
    killed: bool  # the driver had to send SIGKILL
    wall_ms: int
    peak_rss_kb: int | None
    stdout: str  # paths of the captured streams
    stderr: str
    timeout_s: float | None = None
    leftovers_killed: bool = False  # descendants were still running when the child ended
    extra: dict = field(default_factory=dict)

    @property
    def signal(self) -> int | None:
        return -self.exit if self.exit < 0 else None

    def read_stdout(self) -> str:
        return _read(self.stdout)

    def read_stderr(self) -> str:
        return _read(self.stderr)

    def as_record(self) -> dict:
        """The provenance record of the command (§5 Provenance)."""
        return {
            "argv": list(self.argv),
            "cwd": self.cwd,
            "env": dict(self.env_delta),
            "exit": self.exit,
            "timed_out": self.timed_out,
            "killed": self.killed,
            "wall_ms": self.wall_ms,
            "peak_rss_kb": self.peak_rss_kb,
            "stdout": self.stdout,
            "stderr": self.stderr,
        }


def _read(p: str) -> str:
    try:
        with open(p, "rb") as f:
            return f.read().decode("utf-8", errors="replace")
    except FileNotFoundError:
        return ""


def _killpg(pgid: int, sig: int) -> bool:
    try:
        os.killpg(pgid, sig)
        return True
    except (ProcessLookupError, PermissionError):
        return False


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False


class Interrupted(BaseException):
    """The driver received SIGTERM (__main__ installs the handler); handled like KeyboardInterrupt."""


_HELD = {signal.SIGINT, signal.SIGTERM}


class _signals_held:
    """A process group's cleanup that an interrupt cannot cut short (review S1-S6, Astra round 3): SIGINT and
    SIGTERM are blocked in this thread, and if one is still delivered (through another thread, or a handler
    raising), the group is SIGKILLed and awaited before the interruption goes on."""

    def __init__(self, pgid: int | None = None):
        self.pgid = pgid

    def __enter__(self):
        self.old = signal.pthread_sigmask(signal.SIG_BLOCK, _HELD)
        return self

    def __exit__(self, et, ev, tb):
        if et is not None and self.pgid is not None:
            end = time.monotonic() + 5
            while True:
                try:
                    _killpg(self.pgid, signal.SIGKILL)
                    while _group_alive(self.pgid) and time.monotonic() < end:
                        time.sleep(_POLL_S)
                    break
                except BaseException:  # yet another interrupt: keep killing
                    if time.monotonic() >= end:
                        break
        signal.pthread_sigmask(signal.SIG_SETMASK, self.old)
        return False


def stop_group(pgid: int, pid: int, grace: float = KILL_GRACE_S) -> None:
    """SIGTERM to the group, SIGKILL once the grace has passed with a member left, the leader reaped; not
    interruptible (signals held until the group is gone)."""
    with _signals_held(pgid):
        _stop_group(pgid, pid, grace)


def _stop_group(pgid: int, pid: int, grace: float) -> None:
    _killpg(pgid, signal.SIGTERM)
    end = time.monotonic() + grace
    while _group_alive(pgid) and time.monotonic() < end:
        try:
            os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            pass
        time.sleep(_POLL_S)
    if _group_alive(pgid):
        _killpg(pgid, signal.SIGKILL)
    try:
        os.waitpid(pid, 0)
    except ChildProcessError:
        pass


def _die_with_parent():
    """preexec: the child gets SIGKILL when the driver dies (Linux PR_SET_PDEATHSIG), so a SIGKILLed driver
    leaves no tool running (Opus F15); harmless where prctl is missing."""
    try:
        import ctypes
        ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL, 0, 0, 0)
    except Exception:
        pass


def run(argv, cwd, stdout_path, stderr_path, env_delta=None, timeout_s=None,
        kill_grace_s: float = KILL_GRACE_S, base_env=None, stdin_path=None) -> Result:
    """Runs `argv` in `cwd` to completion or timeout; never raises for the child's own failure.

    `env_delta` is added to `base_env` (default: this process's environment); a value None removes
    the variable. A program that cannot be started gives exit 127 and its reason on the stderr file.
    """
    env = dict(os.environ if base_env is None else base_env)
    delta = dict(env_delta or {})
    for k, v in delta.items():
        if v is None:
            env.pop(k, None)
        else:
            env[k] = str(v)
    Path(stdout_path).parent.mkdir(parents=True, exist_ok=True)
    Path(stderr_path).parent.mkdir(parents=True, exist_ok=True)
    argv = [str(a) for a in argv]
    start = time.monotonic()
    with open(stdout_path, "wb") as out, open(stderr_path, "wb") as err, \
            open(stdin_path or os.devnull, "rb") as inp:
        try:
            p = subprocess.Popen(argv, cwd=str(cwd), env=env, stdin=inp, stdout=out, preexec_fn=_die_with_parent,
                                 stderr=err, start_new_session=True, close_fds=True)
        except OSError as e:
            err.write(f"daavar: cannot start {argv[0]}: {e}\n".encode())
            return Result(argv, str(cwd), delta, 127, False, False, 0, None, str(stdout_path),
                          str(stderr_path), timeout_s)
    pid = pgid = p.pid  # start_new_session: the child leads its own session and group
    deadline = None if timeout_s is None else start + timeout_s
    timed_out = killed = False
    kill_at = None
    status = rusage = None
    try:
        while True:
            wpid, st, ru = os.wait4(pid, os.WNOHANG)
            if wpid == pid:
                status, rusage = st, ru
                break
            now = time.monotonic()
            if deadline is not None and not timed_out and now >= deadline:
                timed_out = True
                _killpg(pgid, signal.SIGTERM)
                kill_at = now + kill_grace_s
            if kill_at is not None and not killed and now >= kill_at:
                killed = True
                _killpg(pgid, signal.SIGKILL)
            time.sleep(_POLL_S)
    except BaseException:
        # The driver is interrupted (SIGINT, or SIGTERM turned into Interrupted by __main__): the child's group
        # goes with it, SIGTERM then SIGKILL after the grace, reaped, then the interruption goes on.
        stop_group(pgid, pid, kill_grace_s)
        p.returncode = -signal.SIGTERM
        raise
    p.returncode = os.waitstatus_to_exitcode(status)  # reaped here; Popen must not wait again
    wall_ms = int((time.monotonic() - start) * 1000)
    leftovers = False
    with _signals_held(pgid):
        leftovers = _kill_leftovers(pgid, timed_out, killed, kill_at)
        killed = killed or (leftovers and timed_out)
    peak = int(rusage.ru_maxrss) if rusage is not None else None
    return Result(argv, str(cwd), delta, p.returncode, timed_out, killed, wall_ms, peak, str(stdout_path),
                  str(stderr_path), timeout_s, leftovers)


def _kill_leftovers(pgid, timed_out, killed, kill_at) -> bool:
    leftovers = False
    if _group_alive(pgid):
        # The child ended (or was killed) and left descendants in its group: they go too. A pid
        # stays reserved while it names a live group, so this group is still the child's.
        leftovers = True
        if timed_out and not killed:
            # Give the group the rest of the grace period after SIGTERM, then SIGKILL.
            while _group_alive(pgid) and time.monotonic() < (kill_at or 0):
                time.sleep(_POLL_S)
        if _group_alive(pgid):
            _killpg(pgid, signal.SIGKILL)
        t_end = time.monotonic() + 5
        while _group_alive(pgid) and time.monotonic() < t_end:
            time.sleep(_POLL_S)
    return leftovers


def capture(argv, cwd=None, env_delta=None, timeout_s=60.0, kill_grace_s: float = KILL_GRACE_S) -> tuple:
    """A short helper run (git, a compiler's version): (exit, stdout, stderr), stdin closed, in its own
    session, output through temporary files; a timeout treats the group as run() does: SIGTERM, then SIGKILL
    once the grace has passed with any member left; leftovers of a finished helper are killed."""
    env = dict(os.environ)
    for k, v in (env_delta or {}).items():
        if v is None:
            env.pop(k, None)
        else:
            env[k] = str(v)
    from . import cache  # the temporary files go to the cache's tmp/, never to /tmp by default
    tdir = cache.ensure("tmp")
    with tempfile.TemporaryFile(dir=tdir) as fo, tempfile.TemporaryFile(dir=tdir) as fe:
        try:
            p = subprocess.Popen([str(a) for a in argv], cwd=None if cwd is None else str(cwd), env=env,
                                 stdin=subprocess.DEVNULL, stdout=fo, stderr=fe, start_new_session=True)
        except OSError as e:
            return 127, b"", str(e).encode()
        pgid, deadline, kill_at, code = p.pid, time.monotonic() + timeout_s, None, None
        try:
            while True:
                rc = p.poll()
                now = time.monotonic()
                if rc is not None and (kill_at is None or not _group_alive(pgid)):
                    code = 124 if kill_at is not None else rc
                    break
                if kill_at is None and now >= deadline:
                    _killpg(pgid, signal.SIGTERM)
                    kill_at = now + kill_grace_s
                elif kill_at is not None and now >= kill_at:
                    _killpg(pgid, signal.SIGKILL)
                    p.wait()
                    code = 124
                    break
                time.sleep(_POLL_S)
        except BaseException:
            stop_group(pgid, p.pid, kill_grace_s)
            p.returncode = -signal.SIGTERM
            raise
        with _signals_held(pgid):
            if _group_alive(pgid):
                _killpg(pgid, signal.SIGKILL)
        fo.seek(0)
        fe.seek(0)
        out, err = fo.read(), fe.read()
    if code == 124:
        err += f"\ntimed out after {timeout_s} s".encode()
    return code, out, err
