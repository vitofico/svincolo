"""Persistent USD spending policies and calendar-period totals shared with the router."""
import fcntl
import hashlib
import json
import os
import re
import tempfile
from datetime import timedelta
from pathlib import Path

DIR = Path(os.environ.get("SVINCOLO_SPEND_DIR", "/data/spend"))
PERIODS = ("day", "week", "month")
SCOPES = ("app", "provider")
UNIT = 1_000_000


def windows(now):
    day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    monday = day - timedelta(days=day.weekday())
    month = day.replace(day=1)
    return {"day": (day, day + timedelta(days=1)),
            "week": (monday, monday + timedelta(days=7)),
            "month": (month, (month + timedelta(days=32)).replace(day=1))}


def read(name):
    return json.loads((DIR / name).read_text())


def atomic(name, obj):
    with tempfile.NamedTemporaryFile("w", dir=DIR, delete=False) as f:
        path = Path(f.name)
        try:
            json.dump(obj, f)
            f.flush()
            os.fsync(f.fileno())
            os.replace(path, DIR / name)
        finally:
            path.unlink(missing_ok=True)


def initialize():
    DIR.mkdir(mode=0o700, parents=True, exist_ok=True)
    with (DIR / "policy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if not (DIR / "limits.json").exists():
            atomic("limits.json", {scope: {} for scope in SCOPES})


def revision(policies):
    return hashlib.sha256(json.dumps(policies, sort_keys=True).encode()).hexdigest()


def amount(raw):
    if raw == "":
        return None
    if not re.fullmatch(r"[0-9]{1,9}(?:\.[0-9]{1,6})?", raw):
        raise ValueError("Enter a nonnegative USD amount with up to six decimal places.")
    dollars, _, fraction = raw.partition(".")
    return int(dollars) * UNIT + int(fraction.ljust(6, "0"))


def save(expected, scope, name, period, soft, hard):
    if scope not in SCOPES or period not in PERIODS:
        raise ValueError("Invalid budget period or scope.")
    soft, hard = amount(soft), amount(hard)
    if soft is not None and hard is not None and soft > hard:
        raise ValueError("The soft cap must be at or below the hard cap.")
    with (DIR / "policy.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        policies = read("limits.json")
        if revision(policies) != expected:
            raise ValueError("Another operator saved a change. Reload and try again.")
        if soft is None and hard is None:
            policies[scope].pop(name, None)
        else:
            policies[scope][name] = dict(period=period, soft=soft, hard=hard)
        atomic("limits.json", policies)


def label(value):
    if value is None:
        return ""
    return f"{value // UNIT}.{value % UNIT:06d}".rstrip("0").rstrip(".")


def alerts(policies, state):
    for scope in SCOPES:
        for name, cap in sorted(policies[scope].items()):
            used = state["periods"][cap["period"]][scope].get(name, 0)
            for kind in ("hard", "soft"):
                if cap[kind] is not None and used >= cap[kind]:
                    yield f"{scope.title()} {name} reached its {cap['period']} {kind} cap of ${label(cap[kind])}."
                    break
