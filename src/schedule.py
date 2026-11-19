import datetime
import re
from typing import Iterable, List, Optional, Tuple, Union

_TIME_RE = re.compile(r"^\s*(\d{1,2}):(\d{2})\s*$")


def parse_schedule_times(raw: Union[str, Iterable[str], None]) -> List[Tuple[int, int]]:
    """Parse 'HH:MM' entries (list or comma/space separated string) into sorted unique (hour, minute) tuples."""
    if not raw:
        return []
    items = re.split(r"[,\s;]+", raw) if isinstance(raw, str) else list(raw)
    parsed = set()
    for item in items:
        match = _TIME_RE.match(str(item or ""))
        if not match:
            continue
        hour, minute = int(match.group(1)), int(match.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            parsed.add((hour, minute))
    return sorted(parsed)


def normalize_schedule_times(raw: Union[str, Iterable[str], None]) -> List[str]:
    """Return schedule entries as canonical 'HH:MM' strings."""
    return [f"{h:02d}:{m:02d}" for h, m in parse_schedule_times(raw)]


def next_scheduled_run(
    times: List[Tuple[int, int]],
    now: Optional[datetime.datetime] = None,
) -> Optional[datetime.datetime]:
    """Return the next local datetime matching one of the scheduled times (strictly after now)."""
    if not times:
        return None
    current = now or datetime.datetime.now()
    for day_offset in (0, 1):
        day = current.date() + datetime.timedelta(days=day_offset)
        for hour, minute in times:
            candidate = datetime.datetime.combine(day, datetime.time(hour, minute))
            if candidate > current:
                return candidate
    return None
