from ..core import TimeHint


def system_time_hint_clause(time_hint: TimeHint | None) -> str | None:
    if time_hint is None:
        return None
    return time_hint.build()
