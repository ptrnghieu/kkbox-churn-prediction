"""In-memory cache: msno → last streaming feature date.

Updated by /stream/notify after each day is flushed.
Read by the prediction service to show 'Features as of YYYY-MM-DD'.
"""
_msno_date: dict[str, str] = {}


def update(date: str, msnos: list[str]) -> None:
    for msno in msnos:
        _msno_date[msno] = date


def get_date(msno: str) -> str | None:
    return _msno_date.get(msno)


def clear() -> None:
    _msno_date.clear()
