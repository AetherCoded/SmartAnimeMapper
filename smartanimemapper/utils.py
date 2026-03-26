from __future__ import annotations

import os
import re
import unicodedata
from datetime import datetime, timezone
from typing import Optional



def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec='seconds')



def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None



def format_local_datetime(value: Optional[str]) -> str:
    dt = parse_iso(value)
    if not dt:
        return 'not available'
    return dt.astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')



def format_file_mtime(path: str) -> str:
    if not path or not os.path.exists(path):
        return 'not available'
    return datetime.fromtimestamp(os.path.getmtime(path), tz=timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')



def normalize_alias(value: str) -> str:
    value = unicodedata.normalize('NFKC', value).casefold().strip()
    value = re.sub(r'\s+', ' ', value)
    return value



def clean_title(value: str) -> str:
    value = unicodedata.normalize('NFKC', value).lower().strip()
    return ''.join(ch for ch in value if ch.isalnum())
