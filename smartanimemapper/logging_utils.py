from __future__ import annotations

import os
import traceback
from datetime import datetime, timezone
from typing import Optional


def utc_now_text() -> str:
    return datetime.now(timezone.utc).astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')



def log_error(log_path: str, message: str, exc: Optional[BaseException] = None) -> None:
    os.makedirs(os.path.dirname(log_path) or '.', exist_ok=True)
    with open(log_path, 'a', encoding='utf-8') as handle:
        handle.write(f'[{utc_now_text()}] ERROR: {message}\n')
        if exc is not None:
            handle.write(''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)))
            handle.write('\n')



def read_log(log_path: str, max_bytes: int = 200_000) -> str:
    if not os.path.exists(log_path):
        return ''
    with open(log_path, 'rb') as handle:
        handle.seek(0, os.SEEK_END)
        size = handle.tell()
        start = max(0, size - max_bytes)
        handle.seek(start)
        data = handle.read().decode('utf-8', errors='replace')
    return data



def clear_log(log_path: str) -> None:
    if os.path.exists(log_path):
        with open(log_path, 'w', encoding='utf-8'):
            pass
