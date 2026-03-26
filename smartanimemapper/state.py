from __future__ import annotations

import threading
from typing import Dict, Optional

from .config_store import load_runtime, save_runtime
from .utils import now_iso


class RuntimeState:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._runtime = load_runtime()

    def snapshot(self) -> Dict:
        with self._lock:
            return self._runtime

    def task_running(self, name: str) -> bool:
        with self._lock:
            return bool(self._runtime['tasks'].get(name, {}).get('running'))

    def start_task(self, name: str, message: str) -> None:
        with self._lock:
            task = self._runtime['tasks'][name]
            task.update({
                'running': True,
                'progress': 0,
                'message': message,
                'started_at': now_iso(),
                'finished_at': None,
            })
            save_runtime(self._runtime)

    def update_task(self, name: str, progress: Optional[int] = None, message: Optional[str] = None) -> None:
        with self._lock:
            task = self._runtime['tasks'][name]
            if progress is not None:
                task['progress'] = max(0, min(100, int(progress)))
            if message is not None:
                task['message'] = message
            save_runtime(self._runtime)

    def finish_task(self, name: str, result: Dict, message: Optional[str] = None) -> None:
        with self._lock:
            task = self._runtime['tasks'][name]
            task['running'] = False
            task['progress'] = 100
            task['message'] = message or 'finished'
            task['finished_at'] = now_iso()
            self._runtime['results'][name] = result
            save_runtime(self._runtime)

    def fail_task(self, name: str, result: Dict, message: str) -> None:
        with self._lock:
            task = self._runtime['tasks'][name]
            task['running'] = False
            task['message'] = message
            task['finished_at'] = now_iso()
            self._runtime['results'][name] = result
            save_runtime(self._runtime)

    def set_backup_result(self, result: Dict) -> None:
        with self._lock:
            self._runtime['results']['backup'] = result
            save_runtime(self._runtime)
