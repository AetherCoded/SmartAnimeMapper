from __future__ import annotations

import json
import os
import threading
from typing import Callable, Dict
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from filelock import FileLock, Timeout

from .compiler import compile_patch
from .config_store import load_settings, save_settings
from .db_ops import apply_radarr_patch, apply_sonarr_patch, detect_radarr_alt_table, sqlite_backup
from .fetchers import fetch_all, monthly_fetch_due
from .logging_utils import log_error
from .state import RuntimeState
from .utils import now_iso


class JobManager:
    def __init__(self, runtime: RuntimeState):
        self.runtime = runtime
        timezone_name = os.environ.get('TZ', 'UTC')
        try:
            tz = ZoneInfo(timezone_name)
        except Exception:
            tz = ZoneInfo('UTC')
        self.scheduler = BackgroundScheduler(timezone=tz)
        self.scheduler.add_job(self._scheduled_fetch_guard, 'cron', hour='*', minute='17', id='monthly_fetch_guard', replace_existing=True)
        self.scheduler.start()

    def shutdown(self) -> None:
        if self.scheduler.running:
            self.scheduler.shutdown(wait=False)

    def _scheduled_fetch_guard(self) -> None:
        settings = load_settings()
        schedule = settings['fetch']['schedule']
        if not schedule.get('enabled', True):
            return
        if self.runtime.task_running('fetch'):
            return
        day_of_month = int(schedule.get('day_of_month', 1))
        last_anidb = settings['fetch']['last_fetch'].get('anidb_titles')
        last_kometa = settings['fetch']['last_fetch'].get('kometa_mapping')
        if not monthly_fetch_due(last_anidb, day_of_month) and not monthly_fetch_due(last_kometa, day_of_month):
            return
        self.start_background_task('fetch', self._run_fetch, force=False)

    def start_background_task(self, name: str, func: Callable[..., Dict], **kwargs) -> bool:
        if self.runtime.task_running(name):
            return False
        thread = threading.Thread(target=self._task_wrapper, args=(name, func), kwargs=kwargs, daemon=True)
        thread.start()
        return True

    def _task_wrapper(self, name: str, func: Callable[..., Dict], **kwargs) -> None:
        settings = load_settings()
        lock_path = os.path.join(settings['paths']['patch_dir'], f'{name}.lock')
        os.makedirs(os.path.dirname(lock_path), exist_ok=True)
        self.runtime.start_task(name, f'{name} started')
        def progress(value: int, message: str) -> None:
            self.runtime.update_task(name, value, message)
        try:
            with FileLock(lock_path, timeout=0):
                result = func(settings=settings, progress=progress, **kwargs)
                self.runtime.finish_task(name, result, result.get('message') or 'finished')
        except Timeout:
            self.runtime.fail_task(name, {'errors': [f'{name} is already running.']}, f'{name} is already running')
        except Exception as exc:
            log_error(settings['paths']['log_path'], f'{name} task failed', exc)
            self.runtime.fail_task(name, {'errors': [str(exc)]}, f'{name} failed')

    def _run_fetch(self, settings: Dict, progress: Callable[[int, str], None], force: bool = False) -> Dict:
        result = fetch_all(settings, force=force, progress=progress)
        result['message'] = 'Fetch complete' if not result['errors'] else 'Fetch complete with errors'
        return result

    def _run_compile(self, settings: Dict, progress: Callable[[int, str], None]) -> Dict:
        result = compile_patch(settings, progress=progress)
        result['message'] = 'Compile complete' if not result['errors'] else 'Compile complete with errors'
        return result

    def _run_patch(self, settings: Dict, progress: Callable[[int, str], None]) -> Dict:
        compiled_path = settings['paths']['compiled_patch']
        if not os.path.exists(compiled_path):
            raise FileNotFoundError('No compiled patch file exists. Compile a patch first.')
        with open(compiled_path, 'r', encoding='utf-8') as handle:
            patch = json.load(handle)

        result = {
            'started_at': now_iso(),
            'sonarr': None,
            'radarr': None,
            'errors': [],
        }

        if progress:
            progress(10, 'Preparing database patch')

        # Sonarr patch
        if patch.get('sonarr', {}).get('enabled') and not settings['products']['sonarr'].get('skip', False) and os.path.exists(settings['paths']['sonarr_db']):
            try:
                backup_path = os.path.join(os.path.dirname(settings['paths']['sonarr_db']), 'sonarr.db.bak')
                sqlite_backup(settings['paths']['sonarr_db'], backup_path)
                settings['last_actions']['last_backup']['sonarr'] = now_iso()
                sonarr_items = [item for item in patch['sonarr']['items'] if item.get('aliases_to_add')]
                sonarr_result = apply_sonarr_patch(settings['paths']['sonarr_db'], sonarr_items)
                sonarr_result['backup_path'] = backup_path
                result['sonarr'] = sonarr_result
                result['errors'].extend(sonarr_result.get('errors', []))
            except Exception as exc:
                log_error(settings['paths']['log_path'], 'Sonarr patch failed', exc)
                result['errors'].append(f'Sonarr patch failed: {exc}')
        if progress:
            progress(55, 'Sonarr patch stage complete')

        # Radarr patch
        if patch.get('radarr', {}).get('enabled') and not settings['products']['radarr'].get('skip', False) and os.path.exists(settings['paths']['radarr_db']):
            try:
                backup_path = os.path.join(os.path.dirname(settings['paths']['radarr_db']), 'radarr.db.bak')
                sqlite_backup(settings['paths']['radarr_db'], backup_path)
                settings['last_actions']['last_backup']['radarr'] = now_iso()
                radarr_items = [item for item in patch['radarr']['items'] if item.get('aliases_to_add')]
                alt_table = detect_radarr_alt_table(settings['paths']['radarr_db'])
                radarr_result = apply_radarr_patch(settings['paths']['radarr_db'], alt_table, radarr_items)
                radarr_result['backup_path'] = backup_path
                result['radarr'] = radarr_result
                result['errors'].extend(radarr_result.get('errors', []))
            except Exception as exc:
                log_error(settings['paths']['log_path'], 'Radarr patch failed', exc)
                result['errors'].append(f'Radarr patch failed: {exc}')
        if progress:
            progress(100, 'Patch complete')

        settings['last_actions']['last_patch'] = now_iso()
        save_settings(settings)
        result['message'] = 'Patch complete' if not result['errors'] else 'Patch complete with errors'
        return result

    def request_fetch(self, force: bool = False) -> bool:
        return self.start_background_task('fetch', self._run_fetch, force=force)

    def request_compile(self) -> bool:
        return self.start_background_task('compile', self._run_compile)

    def request_patch(self) -> bool:
        return self.start_background_task('patch', self._run_patch)
