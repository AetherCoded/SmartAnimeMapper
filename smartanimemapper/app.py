from __future__ import annotations

import atexit
import os
from pathlib import Path
from typing import Dict

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

from .config_store import allowed_browser_roots, load_settings, save_settings
from .db_ops import sqlite_backup
from .fetchers import can_fetch
from .jobs import JobManager
from .logging_utils import clear_log, read_log
from .state import RuntimeState
from .utils import format_file_mtime, format_local_datetime, now_iso

RUNTIME = RuntimeState()
JOB_MANAGER = JobManager(RUNTIME)
atexit.register(JOB_MANAGER.shutdown)


def _bool_from_form(name: str) -> bool:
    return request.form.get(name) in {'on', 'true', '1', 'yes'}



def _settings_from_form(settings: Dict) -> Dict:
    settings['products']['sonarr']['skip'] = _bool_from_form('sonarr_skip')
    settings['products']['radarr']['skip'] = _bool_from_form('radarr_skip')
    settings['paths']['sonarr_db'] = request.form.get('sonarr_db', settings['paths']['sonarr_db']).strip() or settings['paths']['sonarr_db']
    settings['paths']['radarr_db'] = request.form.get('radarr_db', settings['paths']['radarr_db']).strip() or settings['paths']['radarr_db']
    settings['paths']['log_path'] = request.form.get('log_path', settings['paths']['log_path']).strip() or settings['paths']['log_path']
    settings['paths']['data_dir'] = request.form.get('data_dir', settings['paths']['data_dir']).strip() or settings['paths']['data_dir']
    settings['paths']['patch_dir'] = request.form.get('patch_dir', settings['paths']['patch_dir']).strip() or settings['paths']['patch_dir']
    settings['paths']['anidb_titles_gz'] = request.form.get('anidb_titles_gz', settings['paths']['anidb_titles_gz']).strip() or settings['paths']['anidb_titles_gz']
    settings['paths']['anidb_titles_xml'] = request.form.get('anidb_titles_xml', settings['paths']['anidb_titles_xml']).strip() or settings['paths']['anidb_titles_xml']
    settings['paths']['kometa_mapping'] = request.form.get('kometa_mapping', settings['paths']['kometa_mapping']).strip() or settings['paths']['kometa_mapping']
    settings['paths']['compiled_patch'] = request.form.get('compiled_patch', settings['paths']['compiled_patch']).strip() or settings['paths']['compiled_patch']
    settings['fetch']['throttle_hours']['anidb_titles'] = int(request.form.get('anidb_throttle_hours', settings['fetch']['throttle_hours']['anidb_titles']) or 24)
    settings['fetch']['throttle_hours']['kometa_mapping'] = int(request.form.get('kometa_throttle_hours', settings['fetch']['throttle_hours']['kometa_mapping']) or 24)
    settings['fetch']['schedule']['enabled'] = _bool_from_form('schedule_enabled')
    settings['fetch']['schedule']['day_of_month'] = int(request.form.get('schedule_day_of_month', settings['fetch']['schedule']['day_of_month']) or 1)
    settings['fetch']['schedule']['hour'] = int(request.form.get('schedule_hour', settings['fetch']['schedule']['hour']) or 4)
    settings['fetch']['schedule']['minute'] = int(request.form.get('schedule_minute', settings['fetch']['schedule']['minute']) or 15)
    settings['advanced']['xem_probe_enabled'] = _bool_from_form('xem_probe_enabled')
    settings['advanced']['xem_probe_timeout_seconds'] = int(request.form.get('xem_probe_timeout_seconds', settings['advanced']['xem_probe_timeout_seconds']) or 10)
    return settings



def _build_dashboard_state(settings: Dict) -> Dict:
    runtime = RUNTIME.snapshot()
    sonarr_db = settings['paths']['sonarr_db']
    radarr_db = settings['paths']['radarr_db']
    sonarr_backup = os.path.join(os.path.dirname(sonarr_db), 'sonarr.db.bak') if sonarr_db else ''
    radarr_backup = os.path.join(os.path.dirname(radarr_db), 'radarr.db.bak') if radarr_db else ''
    log_path = settings['paths']['log_path']
    fetch_allowed = (
        can_fetch(settings['fetch']['last_fetch'].get('anidb_titles'), int(settings['fetch']['throttle_hours']['anidb_titles']))
        or can_fetch(settings['fetch']['last_fetch'].get('kometa_mapping'), int(settings['fetch']['throttle_hours']['kometa_mapping']))
    )
    return {
        'settings': settings,
        'runtime': runtime,
        'status': {
            'sonarr_backup_display': format_file_mtime(sonarr_backup) if not settings['products']['sonarr'].get('skip') else 'skipped',
            'radarr_backup_display': format_file_mtime(radarr_backup) if not settings['products']['radarr'].get('skip') else 'skipped',
            'anidb_fetch_display': format_local_datetime(settings['fetch']['last_fetch'].get('anidb_titles')),
            'kometa_fetch_display': format_local_datetime(settings['fetch']['last_fetch'].get('kometa_mapping')),
            'compiled_patch_display': format_file_mtime(settings['paths']['compiled_patch']),
            'log_exists': os.path.exists(log_path) and os.path.getsize(log_path) > 0,
            'log_path': log_path,
            'fetch_allowed_dashboard': fetch_allowed,
        },
    }



def _within_allowed(path: str) -> bool:
    if not path:
        return False
    abs_path = os.path.abspath(path)
    for root in allowed_browser_roots().values():
        try:
            common = os.path.commonpath([abs_path, root])
        except ValueError:
            continue
        if common == root:
            return True
    return False



def create_app() -> Flask:
    app = Flask(__name__)
    app.secret_key = os.environ.get('SMARTANIMEMAPPER_SECRET', 'smartanimemapper-dev-secret')

    @app.get('/')
    def index():
        settings = load_settings()
        if not settings.get('wizard_finished'):
            return render_template('splash.html', page='splash')
        return render_template('dashboard.html', page='dashboard', **_build_dashboard_state(settings))

    @app.route('/wizard', methods=['GET', 'POST'])
    def wizard():
        settings = load_settings()
        if request.method == 'POST':
            settings['products']['sonarr']['skip'] = _bool_from_form('sonarr_skip')
            settings['products']['radarr']['skip'] = _bool_from_form('radarr_skip')
            settings['paths']['sonarr_db'] = request.form.get('sonarr_db', settings['paths']['sonarr_db']).strip() or settings['paths']['sonarr_db']
            settings['paths']['radarr_db'] = request.form.get('radarr_db', settings['paths']['radarr_db']).strip() or settings['paths']['radarr_db']
            if settings['products']['sonarr']['skip'] and settings['products']['radarr']['skip']:
                flash('no databases configured, aborting to settings', 'error')
                save_settings(settings)
                return redirect(url_for('settings_view'))
            settings['wizard_finished'] = True
            save_settings(settings)
            flash('Wizard complete.', 'success')
            return redirect(url_for('index'))
        return render_template('wizard.html', page='wizard', settings=settings)

    @app.get('/settings')
    def settings_view():
        settings = load_settings()
        if not settings.get('wizard_finished'):
            settings['wizard_finished'] = True
            save_settings(settings)
        return render_template('settings.html', page='settings', settings=settings, dashboard=_build_dashboard_state(settings), log_text=read_log(settings['paths']['log_path']))

    @app.post('/settings/save')
    def settings_save():
        settings = load_settings()
        settings = _settings_from_form(settings)
        settings['wizard_finished'] = True
        save_settings(settings)
        flash('Settings saved.', 'success')
        return redirect(url_for('settings_view'))

    @app.get('/api/status')
    def api_status():
        return jsonify(_build_dashboard_state(load_settings()))

    @app.post('/api/backup')
    def api_backup():
        settings = load_settings()
        result = {'sonarr': None, 'radarr': None, 'errors': []}
        if not settings['products']['sonarr'].get('skip') and os.path.exists(settings['paths']['sonarr_db']):
            backup_path = os.path.join(os.path.dirname(settings['paths']['sonarr_db']), 'sonarr.db.bak')
            sqlite_backup(settings['paths']['sonarr_db'], backup_path)
            settings['last_actions']['last_backup']['sonarr'] = now_iso()
            result['sonarr'] = {'backup_path': backup_path}
        if not settings['products']['radarr'].get('skip') and os.path.exists(settings['paths']['radarr_db']):
            backup_path = os.path.join(os.path.dirname(settings['paths']['radarr_db']), 'radarr.db.bak')
            sqlite_backup(settings['paths']['radarr_db'], backup_path)
            settings['last_actions']['last_backup']['radarr'] = now_iso()
            result['radarr'] = {'backup_path': backup_path}
        save_settings(settings)
        RUNTIME.set_backup_result(result)
        return jsonify({'ok': True, 'result': result, 'state': _build_dashboard_state(settings)})

    @app.post('/api/fetch')
    def api_fetch():
        force = request.json.get('force', False) if request.is_json else False
        started = JOB_MANAGER.request_fetch(force=bool(force))
        return jsonify({'ok': started, 'message': 'fetch started' if started else 'fetch already running'})

    @app.post('/api/compile')
    def api_compile():
        started = JOB_MANAGER.request_compile()
        return jsonify({'ok': started, 'message': 'compile started' if started else 'compile already running'})

    @app.post('/api/patch')
    def api_patch():
        started = JOB_MANAGER.request_patch()
        return jsonify({'ok': started, 'message': 'patch started' if started else 'patch already running'})

    @app.get('/api/tasks')
    def api_tasks():
        return jsonify(RUNTIME.snapshot())

    @app.get('/api/logs')
    def api_logs():
        settings = load_settings()
        return jsonify({'log_path': settings['paths']['log_path'], 'text': read_log(settings['paths']['log_path'])})

    @app.post('/api/logs/clear')
    def api_logs_clear():
        settings = load_settings()
        clear_log(settings['paths']['log_path'])
        return jsonify({'ok': True})

    @app.get('/api/fs/list')
    def api_fs_list():
        requested = request.args.get('path', '').strip()
        roots = allowed_browser_roots()
        if not requested:
            return jsonify({'roots': [{'label': label, 'path': path} for label, path in roots.items()]})
        abs_path = os.path.abspath(requested)
        if not _within_allowed(abs_path):
            return jsonify({'error': 'Path is outside allowed roots.'}), 400
        if not os.path.exists(abs_path):
            return jsonify({'error': 'Path does not exist.'}), 404
        if os.path.isfile(abs_path):
            abs_path = os.path.dirname(abs_path)
        entries = []
        for entry in sorted(Path(abs_path).iterdir(), key=lambda p: (not p.is_dir(), p.name.lower())):
            if entry.is_dir() or entry.suffix.lower() == '.db' or entry.name.endswith('.bak'):
                entries.append({
                    'name': entry.name,
                    'path': str(entry),
                    'is_dir': entry.is_dir(),
                    'is_file': entry.is_file(),
                })
        parent = str(Path(abs_path).parent)
        if not _within_allowed(parent):
            parent = None
        return jsonify({'current': abs_path, 'parent': parent, 'entries': entries})

    return app


app = create_app()
