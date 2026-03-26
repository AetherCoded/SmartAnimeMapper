from __future__ import annotations

import json
import os
from copy import deepcopy
from typing import Any, Dict

CONFIG_DIR = os.environ.get('SMARTANIMEMAPPER_CONFIG_DIR', '/config')
SONARR_CONFIG_MOUNT = os.environ.get('SONARR_CONFIG_MOUNT', '/sonarr-config')
RADARR_CONFIG_MOUNT = os.environ.get('RADARR_CONFIG_MOUNT', '/radarr-config')

SETTINGS_PATH = os.path.join(CONFIG_DIR, 'settings.json')
RUNTIME_PATH = os.path.join(CONFIG_DIR, 'runtime.json')


def _default_settings() -> Dict[str, Any]:
    data_dir = os.path.join(CONFIG_DIR, 'data')
    patch_dir = os.path.join(CONFIG_DIR, 'patches')
    os.makedirs(CONFIG_DIR, exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(patch_dir, exist_ok=True)
    return {
        'wizard_finished': False,
        'paths': {
            'sonarr_db': os.path.join(SONARR_CONFIG_MOUNT, 'sonarr.db'),
            'radarr_db': os.path.join(RADARR_CONFIG_MOUNT, 'radarr.db'),
            'log_path': os.path.join(CONFIG_DIR, 'errors.log'),
            'data_dir': data_dir,
            'patch_dir': patch_dir,
            'anidb_titles_gz': os.path.join(data_dir, 'anime-titles.xml.gz'),
            'anidb_titles_xml': os.path.join(data_dir, 'anime-titles.xml'),
            'kometa_mapping': os.path.join(data_dir, 'anime_ids.json'),
            'compiled_patch': os.path.join(patch_dir, 'compiled_patch.json'),
        },
        'products': {
            'sonarr': {'skip': False},
            'radarr': {'skip': False},
        },
        'fetch': {
            'throttle_hours': {
                'anidb_titles': 24,
                'kometa_mapping': 24,
            },
            'last_fetch': {
                'anidb_titles': None,
                'kometa_mapping': None,
            },
            'schedule': {
                'enabled': True,
                'day_of_month': 1,
                'hour': 4,
                'minute': 15,
            },
        },
        'advanced': {
            'xem_probe_enabled': False,
            'xem_probe_timeout_seconds': 10,
        },
        'last_actions': {
            'last_compile': None,
            'last_patch': None,
            'last_backup': {
                'sonarr': None,
                'radarr': None,
            },
        },
    }


def _default_runtime() -> Dict[str, Any]:
    return {
        'tasks': {
            'fetch': {'running': False, 'progress': 0, 'message': 'idle', 'started_at': None, 'finished_at': None},
            'compile': {'running': False, 'progress': 0, 'message': 'idle', 'started_at': None, 'finished_at': None},
            'patch': {'running': False, 'progress': 0, 'message': 'idle', 'started_at': None, 'finished_at': None},
        },
        'results': {
            'fetch': None,
            'compile': None,
            'patch': None,
            'backup': None,
        },
    }


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
    return base


DEFAULT_SETTINGS = _default_settings()
DEFAULT_RUNTIME = _default_runtime()


def ensure_directories(settings: Dict[str, Any]) -> None:
    paths = settings['paths']
    for key in ('data_dir', 'patch_dir'):
        os.makedirs(paths[key], exist_ok=True)
    log_dir = os.path.dirname(paths['log_path']) or CONFIG_DIR
    os.makedirs(log_dir, exist_ok=True)



def load_settings() -> Dict[str, Any]:
    data = deepcopy(DEFAULT_SETTINGS)
    if os.path.exists(SETTINGS_PATH):
        try:
            with open(SETTINGS_PATH, 'r', encoding='utf-8') as handle:
                loaded = json.load(handle)
            data = _deep_merge(data, loaded)
        except Exception:
            # Fall back to defaults and let the app continue.
            pass
    ensure_directories(data)
    return data



def save_settings(settings: Dict[str, Any]) -> None:
    ensure_directories(settings)
    with open(SETTINGS_PATH, 'w', encoding='utf-8') as handle:
        json.dump(settings, handle, ensure_ascii=False, indent=2)



def load_runtime() -> Dict[str, Any]:
    data = deepcopy(DEFAULT_RUNTIME)
    if os.path.exists(RUNTIME_PATH):
        try:
            with open(RUNTIME_PATH, 'r', encoding='utf-8') as handle:
                loaded = json.load(handle)
            data = _deep_merge(data, loaded)
        except Exception:
            pass
    return data



def save_runtime(runtime: Dict[str, Any]) -> None:
    with open(RUNTIME_PATH, 'w', encoding='utf-8') as handle:
        json.dump(runtime, handle, ensure_ascii=False, indent=2)



def allowed_browser_roots() -> Dict[str, str]:
    roots = {
        '/config': CONFIG_DIR,
        '/sonarr-config': SONARR_CONFIG_MOUNT,
        '/radarr-config': RADARR_CONFIG_MOUNT,
    }
    cleaned: Dict[str, str] = {}
    for label, path in roots.items():
        if path and os.path.exists(path):
            cleaned[label] = os.path.abspath(path)
    return cleaned
