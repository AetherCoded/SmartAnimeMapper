from __future__ import annotations

import gzip
import os
import shutil
import tempfile
from datetime import datetime, timezone
from typing import Callable, Dict, Optional

import requests

from .config_store import save_settings
from .logging_utils import log_error
from .utils import now_iso, parse_iso

ANIDB_TITLES_URL = 'http://anidb.net/api/anime-titles.xml.gz'
KOMETA_MAPPING_URL = 'https://raw.githubusercontent.com/Kometa-Team/Anime-IDs/master/anime_ids.json'

ProgressCallback = Callable[[int, str], None]


def _download(url: str, dest_path: str, user_agent: str) -> None:
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    headers = {
        'User-Agent': user_agent,
        'Accept': '*/*',
    }
    with requests.get(url, headers=headers, timeout=60, stream=True) as response:
        response.raise_for_status()
        with tempfile.NamedTemporaryFile('wb', delete=False, dir=os.path.dirname(dest_path)) as tmp:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)
            tmp_path = tmp.name
    os.replace(tmp_path, dest_path)



def _gunzip(gz_path: str, out_path: str) -> None:
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with gzip.open(gz_path, 'rb') as src, tempfile.NamedTemporaryFile('wb', delete=False, dir=os.path.dirname(out_path)) as tmp:
        shutil.copyfileobj(src, tmp)
        tmp_path = tmp.name
    os.replace(tmp_path, out_path)



def can_fetch(last_fetch_iso: Optional[str], throttle_hours: int, force: bool = False) -> bool:
    if force:
        return True
    if not last_fetch_iso:
        return True
    last_fetch = parse_iso(last_fetch_iso)
    if not last_fetch:
        return True
    elapsed = datetime.now(timezone.utc) - last_fetch.astimezone(timezone.utc)
    return elapsed.total_seconds() >= throttle_hours * 3600



def monthly_fetch_due(last_fetch_iso: Optional[str], day_of_month: int) -> bool:
    if not last_fetch_iso:
        return True
    last_fetch = parse_iso(last_fetch_iso)
    if not last_fetch:
        return True
    now = datetime.now(timezone.utc)
    if last_fetch.year == now.year and last_fetch.month == now.month:
        return False
    if now.day < max(1, min(day_of_month, 28)):
        return False
    return True



def fetch_all(settings: Dict, force: bool = False, progress: Optional[ProgressCallback] = None) -> Dict:
    paths = settings['paths']
    fetch_settings = settings['fetch']
    throttles = fetch_settings['throttle_hours']
    last_fetch = fetch_settings['last_fetch']
    log_path = paths['log_path']
    user_agent = 'SmartAnimeMapper/0.1 (+local self-hosted utility)'

    result = {
        'anidb_titles': {'downloaded': False, 'path': paths['anidb_titles_gz']},
        'kometa_mapping': {'downloaded': False, 'path': paths['kometa_mapping']},
        'errors': [],
    }

    if progress:
        progress(5, 'Starting source fetch')

    try:
        if force or can_fetch(last_fetch.get('anidb_titles'), int(throttles['anidb_titles']), force=force):
            _download(ANIDB_TITLES_URL, paths['anidb_titles_gz'], user_agent)
            _gunzip(paths['anidb_titles_gz'], paths['anidb_titles_xml'])
            settings['fetch']['last_fetch']['anidb_titles'] = now_iso()
            result['anidb_titles']['downloaded'] = True
        elif os.path.exists(paths['anidb_titles_gz']) and (not os.path.exists(paths['anidb_titles_xml']) or os.path.getmtime(paths['anidb_titles_gz']) > os.path.getmtime(paths['anidb_titles_xml'])):
            _gunzip(paths['anidb_titles_gz'], paths['anidb_titles_xml'])
    except Exception as exc:
        result['errors'].append(f'AniDB titles fetch failed: {exc}')
        log_error(log_path, 'AniDB titles fetch failed', exc)

    if progress:
        progress(55, 'Fetching Kometa mapping')

    try:
        if force or can_fetch(last_fetch.get('kometa_mapping'), int(throttles['kometa_mapping']), force=force):
            _download(KOMETA_MAPPING_URL, paths['kometa_mapping'], user_agent)
            settings['fetch']['last_fetch']['kometa_mapping'] = now_iso()
            result['kometa_mapping']['downloaded'] = True
    except Exception as exc:
        result['errors'].append(f'Kometa mapping fetch failed: {exc}')
        log_error(log_path, 'Kometa mapping fetch failed', exc)

    save_settings(settings)
    if progress:
        progress(100, 'Fetch finished')
    return result
