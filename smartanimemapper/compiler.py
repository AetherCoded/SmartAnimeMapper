from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ET
from difflib import SequenceMatcher
from typing import Callable, Dict, Iterable, List, Optional, Set

import requests

from .config_store import save_settings
from .db_ops import (
    detect_radarr_alt_table,
    get_radarr_existing_aliases,
    get_radarr_movie_rows,
    get_sonarr_existing_aliases,
    get_sonarr_series_rows,
    resolve_radarr_movie_key,
)
from .logging_utils import log_error
from .utils import clean_title, normalize_alias, now_iso

ProgressCallback = Callable[[int, str], None]
XML_LANG = '{http://www.w3.org/XML/1998/namespace}lang'


def _safe_int(value) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None



def build_kometa_indexes(kometa_payload: Dict) -> Dict[str, Dict]:
    tvdb_to_aids: Dict[int, List[int]] = {}
    tmdb_movie_to_aids: Dict[int, List[int]] = {}
    imdb_to_aids: Dict[str, List[int]] = {}

    for aid_str, payload in kometa_payload.items():
        aid = _safe_int(aid_str)
        if not aid:
            continue
        tvdb_id = _safe_int(payload.get('tvdb_id'))
        tmdb_movie_id = _safe_int(payload.get('tmdb_movie_id'))
        imdb_ids = str(payload.get('imdb_id') or '').strip()
        if tvdb_id:
            tvdb_to_aids.setdefault(tvdb_id, []).append(aid)
        if tmdb_movie_id:
            tmdb_movie_to_aids.setdefault(tmdb_movie_id, []).append(aid)
        if imdb_ids:
            for imdb_id in [part.strip() for part in imdb_ids.split(',') if part.strip()]:
                imdb_to_aids.setdefault(imdb_id, []).append(aid)

    for mapping in (tvdb_to_aids, tmdb_movie_to_aids, imdb_to_aids):
        for key, value in mapping.items():
            mapping[key] = sorted(set(value))
    return {
        'tvdb_to_aids': tvdb_to_aids,
        'tmdb_movie_to_aids': tmdb_movie_to_aids,
        'imdb_to_aids': imdb_to_aids,
    }



def parse_anidb_titles(xml_path: str, needed_aids: Set[int], progress: Optional[ProgressCallback] = None) -> Dict[int, List[Dict]]:
    titles_by_aid: Dict[int, List[Dict]] = {aid: [] for aid in needed_aids}
    if not needed_aids:
        return titles_by_aid

    processed = 0
    every = max(1, len(needed_aids) // 25)
    for _event, elem in ET.iterparse(xml_path, events=('end',)):
        if elem.tag != 'anime':
            continue
        aid = _safe_int(elem.attrib.get('aid'))
        if aid in needed_aids:
            rows = []
            for title_node in elem.findall('title'):
                text = (title_node.text or '').strip()
                if not text:
                    continue
                rows.append({
                    'lang': title_node.attrib.get(XML_LANG) or title_node.attrib.get('lang') or '',
                    'type': title_node.attrib.get('type') or '',
                    'title': text,
                })
            titles_by_aid[aid] = rows
            processed += 1
            if progress and processed % every == 0:
                progress(55 + int((processed / max(1, len(needed_aids))) * 10), f'Parsing AniDB titles ({processed}/{len(needed_aids)})')
        elem.clear()
    return titles_by_aid



def best_aid_for_title(title: str, candidate_aids: Iterable[int], titles_by_aid: Dict[int, List[Dict]]) -> Optional[int]:
    candidates = list(candidate_aids)
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    needle = title.strip().lower()
    best_score = -1.0
    best_aid = None
    for aid in candidates:
        choices = [row['title'] for row in titles_by_aid.get(aid, []) if row.get('lang') == 'en']
        if not choices:
            choices = [row['title'] for row in titles_by_aid.get(aid, [])]
        if not choices:
            continue
        score = max(SequenceMatcher(None, needle, choice.lower()).ratio() for choice in choices)
        if score > best_score:
            best_score = score
            best_aid = aid
    return best_aid or candidates[0]



def select_aliases(titles_for_aid: List[Dict], primary_title: str, max_aliases: int = 6) -> List[str]:
    def pick(lang: str, types: List[str]) -> Optional[str]:
        for tp in types:
            for row in titles_for_aid:
                if row.get('lang') == lang and row.get('type') == tp:
                    return row['title']
        for row in titles_for_aid:
            if row.get('lang') == lang:
                return row['title']
        return None

    candidates: List[str] = []
    ordered = [
        pick('x-jat', ['main', 'official', 'short']),
        pick('ja', ['official', 'main', 'short']),
        pick('en', ['official', 'main', 'short']),
    ]
    for item in ordered:
        if item:
            candidates.append(item)

    if len(candidates) < max_aliases:
        for row in titles_for_aid:
            if row['title'] not in candidates and row.get('lang') in {'x-jat', 'ja', 'en'}:
                candidates.append(row['title'])
                if len(candidates) >= max_aliases:
                    break

    seen = {normalize_alias(primary_title)}
    aliases: List[str] = []
    for candidate in candidates:
        normalized = normalize_alias(candidate)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        aliases.append(candidate)
        if len(aliases) >= max_aliases:
            break
    return aliases



def probe_xem_tvdb(tvdb_id: int, cache_dir: str, timeout_seconds: int = 10) -> Dict:
    os.makedirs(cache_dir, exist_ok=True)
    cache_path = os.path.join(cache_dir, f'{tvdb_id}.json')
    if os.path.exists(cache_path):
        try:
            with open(cache_path, 'r', encoding='utf-8') as handle:
                return json.load(handle)
        except Exception:
            pass
    url = f'https://thexem.info/map/all?id={tvdb_id}&origin=tvdb'
    response = requests.get(url, timeout=timeout_seconds)
    response.raise_for_status()
    payload = response.json()
    with open(cache_path, 'w', encoding='utf-8') as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return payload



def _xem_has_any_anidb_mapping(payload: Dict) -> bool:
    def walk(value) -> bool:
        if isinstance(value, dict):
            if 'anidb' in value:
                return True
            return any(walk(v) for v in value.values())
        if isinstance(value, list):
            return any(walk(v) for v in value)
        return False
    return walk(payload)



def compile_patch(settings: Dict, progress: Optional[ProgressCallback] = None) -> Dict:
    paths = settings['paths']
    log_path = paths['log_path']
    compiled_patch_path = paths['compiled_patch']

    if progress:
        progress(5, 'Loading data sources')

    if not os.path.exists(paths['anidb_titles_xml']) or not os.path.exists(paths['kometa_mapping']):
        raise FileNotFoundError('Required source files are missing. Use Fetch Now first.')

    with open(paths['kometa_mapping'], 'r', encoding='utf-8') as handle:
        kometa_payload = json.load(handle)
    indexes = build_kometa_indexes(kometa_payload)

    if progress:
        progress(15, 'Reading configured databases')

    sonarr_enabled = not settings['products']['sonarr'].get('skip', False) and os.path.exists(paths['sonarr_db'])
    radarr_enabled = not settings['products']['radarr'].get('skip', False) and os.path.exists(paths['radarr_db'])

    sonarr_rows = get_sonarr_series_rows(paths['sonarr_db']) if sonarr_enabled else []
    radarr_rows = get_radarr_movie_rows(paths['radarr_db']) if radarr_enabled else []
    sonarr_existing = get_sonarr_existing_aliases(paths['sonarr_db']) if sonarr_enabled else {}
    radarr_alt_table = detect_radarr_alt_table(paths['radarr_db']) if radarr_enabled else None
    radarr_existing = {}

    needed_aids: Set[int] = set()
    for row in sonarr_rows:
        needed_aids.update(indexes['tvdb_to_aids'].get(row['tvdb_id'], []))
    for row in radarr_rows:
        if row.get('tmdb_id'):
            needed_aids.update(indexes['tmdb_movie_to_aids'].get(row['tmdb_id'], []))
        if row.get('imdb_id'):
            needed_aids.update(indexes['imdb_to_aids'].get(row['imdb_id'], []))

    if progress:
        progress(35, f'Parsing AniDB title cache for {len(needed_aids)} candidate IDs')

    titles_by_aid = parse_anidb_titles(paths['anidb_titles_xml'], needed_aids, progress=progress)

    if radarr_enabled and radarr_alt_table:
        movie_key_lookup = {}
        for row in radarr_rows:
            movie_key = resolve_radarr_movie_key(row, radarr_alt_table)
            if movie_key is not None:
                movie_key_lookup[row['movie_id']] = movie_key
        radarr_existing = get_radarr_existing_aliases(paths['radarr_db'], radarr_alt_table, movie_key_lookup)

    result = {
        'compiled_at': now_iso(),
        'source_files': {
            'anidb_titles_xml': paths['anidb_titles_xml'],
            'kometa_mapping': paths['kometa_mapping'],
        },
        'sonarr': {
            'enabled': sonarr_enabled,
            'db_path': paths['sonarr_db'],
            'items': [],
            'counts': {'series_total': len(sonarr_rows), 'series_matched': 0, 'aliases_existing': 0, 'aliases_to_add': 0},
        },
        'radarr': {
            'enabled': radarr_enabled,
            'db_path': paths['radarr_db'],
            'alt_table': radarr_alt_table['table'] if radarr_alt_table else None,
            'items': [],
            'counts': {'movies_total': len(radarr_rows), 'movies_matched': 0, 'aliases_existing': 0, 'aliases_to_add': 0},
        },
        'errors': [],
    }

    xem_enabled = settings.get('advanced', {}).get('xem_probe_enabled', False)
    xem_timeout = int(settings.get('advanced', {}).get('xem_probe_timeout_seconds', 10))
    xem_cache_dir = os.path.join(paths['data_dir'], 'xem-cache')

    if progress:
        progress(70, 'Compiling Sonarr patch preview')

    sonarr_total = max(1, len(sonarr_rows))
    for idx, row in enumerate(sonarr_rows, start=1):
        candidate_aids = indexes['tvdb_to_aids'].get(row['tvdb_id'], [])
        if not candidate_aids:
            continue
        aid = best_aid_for_title(row['title'], candidate_aids, titles_by_aid)
        if not aid:
            continue
        aliases = select_aliases(titles_by_aid.get(aid, []), row['title'])
        existing_norm = sonarr_existing.get(row['tvdb_id'], set())
        aliases_existing = [alias for alias in aliases if normalize_alias(alias) in existing_norm]
        aliases_to_add = [alias for alias in aliases if normalize_alias(alias) not in existing_norm]
        xem_info = None
        if xem_enabled:
            try:
                payload = probe_xem_tvdb(row['tvdb_id'], xem_cache_dir, timeout_seconds=xem_timeout)
                xem_info = {'probe_ok': True, 'has_any_anidb_mapping': _xem_has_any_anidb_mapping(payload)}
            except Exception as exc:
                xem_info = {'probe_ok': False, 'error': str(exc)}
                log_error(log_path, f'XEM probe failed for TVDB {row["tvdb_id"]}', exc)
        result['sonarr']['items'].append({
            'series_title': row['title'],
            'tvdb_id': row['tvdb_id'],
            'anidb_id': aid,
            'aliases_existing': aliases_existing,
            'aliases_to_add': aliases_to_add,
            'existing_scene_alias_count': len(existing_norm),
            'xem_probe': xem_info,
        })
        result['sonarr']['counts']['series_matched'] += 1
        result['sonarr']['counts']['aliases_existing'] += len(aliases_existing)
        result['sonarr']['counts']['aliases_to_add'] += len(aliases_to_add)
        if progress and idx % max(1, sonarr_total // 20) == 0:
            progress(70 + int((idx / sonarr_total) * 10), f'Compiling Sonarr patch ({idx}/{sonarr_total})')

    if progress:
        progress(82, 'Compiling Radarr patch preview')

    radarr_total = max(1, len(radarr_rows))
    for idx, row in enumerate(radarr_rows, start=1):
        candidate_aids = []
        if row.get('tmdb_id'):
            candidate_aids.extend(indexes['tmdb_movie_to_aids'].get(row['tmdb_id'], []))
        if row.get('imdb_id'):
            candidate_aids.extend(indexes['imdb_to_aids'].get(row['imdb_id'], []))
        candidate_aids = sorted(set(candidate_aids))
        if not candidate_aids:
            continue
        aid = best_aid_for_title(row['title'], candidate_aids, titles_by_aid)
        if not aid:
            continue
        aliases = select_aliases(titles_by_aid.get(aid, []), row['title'])
        movie_key = resolve_radarr_movie_key(row, radarr_alt_table) if radarr_alt_table else None
        existing_norm = radarr_existing.get(movie_key, set()) if movie_key is not None else set()
        aliases_existing = [alias for alias in aliases if normalize_alias(alias) in existing_norm]
        aliases_to_add = [alias for alias in aliases if normalize_alias(alias) not in existing_norm]
        result['radarr']['items'].append({
            'movie_title': row['title'],
            'movie_id': row['movie_id'],
            'movie_key': movie_key,
            'tmdb_id': row.get('tmdb_id'),
            'imdb_id': row.get('imdb_id'),
            'anidb_id': aid,
            'aliases_existing': aliases_existing,
            'aliases_to_add': aliases_to_add,
        })
        result['radarr']['counts']['movies_matched'] += 1
        result['radarr']['counts']['aliases_existing'] += len(aliases_existing)
        result['radarr']['counts']['aliases_to_add'] += len(aliases_to_add)
        if progress and idx % max(1, radarr_total // 20) == 0:
            progress(82 + int((idx / radarr_total) * 13), f'Compiling Radarr patch ({idx}/{radarr_total})')

    os.makedirs(os.path.dirname(compiled_patch_path), exist_ok=True)
    with open(compiled_patch_path, 'w', encoding='utf-8') as handle:
        json.dump(result, handle, ensure_ascii=False, indent=2)

    settings['last_actions']['last_compile'] = result['compiled_at']
    save_settings(settings)

    if progress:
        progress(100, 'Patch preview compiled')
    return result
