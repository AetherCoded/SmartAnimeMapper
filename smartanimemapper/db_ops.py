from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from .utils import clean_title, normalize_alias



def connect_ro(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(f'file:{db_path}?mode=ro', uri=True, timeout=60)
    conn.row_factory = sqlite3.Row
    return conn



def connect_rw(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=60)
    conn.row_factory = sqlite3.Row
    return conn



def sqlite_backup(db_path: str, backup_path: str) -> str:
    os.makedirs(os.path.dirname(backup_path) or '.', exist_ok=True)
    if os.path.exists(backup_path):
        os.remove(backup_path)
    source = connect_ro(db_path)
    dest = sqlite3.connect(backup_path)
    try:
        source.backup(dest)
    finally:
        source.close()
        dest.close()
    return backup_path



def list_tables(conn: sqlite3.Connection) -> List[str]:
    cur = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [row[0] for row in cur.fetchall()]



def table_columns(conn: sqlite3.Connection, table_name: str) -> List[Dict]:
    cur = conn.execute(f'PRAGMA table_info({table_name})')
    out = []
    for cid, name, ctype, notnull, dflt_value, pk in cur.fetchall():
        out.append({
            'cid': cid,
            'name': name,
            'type': ctype,
            'notnull': bool(notnull),
            'default': dflt_value,
            'pk': bool(pk),
        })
    return out



def get_sonarr_series_rows(db_path: str) -> List[Dict]:
    conn = connect_ro(db_path)
    try:
        cols = {c['name'] for c in table_columns(conn, 'Series')}
        tvdb_col = 'TvdbId' if 'TvdbId' in cols else None
        title_col = 'Title' if 'Title' in cols else ('SortTitle' if 'SortTitle' in cols else None)
        if not tvdb_col or not title_col:
            return []
        cur = conn.execute(f'SELECT Id, {tvdb_col} as TvdbId, {title_col} as Title FROM Series')
        rows = []
        for row in cur.fetchall():
            try:
                tvdb_id = int(row['TvdbId'])
            except Exception:
                continue
            if tvdb_id <= 0:
                continue
            rows.append({
                'id': int(row['Id']),
                'tvdb_id': tvdb_id,
                'title': (row['Title'] or '').strip(),
            })
        return rows
    finally:
        conn.close()



def get_sonarr_existing_aliases(db_path: str) -> Dict[int, set]:
    conn = connect_ro(db_path)
    try:
        tables = set(list_tables(conn))
        if 'SceneMappings' not in tables:
            return {}
        cols = {c['name'] for c in table_columns(conn, 'SceneMappings')}
        if 'TvdbId' not in cols:
            return {}
        alias_col = 'SearchTerm' if 'SearchTerm' in cols else ('Title' if 'Title' in cols else None)
        if not alias_col:
            return {}
        cur = conn.execute(f'SELECT TvdbId, {alias_col} as Alias FROM SceneMappings')
        out: Dict[int, set] = {}
        for row in cur.fetchall():
            try:
                tvdb_id = int(row['TvdbId'])
            except Exception:
                continue
            alias = (row['Alias'] or '').strip()
            if not alias:
                continue
            out.setdefault(tvdb_id, set()).add(normalize_alias(alias))
        return out
    finally:
        conn.close()



def apply_sonarr_patch(db_path: str, items: List[Dict]) -> Dict:
    conn = connect_rw(db_path)
    result = {'inserted': 0, 'skipped': 0, 'errors': []}
    try:
        cols = table_columns(conn, 'SceneMappings')
        colnames = {c['name'] for c in cols}
        has_type = 'Type' in colnames
        has_comment = 'Comment' in colnames
        existing = get_sonarr_existing_aliases(db_path)

        for item in items:
            tvdb_id = item['tvdb_id']
            for alias in item['aliases_to_add']:
                if normalize_alias(alias) in existing.get(tvdb_id, set()):
                    result['skipped'] += 1
                    continue
                values: Dict[str, object] = {}
                def set_if(col: str, val: object) -> None:
                    if col in colnames:
                        values[col] = val
                set_if('TvdbId', tvdb_id)
                set_if('SeasonNumber', -1)
                set_if('SearchTerm', alias)
                set_if('ParseTerm', clean_title(alias))
                set_if('Title', alias)
                set_if('Type', 'Custom')
                set_if('Comment', 'SmartAnimeMapper')
                set_if('FilterRegex', None)
                set_if('SceneSeasonNumber', None)
                set_if('SearchMode', None)
                missing_required = [c['name'] for c in cols if not c['pk'] and c['notnull'] and c['default'] is None and c['name'] not in values]
                if missing_required:
                    result['errors'].append(f"Sonarr schema has unsupported required columns: {', '.join(missing_required)}")
                    continue
                insert_cols = list(values.keys())
                placeholders = ','.join(['?'] * len(insert_cols))
                sql = f"INSERT INTO SceneMappings ({', '.join(insert_cols)}) VALUES ({placeholders})"
                conn.execute(sql, [values[c] for c in insert_cols])
                existing.setdefault(tvdb_id, set()).add(normalize_alias(alias))
                result['inserted'] += 1
        conn.commit()
        return result
    finally:
        conn.close()



def get_radarr_movie_rows(db_path: str) -> List[Dict]:
    conn = connect_ro(db_path)
    try:
        tables = set(list_tables(conn))
        if 'Movies' not in tables:
            return []
        cols = {c['name'] for c in table_columns(conn, 'Movies')}
        if 'Id' not in cols or 'Title' not in cols:
            return []
        tmdb_col = 'TmdbId' if 'TmdbId' in cols else ('TMDbId' if 'TMDbId' in cols else None)
        imdb_col = 'ImdbId' if 'ImdbId' in cols else ('IMDbId' if 'IMDbId' in cols else None)
        metadata_col = 'MovieMetadataId' if 'MovieMetadataId' in cols else None
        selected = ['Id', 'Title']
        if tmdb_col:
            selected.append(f'{tmdb_col} as TmdbId')
        if imdb_col:
            selected.append(f'{imdb_col} as ImdbId')
        if metadata_col:
            selected.append(f'{metadata_col} as MovieMetadataId')
        cur = conn.execute(f"SELECT {', '.join(selected)} FROM Movies")
        rows = []
        for row in cur.fetchall():
            tmdb_id = row['TmdbId'] if 'TmdbId' in row.keys() else None
            try:
                tmdb_id = int(tmdb_id) if tmdb_id not in (None, '') else None
            except Exception:
                tmdb_id = None
            rows.append({
                'movie_id': int(row['Id']),
                'title': (row['Title'] or '').strip(),
                'tmdb_id': tmdb_id,
                'imdb_id': (row['ImdbId'] or '').strip() if 'ImdbId' in row.keys() else None,
                'movie_metadata_id': int(row['MovieMetadataId']) if 'MovieMetadataId' in row.keys() and row['MovieMetadataId'] not in (None, '') else None,
            })
        if any(r['movie_metadata_id'] is None for r in rows):
            metadata_map = _load_movie_metadata_map(conn)
            for row in rows:
                if row['movie_metadata_id'] is None and row['tmdb_id'] in metadata_map:
                    row['movie_metadata_id'] = metadata_map[row['tmdb_id']]
        return rows
    finally:
        conn.close()



def _load_movie_metadata_map(conn: sqlite3.Connection) -> Dict[int, int]:
    tables = set(list_tables(conn))
    if 'MovieMetadata' not in tables:
        return {}
    cols = {c['name'] for c in table_columns(conn, 'MovieMetadata')}
    if 'Id' not in cols:
        return {}
    tmdb_col = 'TmdbId' if 'TmdbId' in cols else ('TMDbId' if 'TMDbId' in cols else None)
    if not tmdb_col:
        return {}
    cur = conn.execute(f'SELECT Id, {tmdb_col} as TmdbId FROM MovieMetadata')
    out: Dict[int, int] = {}
    for row in cur.fetchall():
        try:
            tmdb_id = int(row['TmdbId'])
            meta_id = int(row['Id'])
        except Exception:
            continue
        out[tmdb_id] = meta_id
    return out



def detect_radarr_alt_table(db_path: str) -> Optional[Dict]:
    conn = connect_ro(db_path)
    try:
        candidates = []
        for table in list_tables(conn):
            lowered = table.lower()
            if 'title' in lowered and ('alt' in lowered or 'alternative' in lowered):
                cols = table_columns(conn, table)
                colnames = {c['name'] for c in cols}
                title_col = next((n for n in ['Title', 'SourceTitle', 'AlternateTitle'] if n in colnames), None)
                key_col = next((n for n in ['MovieMetadataId', 'MovieId', 'TmdbId', 'TMDbId'] if n in colnames), None)
                clean_col = next((n for n in ['CleanTitle', 'CleanSourceTitle'] if n in colnames), None)
                if title_col and key_col:
                    candidates.append({
                        'table': table,
                        'cols': cols,
                        'title_col': title_col,
                        'key_col': key_col,
                        'clean_col': clean_col,
                    })
        if not candidates:
            return None
        # Prefer the most specific/common shape.
        candidates.sort(key=lambda c: (0 if c['key_col'] == 'MovieMetadataId' else 1, 0 if c['clean_col'] else 1, c['table']))
        return candidates[0]
    finally:
        conn.close()



def get_radarr_existing_aliases(db_path: str, alt_table: Optional[Dict], movie_key_lookup: Dict[int, int]) -> Dict[int, set]:
    if not alt_table:
        return {}
    conn = connect_ro(db_path)
    try:
        key_col = alt_table['key_col']
        title_col = alt_table['title_col']
        cur = conn.execute(f"SELECT {key_col} as K, {title_col} as T FROM {alt_table['table']}")
        out: Dict[int, set] = {}
        for row in cur.fetchall():
            try:
                key = int(row['K'])
            except Exception:
                continue
            title = (row['T'] or '').strip()
            if not title:
                continue
            out.setdefault(key, set()).add(normalize_alias(title))
        return out
    finally:
        conn.close()



def resolve_radarr_movie_key(movie_row: Dict, alt_table: Dict) -> Optional[int]:
    key_col = alt_table['key_col']
    if key_col == 'MovieId':
        return movie_row.get('movie_id')
    if key_col == 'MovieMetadataId':
        return movie_row.get('movie_metadata_id')
    if key_col in ('TmdbId', 'TMDbId'):
        return movie_row.get('tmdb_id')
    return None



def _fetch_template_row(conn: sqlite3.Connection, alt_table: Dict, movie_key: Optional[int]) -> Optional[sqlite3.Row]:
    table = alt_table['table']
    key_col = alt_table['key_col']
    if movie_key is not None:
        cur = conn.execute(f'SELECT * FROM {table} WHERE {key_col} = ? LIMIT 1', (movie_key,))
        row = cur.fetchone()
        if row is not None:
            return row
    cur = conn.execute(f'SELECT * FROM {table} LIMIT 1')
    return cur.fetchone()



def apply_radarr_patch(db_path: str, alt_table: Optional[Dict], items: List[Dict]) -> Dict:
    result = {'inserted': 0, 'skipped': 0, 'errors': []}
    if not alt_table:
        result['errors'].append('No recognizable Radarr alternative title table detected.')
        return result

    conn = connect_rw(db_path)
    try:
        cols = alt_table['cols']
        colnames = {c['name'] for c in cols}
        key_col = alt_table['key_col']
        title_col = alt_table['title_col']
        clean_col = alt_table['clean_col']

        # Snapshot existing aliases.
        cur = conn.execute(f'SELECT {key_col} as K, {title_col} as T FROM {alt_table["table"]}')
        existing: Dict[int, set] = {}
        for row in cur.fetchall():
            try:
                key = int(row['K'])
            except Exception:
                continue
            title = (row['T'] or '').strip()
            if title:
                existing.setdefault(key, set()).add(normalize_alias(title))

        for item in items:
            movie_key = item.get('movie_key')
            if movie_key is None:
                result['errors'].append(f"Movie '{item.get('movie_title', 'unknown')}' has no usable key for Radarr alt-title table.")
                continue
            template = _fetch_template_row(conn, alt_table, movie_key)
            template_map = dict(template) if template is not None else {}
            for alias in item['aliases_to_add']:
                if normalize_alias(alias) in existing.get(movie_key, set()):
                    result['skipped'] += 1
                    continue
                values: Dict[str, object] = {}
                unsupported: List[str] = []
                for col in cols:
                    name = col['name']
                    if col['pk']:
                        continue
                    if name == key_col:
                        values[name] = movie_key
                    elif name == title_col:
                        values[name] = alias
                    elif clean_col and name == clean_col:
                        values[name] = clean_title(alias)
                    elif name in template_map:
                        values[name] = template_map[name]
                    elif name.lower() == 'comment':
                        values[name] = 'SmartAnimeMapper'
                    elif 'date' in name.lower() or name.lower().endswith('at'):
                        values[name] = datetime.now(timezone.utc).isoformat(timespec='seconds')
                    elif not col['notnull'] or col['default'] is not None:
                        continue
                    else:
                        unsupported.append(name)
                if unsupported:
                    result['errors'].append(
                        f"Radarr alt-title schema for table {alt_table['table']} has unsupported required columns: {', '.join(sorted(unsupported))}"
                    )
                    continue
                insert_cols = list(values.keys())
                placeholders = ','.join(['?'] * len(insert_cols))
                sql = f"INSERT INTO {alt_table['table']} ({', '.join(insert_cols)}) VALUES ({placeholders})"
                try:
                    conn.execute(sql, [values[c] for c in insert_cols])
                    existing.setdefault(movie_key, set()).add(normalize_alias(alias))
                    result['inserted'] += 1
                except Exception as exc:
                    result['errors'].append(f"Failed inserting Radarr alias '{alias}' for key {movie_key}: {exc}")
        conn.commit()
        return result
    finally:
        conn.close()
