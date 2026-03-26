# SmartAnimeMapper

SmartAnimeMapper is a small self-hosted web app that:

- stores all app state under `/config`
- fetches AniDB titles and Kometa anime ID mappings on a monthly schedule
- lets you force fetch manually from the UI
- creates safe SQLite backups for Sonarr and Radarr
- compiles a preview patch file before modifying anything
- patches Sonarr and Radarr databases only when you explicitly press **Patch DB now**

## What it patches

### Sonarr

It reads `Series.TvdbId`, picks a best AniDB candidate using Kometa + AniDB title data, and builds alias inserts for names not already present in your current Sonarr `SceneMappings` table.

### Radarr

It reads movies from the Radarr DB, uses `tmdb_movie_id` / `imdb_id` matches from Kometa, and builds alias inserts for a detected alternative-title table. The Radarr patch path is **schema-introspected and best-effort**, because Radarr's DB shape can vary and there is not a stable public write endpoint for adding custom alt titles in the UI.

## Important safety notes

- **Back up first.** The app has a backup button and also backs up automatically before patching.
- **Stop Sonarr/Radarr before patching.** The UI warns about this before the patch starts.
- Backup files are written beside the databases as:
  - `sonarr.db.bak`
  - `radarr.db.bak`
- Errors are written to the configured error log path, default:
  - `/config/errors.log`

## Default volume mappings

```yaml
- /mnt/user/appdata/smartanimemapper:/config
- /mnt/user/appdata/sonarr-anime:/sonarr-config
- /mnt/user/appdata/radarr-anime:/radarr-config
```

## Deploy

1. Copy this folder somewhere on Unraid, for example:
   ```bash
   /mnt/user/appdata/smartanimemapper-app
   ```
2. Copy `.env.example` to `.env` and edit it.
3. Build and start the container:
   ```bash
   cd /mnt/user/appdata/smartanimemapper-app
   docker compose up -d --build
   ```
4. Open the UI:
   ```text
   http://YOUR-UNRAID-IP:8844
   ```
5. Run the setup wizard and point the app at your DB files if you changed the defaults.
6. Use **Fetch now**.
7. Use **Compile patch**.
8. Stop your `sonarr-anime` and `radarr-anime` containers.
9. Use **Patch DB now**.
10. Start the containers again and test a few known-problem titles.

## Settings worth knowing

- **Monthly schedule enabled**: defaults on
- **Day of month**: defaults to `1`
- **AniDB throttle hours**: defaults to `24`
- **Kometa throttle hours**: defaults to `24`
- **Probe XEM during Sonarr compile**: optional, off by default, slow and best-effort

## File browser behavior

The built-in browser only allows paths under these mounted roots:

- `/config`
- `/sonarr-config`
- `/radarr-config`

## Project layout

```text
smartanimemapper/
  app.py
  compiler.py
  config_store.py
  db_ops.py
  fetchers.py
  jobs.py
  logging_utils.py
  state.py
  utils.py
  templates/
  static/
Dockerfile
docker-compose.yml
```

## Current limitation

Radarr support is intentionally conservative. If your Radarr DB schema does not expose a recognizable alternative-title table, SmartAnimeMapper will still compile a Radarr preview but may refuse to apply the patch and will write the reason to the error log.
