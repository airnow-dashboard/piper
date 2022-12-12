## docker test (for `current` pipeline)
```bash
docker run --rm \
--mount type=bind,source=/var/lib/airnow,target=/app/output \
--env AIRNOW_DB_HOST=127.0.0.1 \
--env AIRNOW_DB_USER=airnow_admin \
--env AIRNOW_DB_PASSWORD=changeme \
--network=host piper:latest \
/app/output current
```