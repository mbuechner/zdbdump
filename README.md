[![Java CI with Maven](https://github.com/mbuechner/zdbdump/actions/workflows/maven.yml/badge.svg)](https://github.com/mbuechner/zdbdump/actions/workflows/maven.yml) [![Docker](https://github.com/mbuechner/zdbdump/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/mbuechner/zdbdump/actions/workflows/docker-publish.yml)

# zdbdump
Tiny Spring Boot service for generating a fresh RDF dump of the German Union Catalogue of Serials (ZDB).

Java 25 in, gzip out. No fluff.

## What it does
- downloads the upstream dump
- replays OAI updates
- writes the current snapshot to disk
- serves the result via HTTP

## Quickstart
```bash
./mvnw spring-boot:run
```

On Windows PowerShell use:
```powershell
.\mvnw.cmd spring-boot:run
```

Default base URL: `http://localhost:8080/`

## Endpoints
- `GET /` — list available dump files
- `GET /createdump` — trigger a fresh dump run, protected by HTTP Basic Auth
- `GET /{filename}` — download a specific file

## Runtime knobs
Environment variables you will most likely care about:
- `ZDBDUMP_PORT`
- `ZDBDUMP_BASEURL`
- `ZDBDUMP_CRON_JOB`
- `ZDBDUMP_PATH_OUTPUT`
- `ZDBDUMP_PATH_TEMP`
- `ZDBDUMP_SECURITY_USER`
- `ZDBDUMP_SECURITY_PASSWORD`

