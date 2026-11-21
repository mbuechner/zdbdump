[![Java CI with Maven](https://github.com/mbuechner/zdbdump/actions/workflows/maven.yml/badge.svg)](https://github.com/mbuechner/zdbdump/actions/workflows/maven.yml) [![Docker](https://github.com/mbuechner/zdbdump/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/mbuechner/zdbdump/actions/workflows/docker-publish.yml)

# zdbdump

DNB RDF/XML snapshot + OAI-PMH deltas (`zdb`, `RDFxml`) → MVStore → `zdb_lds.rdf.gz`. Replay starts at the snapshot's UTC data state (`Stand`). The finished dump is published atomically.

## Run

Amazon Corretto 25 LTS. Start with `./mvnw spring-boot:run` or `.\mvnw.cmd spring-boot:run` on Windows.

```bash
docker build -t zdbdump .
docker run --rm -p 8080:8080 -v zdbdump-data:/home/zdbdump/data zdbdump
```

The container runs as UID/GID `10001`. Use a writable volume. Generation runs at 04:00 local time by default. Set `SCHEDULER_ENABLED=false` for manual runs only. See [environment template](.env.example) and [application defaults](src/main/resources/application.properties).

## HTTP

- `GET /` lists download URLs and timestamps. `GET /{filename}` and `HEAD /{filename}` support `If-Modified-Since`.
- `GET /actuator/health` reports health.
- `POST /createdump` queues a run (`202`). Set `ZDBDUMP_SECURITY_USER` and `ZDBDUMP_SECURITY_PASSWORD` to enable it.

```bash
curl -u operator -X POST -H 'X-ZDBDUMP-TRIGGER: true' http://localhost:8080/createdump
```

## Build and release

`./mvnw clean verify` builds `target/zdbdump.jar` and a CycloneDX SBOM.

`./mvnw site` generates `target/site`. To publish on pushes to `main`, set **Settings → Pages → Source** to **GitHub Actions**. [Project Documentation](https://mbuechner.github.io/zdbdump/)

- A stable GitHub release dispatches `Docker` on `main` if the tag includes the release trigger. The workflow uses the tag's source, Dockerfile and commit timestamp. For a manual run on `main`, set `release_tag` or leave it empty to use the maintained release.
- Daily at 08:17 Europe/Berlin, `Docker` checks the maintained release's `linux/amd64` base digests. Changed inputs or a missing image trigger a build. Otherwise it scans the existing digest.
- HTTP probe for new images, Trivy scan, Cosign sign and verify, then tag `ghcr.io/mbuechner/zdbdump`. The maintained release gets `latest` and `tagged`. It defaults to the newest stable release. Override with `CONTAINER_RELEASE_TAG`. Grant Actions write access if the GHCR package already exists.

## GND utility

[`GndOldAuthMain`](src/main/java/de/ddb/labs/zdbdump/tools/gnd/GndOldAuthMain.java) exports JSON-LD and `current|former` mappings. Pass `--beacon` for `former||current`.
