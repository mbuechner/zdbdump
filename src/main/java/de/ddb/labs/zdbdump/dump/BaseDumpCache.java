/*
 * Copyright 2023-2026 Michael Büchner, Deutsche Digitale Bibliothek
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package de.ddb.labs.zdbdump.dump;

import de.ddb.labs.zdbdump.dump.rdf.RdfDump;
import de.ddb.labs.zdbdump.dump.support.DumpFailure;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import javax.xml.stream.XMLStreamException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientException;

/**
 * Downloads and caches the dated DNB archive listed in the open data directory. The catalog's
 * data state determines where the OAI harvest begins. The sidecar records the exact archive
 * URL and its strong HTTP validator for cache reuse.
 */
final class BaseDumpCache {

    static final String ARCHIVE_FILENAME = "zdb_lds_base.rdf.gz";
    static final String METADATA_FILENAME = "zdb_lds_base.metadata";

    private static final Logger log = LoggerFactory.getLogger(BaseDumpCache.class);
    private static final URI DIRECTORY_URL = URI.create("https://data.dnb.de/opendata/");
    private static final int MAX_DIRECTORY_BYTES = 1024 * 1024;
    // The Apache directory lists the snapshot in the description following the dated RDF/XML link.
    private static final Pattern ARCHIVE_ENTRY = Pattern.compile(
            "<a\\s+href=\"(zdb_lds_[0-9]{8}\\.rdf\\.gz)\"\\s*>\\1</a>([^\\r\\n<]*)");
    private static final Pattern DATA_STATE = Pattern.compile(
            "\\(Stand:\\s*([0-9]{2}\\.[0-9]{2}\\.[0-9]{4})\\s+([0-9]{2}:[0-9]{2}(?::[0-9]{2})?)\\s+(?:Uhr\\s+)?UTC\\)");
    private static final DateTimeFormatter DATA_STATE_TIME = DateTimeFormatter
            .ofPattern("dd.MM.uuuu HH:mm[:ss]", Locale.ROOT).withResolverStyle(ResolverStyle.STRICT);
    private static final Pattern STRONG_ETAG = Pattern.compile("\"[!#-~\\x80-\\xff]*\"");

    private final RestClient restClient;
    private final RdfDump rdf;
    private final Path cachedDumpPath;
    private final Path metadataPath;

    BaseDumpCache(RestClient restClient, RdfDump rdf, Path directory) {
        this.restClient = restClient;
        this.rdf = rdf;
        this.cachedDumpPath = directory.resolve(ARCHIVE_FILENAME);
        this.metadataPath = directory.resolve(METADATA_FILENAME);
    }

    /** Rebuilds the working map from the verified base and returns the start of the OAI harvest. */
    Instant load(Map<String, String> records) throws IOException, XMLStreamException, InterruptedException {
        final Snapshot snapshot = resolveSnapshot();
        if (loadCachedBase(snapshot, records)) {
            return snapshot.dataState();
        }

        final Path stagingDirectory = cachedDumpPath.getParent();
        final Path stagedDump;
        try {
            Files.createDirectories(stagingDirectory);
            stagedDump = Files.createTempFile(stagingDirectory, ".zdbdump-base-", ".tmp");
        } catch (IOException e) {
            throw new DumpFailure("Preparing local staging for the DNB base archive in " + stagingDirectory
                    + " failed. " + DumpFailure.describe(e), e);
        }
        try {
            final String etag = download(stagedDump, snapshot);
            rdf.loadBase(stagedDump, snapshot.uri(), records);
            if (etag != null) {
                cacheVerifiedBase(stagedDump, snapshot.metadata(etag));
            }
            return snapshot.dataState();
        } finally {
            DumpFailure.discardStage(stagedDump, log);
        }
    }

    /** Fetches the archive index before selecting the exact RDF/XML download. */
    private Snapshot resolveSnapshot() throws DumpFailure {
        log.info("Looking up the current dated ZDB archive at {}", DIRECTORY_URL);
        final byte[] body;
        try {
            body = restClient.get().uri(DIRECTORY_URL).exchange((request, response) -> {
                DumpFailure.requireSuccessfulStatus(response, DIRECTORY_URL);
                return response.getBody().readNBytes(MAX_DIRECTORY_BYTES + 1);
            });
        } catch (RestClientException e) {
            throw DumpFailure.downloadFailure("the DNB archive index", DIRECTORY_URL, e);
        }
        return readSnapshot(body);
    }

    /** Reads the data state and URL of the newest dated RDF/XML entry in the index. */
    private static Snapshot readSnapshot(byte[] body) throws DumpFailure {
        if (body.length > MAX_DIRECTORY_BYTES) {
            throw new DumpFailure("The DNB archive directory exceeds the supported size", null);
        }
        final var entry = ARCHIVE_ENTRY.matcher(new String(body, StandardCharsets.ISO_8859_1))
                .results().max(Comparator.comparing(match -> match.group(1)))
                .orElseThrow(() -> new DumpFailure("The DNB directory contains no dated ZDB RDF/XML archive", null));
        final var timestamp = DATA_STATE.matcher(entry.group(2));
        if (!timestamp.find()) {
            throw new DumpFailure("The dated ZDB archive has no catalog data state", null);
        }
        try {
            final Instant dataState = LocalDateTime.parse(timestamp.group(1) + " " + timestamp.group(2),
                    DATA_STATE_TIME).toInstant(ZoneOffset.UTC);
            if (dataState.isAfter(Instant.now())) {
                throw new DumpFailure("The dated ZDB archive has a future catalog data state", null);
            }
            return new Snapshot(DIRECTORY_URL.resolve(entry.group(1)), dataState);
        } catch (DateTimeParseException e) {
            throw new DumpFailure("The dated ZDB archive has an invalid catalog data state", e);
        }
    }

    /** Reads the cache validator or lets the caller download the archive after a HEAD failure. */
    private String fetchValidator(URI uri) {
        try {
            return restClient.head().uri(uri).exchange((request, response) -> {
                final int status = response.getStatusCode().value();
                if (status == HttpStatus.METHOD_NOT_ALLOWED.value() || status == HttpStatus.NOT_IMPLEMENTED.value()) {
                    return null;
                }
                DumpFailure.requireSuccessfulStatus(response, uri);
                return strongEtag(response.getHeaders());
            });
        } catch (RestClientException e) {
            if (Thread.currentThread().isInterrupted() || DumpFailure.hasCause(e, InterruptedException.class)) {
                throw e;
            }
            log.warn("Could not validate the cached DNB dump at {}. Trying a fresh download.", uri);
            return null;
        }
    }

    /** Reuses a matching archive only if the entire archive can rebuild the working map. */
    private boolean loadCachedBase(Snapshot snapshot, Map<String, String> records)
            throws InterruptedException {
        if (!Files.isRegularFile(cachedDumpPath) || !Files.isRegularFile(metadataPath)) {
            return false;
        }
        try {
            // A legacy sidecar contains only an HTTP timestamp and cannot identify the dated archive.
            final List<String> savedMetadata = Files.readAllLines(metadataPath, StandardCharsets.UTF_8);
            if (savedMetadata.size() != 3 || !savedMetadata.equals(snapshot.metadata(savedMetadata.getLast()))) {
                return false;
            }
            final String currentEtag = fetchValidator(snapshot.uri());
            DumpFailure.checkInterrupted();
            if (currentEtag == null || !currentEtag.equals(savedMetadata.getLast())) {
                return false;
            }
            log.info("Loading cached base dump at {} ({})", cachedDumpPath, snapshot.dataState());
            rdf.loadBase(cachedDumpPath, snapshot.uri(), records);
            return true;
        } catch (IOException | XMLStreamException e) {
            log.warn("Could not use cached base dump at {}. Downloading it again. {}",
                    cachedDumpPath, DumpFailure.describe(e));
            return false;
        }
    }

    /** Downloads the exact dated archive. Only metadata from this response can validate its bytes. */
    private String download(Path stagedDump, Snapshot snapshot) throws DumpFailure {
        log.info("Downloading the base dump from {}", snapshot.uri());
        try {
            return restClient.get().uri(snapshot.uri()).exchange((request, response) -> {
                DumpFailure.requireSuccessfulStatus(response, snapshot.uri());
                if (response.getStatusCode().value() != HttpStatus.OK.value()) {
                    throw DumpFailure.downloadFailure("the DNB RDF/XML dump", snapshot.uri(), null);
                }
                final long bytes;
                try {
                    bytes = Files.copy(response.getBody(), stagedDump, StandardCopyOption.REPLACE_EXISTING);
                } catch (FileSystemException e) {
                    throw new DumpFailure("Writing the DNB base archive to local staging failed. "
                            + DumpFailure.describe(e), e);
                }
                log.info("Downloaded {} bytes for base snapshot dated {}", bytes, snapshot.dataState());
                return strongEtag(response.getHeaders());
            });
        } catch (RestClientException e) {
            if (e.getCause() instanceof DumpFailure failure) {
                throw failure;
            }
            throw DumpFailure.downloadFailure("the DNB RDF/XML dump", snapshot.uri(), e);
        }
    }

    /** Refreshes the optional cache after validation. Failure here still leaves a usable working map. */
    private void cacheVerifiedBase(Path stagedDump, List<String> metadata) {
        try {
            // Invalidate the old metadata before replacing the data so an interrupted update cannot match it.
            Files.deleteIfExists(metadataPath);
            Files.move(stagedDump, cachedDumpPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            Files.write(metadataPath, metadata, StandardCharsets.UTF_8);
        } catch (IOException e) {
            log.warn("Could not update the cache at {}. Continuing with the verified base dump. {}",
                    cachedDumpPath, DumpFailure.describe(e));
        }
    }

    /** Accepts validators that the server asserts identify byte-identical representations. */
    private static String strongEtag(HttpHeaders headers) {
        final String value = headers.getFirst(HttpHeaders.ETAG);
        return value != null && STRONG_ETAG.matcher(value).matches() ? value : null;
    }

    /** Couples the catalog data state with the exact URI from which the archive is retrieved. */
    private record Snapshot(URI uri, Instant dataState) {
        private List<String> metadata(String etag) {
            return List.of(uri.toString(), dataState.toString(), etag);
        }
    }
}
