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
package de.ddb.labs.zdbdump.web;

import de.ddb.labs.zdbdump.dump.DumpPaths;
import de.ddb.labs.zdbdump.dump.ZdbDumpCreationCronJob;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.ServletWebRequest;
import org.springframework.web.util.DisconnectedClientHelper;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Lists and serves published files from the configured output directory and exposes the manual dump trigger.
 * Generation uses the same job as the scheduler and runs independently of HTTP responses.
 */
@RestController
class ZdbDumpController {

    private static final Logger log = LoggerFactory.getLogger(ZdbDumpController.class);

    private final Path outputDirectory;
    private final String outputFilename;
    private final String baseUrl;
    private final ZdbDumpCreationCronJob dumpCreation;

    /**
     * Initializes the HTTP interface with the same validated dump path used for publication.
     *
     * @param paths validated storage paths shared by the worker and the database
     * @param baseUrl public base address used to build download links
     * @param dumpCreation job that handles manual generation requests
     */
    ZdbDumpController(
            DumpPaths paths,
            @Value("${zdbdump.baseurl}") String baseUrl,
            ZdbDumpCreationCronJob dumpCreation) {
        this.outputDirectory = paths.outputFile().getParent();
        this.outputFilename = paths.outputFile().getFileName().toString();
        this.baseUrl = baseUrl;
        this.dumpCreation = dumpCreation;
    }

    /**
     * Requests a background dump run. HTTP 202 confirms dispatch, while the job decides whether
     * it can start a run and publishes the result separately.
     */
    @PostMapping("/createdump")
    @ResponseStatus(HttpStatus.ACCEPTED)
    @Async
    public void createDump() {
        log.info("Dispatching a manually requested dump creation");
        dumpCreation.run();
    }

    /**
     * Reserves the trigger route so a GET or HEAD cannot fall through to the filename handler.
     *
     * @return HTTP 405 with POST as the allowed method
     */
    @GetMapping("/createdump")
    public ResponseEntity<Void> rejectGetTrigger() {
        return ResponseEntity.status(HttpStatus.METHOD_NOT_ALLOWED).allow(HttpMethod.POST).build();
    }

    /**
     * Lists public download URLs and their file modification times in URL order.
     *
     * @param request incoming request, which supplies the servlet context path
     * @return the download listing, or an HTTP 500 response if the directory cannot be read
     */
    @GetMapping(value = "/", produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Map<String, String>> getFileList(HttpServletRequest request) {
        final Map<String, String> downloads = new TreeMap<>();
        try (var files = Files.newDirectoryStream(outputDirectory)) {
            for (Path file : files) {
                addPublishedDump(downloads, file, request.getContextPath());
            }
        } catch (IOException | DirectoryIteratorException e) {
            log.error("Cannot list the dump directory {}", outputDirectory, e);
            return ResponseEntity.internalServerError().body(Map.of("Status", "Error. Can't read the dump directory."));
        }
        return ResponseEntity.ok(downloads);
    }

    /** Adds a download link for an eligible, readable file that still exists at inspection time. */
    private void addPublishedDump(Map<String, String> downloads, Path file, String contextPath) throws IOException {
        final String filename = file.getFileName().toString();
        if (!isDumpFilename(filename)) {
            return;
        }
        try {
            final var attributes = readDumpAttributes(file);
            if (!Files.isReadable(file)) {
                log.debug("Skipping unreadable published dump {} while listing downloads", file);
                return;
            }
            // Encode the filename as one segment under the configured public address.
            final String url = UriComponentsBuilder.fromUriString(baseUrl)
                    .path(contextPath)
                    .pathSegment("{filename}")
                    .encode()
                    .buildAndExpand(filename)
                    .toUriString();
            final var modified = attributes.lastModifiedTime().toInstant().atZone(ZoneId.systemDefault());
            downloads.put(url, DateTimeFormatter.ISO_DATE_TIME.format(modified));
        } catch (NoSuchFileException ignored) {
            // A dump can disappear while listing the directory.
        } catch (AccessDeniedException e) {
            log.debug("Access denied while inspecting published dump {} for the download list", file);
        }
    }

    /**
     * Streams a published file from disk and answers HEAD requests with the corresponding headers.
     * A conditional request can receive HTTP 304 when the file has not changed.
     *
     * @param filename requested filename as a single URL path segment
     * @param request incoming request, including any conditional headers
     * @param response outgoing response used for headers and file content
     * @throws IOException if an error occurs after the response is committed or the error response cannot be sent
     */
    @GetMapping("/{filename}")
    public void getFile(@PathVariable String filename, HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        final Path file = resolveDownloadPath(filename);
        if (file == null) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }

        try {
            sendPublishedDump(filename, file, request, response);
        } catch (IOException e) {
            handleDownloadFailure(file, e, response);
        }
    }

    /** Resolves an eligible name to a direct child of the output directory, or returns {@code null}. */
    private Path resolveDownloadPath(String filename) {
        if (!isDumpFilename(filename)) {
            return null;
        }
        try {
            final Path file = outputDirectory.resolve(filename).normalize();
            return outputDirectory.equals(file.getParent()) ? file : null;
        } catch (InvalidPathException e) {
            return null;
        }
    }

    /** Uses the open file for the content length and a stable path timestamp for conditional requests. */
    private static void sendPublishedDump(String filename, Path file, HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        final var attributes = readDumpAttributes(file);
        try (var channel = FileChannel.open(file, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS)) {
            final long length = channel.size();
            final var currentAttributes = readDumpAttributes(file);
            // Publication can replace the path between reads. Use its timestamp only if it stayed
            // stable and the current path's size matches the open file.
            final boolean canUseTimestamp = currentAttributes.size() == length
                    && currentAttributes.lastModifiedTime().equals(attributes.lastModifiedTime());
            if (canUseTimestamp && new ServletWebRequest(request, response)
                    .checkNotModified(currentAttributes.lastModifiedTime().toMillis())) {
                return;
            }
            response.setContentType(dumpContentType(file));
            response.setHeader(HttpHeaders.CONTENT_DISPOSITION,
                    ContentDisposition.attachment().filename(filename, StandardCharsets.UTF_8).build().toString());
            response.setContentLengthLong(length);
            if (!HttpMethod.HEAD.matches(request.getMethod())) {
                writeDump(file, channel, length, response);
            }
        }
    }

    /** Maps file access failures to HTTP responses when the headers can still be changed. */
    private static void handleDownloadFailure(Path file, IOException failure, HttpServletResponse response)
            throws IOException {
        if (response.isCommitted()) {
            throw new IOException("Could not finish streaming published dump " + file, failure);
        }
        response.reset();
        if (failure instanceof AccessDeniedException) {
            log.debug("Access denied while serving published dump {}", file);
        }
        if (failure instanceof NoSuchFileException || failure instanceof AccessDeniedException) {
            response.setStatus(HttpServletResponse.SC_NOT_FOUND);
            return;
        }
        log.error("Cannot serve published dump {}", file, failure);
        response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
    }

    private static String dumpContentType(Path file) {
        try {
            final String detected = Files.probeContentType(file);
            if (detected != null) {
                return detected;
            }
        } catch (IOException e) {
            log.debug("Could not detect media type for published dump {}. Serving it as binary data", file, e);
        }
        return MediaType.APPLICATION_OCTET_STREAM_VALUE;
    }

    /** Checks that the transfer matches the advertised size and logs routine client disconnects at debug level. */
    private static void writeDump(Path file, FileChannel channel, long length, HttpServletResponse response)
            throws IOException {
        try {
            final long written = Channels.newInputStream(channel).transferTo(response.getOutputStream());
            if (written != length) {
                throw new IOException("Dump size changed during download. Expected " + length + " bytes, read " + written);
            }
            response.flushBuffer();
        } catch (IOException e) {
            if (!DisconnectedClientHelper.isClientDisconnectedException(e)) {
                throw e;
            }
            log.debug("Client disconnected while downloading published dump {}", file);
        }
    }

    /** Allows the configured output name and archived RDF dumps while filtering internal filenames. */
    private boolean isDumpFilename(String filename) {
        if (filename.isBlank() || filename.startsWith(".") || filename.equals("createdump")
                || filename.indexOf('/') >= 0 || filename.indexOf('\\') >= 0
                || filename.indexOf('%') >= 0 || filename.indexOf(';') >= 0
                || filename.codePoints().anyMatch(Character::isISOControl)) {
            return false;
        }
        return filename.equals(outputFilename) || (filename.toLowerCase(Locale.ROOT).endsWith(".rdf.gz")
                && !filename.equalsIgnoreCase(ZdbDumpCreationCronJob.BASE_DUMP_CACHE_FILENAME));
    }

    /** Reads attributes for a nonempty regular file without following symbolic links. */
    private static BasicFileAttributes readDumpAttributes(Path file) throws IOException {
        final BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class,
                LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile() || attributes.size() == 0) {
            throw new NoSuchFileException(file.toString());
        }
        return attributes;
    }
}
