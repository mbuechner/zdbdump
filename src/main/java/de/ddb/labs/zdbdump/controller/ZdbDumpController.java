/*
 * Copyright 2023 Michael Büchner, Deutsche Digitale Bibliothek
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
package de.ddb.labs.zdbdump.controller;

import de.ddb.labs.zdbdump.cronjobs.ZdbDumpCreationCronJob;
import jakarta.servlet.http.HttpServletRequest;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.TransformerConfigurationException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@RestController
@Slf4j
class ZdbDumpController {

    @Value("${zdbdump.path.output}")
    private String outputPath;

    @Value("${zdbdump.baseurl}")
    private String baseurl;

    @Autowired
    private ZdbDumpCreationCronJob downloadDump;

    @RequestMapping(
            method = RequestMethod.GET,
            value = "/createdump",
            produces = "application/json"
    )
    @Async
    public void createDump(HttpServletRequest request) throws IOException, FileNotFoundException, XMLStreamException, TransformerConfigurationException {
        downloadDump.run();
    }

    @RequestMapping(
            method = RequestMethod.GET,
            value = "/",
            produces = "application/json"
    )
    @ResponseBody
    public Map<String, String> getFileList(HttpServletRequest request) {
        final String ruri = request.getRequestURI();
        final File dir = new File(outputPath);
        final Map<String, String> urlList = new HashMap<>();

        if (!dir.isDirectory()) {
            urlList.put("Status", "Error. Data directory is not correct configured.");
            return urlList;
        }

        if (!dir.canRead()) {
            urlList.put("Status", "Error. Can't read data directory.");
            return urlList;
        }

        final File[] files = dir.listFiles();

        if (files != null) {
            for (int i = 0; i < files.length; ++i) {
                final Instant instant = Instant.ofEpochMilli(files[i].lastModified());
                urlList.put(
                        baseurl + ruri + files[i].getName(),
                        ZonedDateTime.ofInstant(instant, ZoneId.systemDefault()).format(DateTimeFormatter.ISO_DATE_TIME)
                );
            }
        }
        return urlList;
    }

    @RequestMapping(
            method = RequestMethod.GET,
            value = "/{filename}"
    )
    @ResponseBody
    public ResponseEntity<InputStreamResource> getFile(@PathVariable String filename) throws FileNotFoundException, IOException {
        final File file = new File(outputPath + filename);
        final MediaType mediaType = MediaType.parseMediaType(Files.probeContentType(file.toPath()));

        if (!file.exists()) {
            return ResponseEntity.notFound().build();
        }
        final InputStreamResource resource = new InputStreamResource(new FileInputStream(file));

        return ResponseEntity.ok()
                // Content-Disposition
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment;filename=" + file.getName())
                // Content-Type
                .contentType(mediaType)
                // Contet-Length
                .contentLength(file.length()) //
                .lastModified(file.lastModified())
                .body(resource);
    }
}
