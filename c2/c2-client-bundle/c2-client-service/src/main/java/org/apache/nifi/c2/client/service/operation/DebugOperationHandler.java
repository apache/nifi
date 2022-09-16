/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.c2.client.service.operation;

import static java.nio.file.Files.copy;
import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.lines;
import static java.nio.file.Files.write;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.apache.commons.compress.utils.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.DEBUG;
import static org.apache.nifi.c2.protocol.api.OperationType.TRANSFER;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.C2OperationState.OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugOperationHandler implements C2OperationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(DebugOperationHandler.class);

    private static final String C2_CALLBACK_URL_NOT_FOUND = "C2 Server callback URL was not found in request";
    private static final String SUCCESSFUL_UPLOAD = "Debug bundle was uploaded successfully";
    private static final String UNABLE_TO_CREATE_BUNDLE = "Unable to create debug bundle";

    static final String TARGET_ARG = "target";
    static final String NEW_LINE = "\n";

    private final C2Client c2Client;
    private final String configDir;
    private final String logDir;
    private final Predicate<String> contentFilter;

    private DebugOperationHandler(C2Client c2Client, String configDir, String logDir, Predicate<String> contentFilter) {
        this.c2Client = c2Client;
        this.configDir = configDir;
        this.logDir = logDir;
        this.contentFilter = contentFilter;
    }

    public static DebugOperationHandler create(C2Client c2Client, String configDir, String logDir, Predicate<String> contentFilter) {
        if (c2Client == null) {
            throw new IllegalArgumentException("C2Client should not be null");
        }
        if (isBlank(configDir)) {
            throw new IllegalArgumentException("configDir should not be not null or empty");
        }
        if (isBlank(logDir)) {
            throw new IllegalArgumentException("logDir should not be not null or empty");
        }
        if (contentFilter == null) {
            throw new IllegalArgumentException("Exclude sensitive filter should not be null");
        }

        return new DebugOperationHandler(c2Client, configDir, logDir, contentFilter);
    }

    @Override
    public OperationType getOperationType() {
        return TRANSFER;
    }

    @Override
    public OperandType getOperandType() {
        return DEBUG;
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        Function<C2OperationState, C2OperationAck> operationAckCreate = operationAck(operation);

        String debugCallbackUrl = operation.getArgs().get(TARGET_ARG);
        if (debugCallbackUrl == null) {
            LOG.error("Callback URL was not found in C2 request.");
            return operationAckCreate.apply(operationState(NOT_APPLIED, C2_CALLBACK_URL_NOT_FOUND));
        }

        List<Path> bundleFilePaths = concat(
            debugBundlePaths(logDir, LogFile.values()),
            debugBundlePaths(configDir, ConfigFile.values()))
            .collect(toList());

        List<Path> contentFilteredFilePaths = null;
        C2OperationState operationState;
        try {
            contentFilteredFilePaths = filterContent(bundleFilePaths);
            operationState = createDebugBundle(contentFilteredFilePaths)
                .map(bundle -> c2Client.uploadDebugBundle(debugCallbackUrl, bundle)
                    .map(errorMessage -> operationState(NOT_APPLIED, errorMessage))
                    .orElseGet(() -> operationState(FULLY_APPLIED, SUCCESSFUL_UPLOAD)))
                .orElseGet(() -> operationState(NOT_APPLIED, UNABLE_TO_CREATE_BUNDLE));
        } catch (Exception e) {
            LOG.error("Unexpected error happened", e);
            operationState = operationState(NOT_APPLIED, UNABLE_TO_CREATE_BUNDLE);
        } finally {
            ofNullable(contentFilteredFilePaths).ifPresent(this::cleanup);
        }

        LOG.debug("Returning operation ack with state={} and details={}", operationState.getState(), operationState.getDetails());
        return operationAckCreate.apply(operationState);
    }

    private Function<C2OperationState, C2OperationAck> operationAck(C2Operation operation) {
        return state -> {
            C2OperationAck operationAck = new C2OperationAck();
            operationAck.setOperationId(ofNullable(operation.getIdentifier()).orElse(EMPTY));
            operationAck.setOperationState(state);
            return operationAck;
        };
    }

    private C2OperationState operationState(OperationState operationState, String details) {
        C2OperationState state = new C2OperationState();
        state.setState(operationState);
        state.setDetails(details);
        return state;
    }

    private Stream<Path> debugBundlePaths(String baseDir, DebugBundleFile... debugBundleFiles) {
        return Stream.of(debugBundleFiles)
            .map(bundleFile -> Paths.get(baseDir, bundleFile.getFileName()))
            .filter(Files::exists)
            .filter(Files::isRegularFile);
    }

    private List<Path> filterContent(List<Path> bundleFilePaths) {
        List<Path> contentFilteredFilePaths = new ArrayList<>();
        for (Path path : bundleFilePaths) {
            String fileName = path.getFileName().toString();
            try {
                Path tempDirectory = createTempDirectory(null);
                Path tempFile = Paths.get(tempDirectory.toAbsolutePath().toString(), fileName);
                write(tempFile, (Iterable<String>) lines(path).filter(contentFilter)::iterator);
                contentFilteredFilePaths.add(tempFile);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return contentFilteredFilePaths;
    }

    public Optional<byte[]> createDebugBundle(List<Path> filePaths) {
        if (filePaths.size() == 0) {
            return empty();
        }
        ByteArrayOutputStream byteOutputStream = null;
        try {
            byteOutputStream = new ByteArrayOutputStream();
            try (GzipCompressorOutputStream gzipCompressorOutputStream = new GzipCompressorOutputStream(byteOutputStream);
                 TarArchiveOutputStream tarOutputStream = new TarArchiveOutputStream(gzipCompressorOutputStream)) {
                for (Path filePath : filePaths) {
                    TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(filePath.toFile(), filePath.getFileName().toString());
                    tarOutputStream.putArchiveEntry(tarArchiveEntry);
                    copy(filePath, tarOutputStream);
                    tarOutputStream.closeArchiveEntry();
                }
                tarOutputStream.finish();
            }
        } catch (Exception e) {
            LOG.error("Error during create compressed bundle", e);
            closeQuietly(byteOutputStream);
            byteOutputStream = null;
        } finally {
            closeQuietly(byteOutputStream);
        }
        return ofNullable(byteOutputStream).map(ByteArrayOutputStream::toByteArray);
    }

    private void cleanup(List<Path> paths) {
        paths.forEach(path -> {
            try {
                deleteIfExists(path);
            } catch (IOException e) {
                LOG.warn("Unable to delete temporary file", e);
            }
        });
    }

    interface DebugBundleFile {
        String getFileName();
    }

    enum LogFile implements DebugBundleFile {
        APP_LOG_FILE("minifi-app.log"),
        BOOTSTRAP_LOG_FILE("minifi-bootstrap.log");

        private final String fileName;

        LogFile(String fileName) {
            this.fileName = fileName;
        }

        public String getFileName() {
            return fileName;
        }
    }

    enum ConfigFile implements DebugBundleFile {
        CONFIG_YML_FILE("config.yml"),
        CONFIG_NEW_YML_FILE("config-new.yml"),
        BOOTSTRAP_CONF_FILE("bootstrap.conf");

        private final String fileName;

        ConfigFile(String fileName) {
            this.fileName = fileName;
        }

        public String getFileName() {
            return fileName;
        }
    }
}
