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
import static java.nio.file.Files.lines;
import static java.nio.file.Files.walk;
import static java.util.Optional.empty;
import static java.util.Optional.ofNullable;
import static org.apache.commons.compress.utils.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.DEBUG;
import static org.apache.nifi.c2.protocol.api.OperationType.TRANSFER;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    private final List<Path> bundleFilePaths;
    private final Predicate<String> contentFilter;
    private final OperandPropertiesProvider operandPropertiesProvider;

    private DebugOperationHandler(C2Client c2Client, List<Path> bundleFilePaths, Predicate<String> contentFilter,
                                  OperandPropertiesProvider operandPropertiesProvider) {
        this.c2Client = c2Client;
        this.bundleFilePaths = bundleFilePaths;
        this.contentFilter = contentFilter;
        this.operandPropertiesProvider = operandPropertiesProvider;
    }

    public static DebugOperationHandler create(C2Client c2Client, List<Path> bundleFilePaths, Predicate<String> contentFilter,
                                               OperandPropertiesProvider operandPropertiesProvider) {
        if (c2Client == null) {
            throw new IllegalArgumentException("C2Client should not be null");
        }
        if (bundleFilePaths == null || bundleFilePaths.isEmpty()) {
            throw new IllegalArgumentException("bundleFilePaths should not be not null or empty");
        }
        if (contentFilter == null) {
            throw new IllegalArgumentException("Content filter should not be null");
        }

        return new DebugOperationHandler(c2Client, bundleFilePaths, contentFilter, operandPropertiesProvider);
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
    public Map<String, Object> getProperties() {
        return operandPropertiesProvider.getProperties();
    }

    @Override
    public C2OperationAck handle(C2Operation operation) {
        String debugCallbackUrl = operation.getArgs().get(TARGET_ARG);
        if (debugCallbackUrl == null) {
            LOG.error("Callback URL was not found in C2 request.");
            return operationAck(operation, operationState(NOT_APPLIED, C2_CALLBACK_URL_NOT_FOUND));
        }

        List<Path> contentFilteredFilePaths = null;
        C2OperationState operationState;
        try {
            contentFilteredFilePaths = filterContent(operation.getIdentifier(), bundleFilePaths);
            operationState = createDebugBundle(contentFilteredFilePaths)
                .map(bundle -> c2Client.uploadBundle(debugCallbackUrl, bundle)
                    .map(errorMessage -> operationState(NOT_APPLIED, errorMessage))
                    .orElseGet(() -> operationState(FULLY_APPLIED, SUCCESSFUL_UPLOAD)))
                .orElseGet(() -> operationState(NOT_APPLIED, UNABLE_TO_CREATE_BUNDLE));
        } catch (Exception e) {
            LOG.error("Unexpected error happened", e);
            operationState = operationState(NOT_APPLIED, UNABLE_TO_CREATE_BUNDLE);
        } finally {
            ofNullable(contentFilteredFilePaths).ifPresent(this::cleanup);
        }

        LOG.debug("Returning operation ack for operation {} with state {} and details {}", operation.getIdentifier(), operationState.getState(), operationState.getDetails());
        return operationAck(operation, operationState);
    }

    private C2OperationAck operationAck(C2Operation operation, C2OperationState state) {
        C2OperationAck operationAck = new C2OperationAck();
        operationAck.setOperationId(ofNullable(operation.getIdentifier()).orElse(EMPTY));
        operationAck.setOperationState(state);
        return operationAck;
    }

    private C2OperationState operationState(OperationState operationState, String details) {
        C2OperationState state = new C2OperationState();
        state.setState(operationState);
        state.setDetails(details);
        return state;
    }

    private List<Path> filterContent(String operationId, List<Path> bundleFilePaths) {
        List<Path> contentFilteredFilePaths = new ArrayList<>();
        for (Path path : bundleFilePaths) {
            String fileName = path.getFileName().toString();
            try (Stream<String> fileStream = lines(path)) {
                Path tempDirectory = createTempDirectory(operationId);
                Path tempFile = Paths.get(tempDirectory.toAbsolutePath().toString(), fileName);
                Files.write(tempFile, (Iterable<String>) fileStream.filter(contentFilter)::iterator);
                contentFilteredFilePaths.add(tempFile);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        return contentFilteredFilePaths;
    }

    private Optional<byte[]> createDebugBundle(List<Path> filePaths) {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        try (GzipCompressorOutputStream gzipCompressorOutputStream = new GzipCompressorOutputStream(byteOutputStream);
             TarArchiveOutputStream tarOutputStream = new TarArchiveOutputStream(gzipCompressorOutputStream)) {
            for (Path filePath : filePaths) {
                TarArchiveEntry tarArchiveEntry = new TarArchiveEntry(filePath.toFile(), filePath.getFileName().toString());
                tarOutputStream.putArchiveEntry(tarArchiveEntry);
                copy(filePath, tarOutputStream);
                tarOutputStream.closeArchiveEntry();
            }
            tarOutputStream.finish();
        } catch (Exception e) {
            LOG.error("Error during create compressed bundle", e);
            return empty();
        } finally {
            closeQuietly(byteOutputStream);
        }
        return Optional.of(byteOutputStream).map(ByteArrayOutputStream::toByteArray);
    }

    private void cleanup(List<Path> paths) {
        paths.stream()
            .findFirst()
            .map(Path::getParent)
            .ifPresent(basePath -> {
                try (Stream<Path> walk = walk(basePath)) {
                    walk.map(Path::toFile).forEach(File::delete);
                } catch (IOException e) {
                    LOG.warn("Unable to clean up temporary directory {}", basePath, e);
                }
            });
    }
}
