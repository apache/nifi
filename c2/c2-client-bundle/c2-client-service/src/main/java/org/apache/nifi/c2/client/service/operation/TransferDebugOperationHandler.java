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
import static org.apache.commons.io.IOUtils.closeQuietly;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.DEBUG;
import static org.apache.nifi.c2.protocol.api.OperationType.TRANSFER;
import static org.apache.nifi.c2.util.Preconditions.requires;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.compress.compressors.gzip.GzipUtils;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.protocol.api.C2OperationState;
import org.apache.nifi.c2.protocol.api.OperandType;
import org.apache.nifi.c2.protocol.api.OperationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferDebugOperationHandler implements C2OperationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TransferDebugOperationHandler.class);

    private static final String C2_CALLBACK_URL_NOT_FOUND = "C2 Server callback URL was not found in request";
    private static final String SUCCESSFUL_UPLOAD = "Debug bundle was uploaded successfully";
    private static final String UNABLE_TO_CREATE_BUNDLE = "Unable to create debug bundle";

    static final String TARGET_ARG = "target";
    static final String RELATIVE_TARGET_ARG = "relativeTarget";
    static final String NEW_LINE = "\n";

    private final C2Client c2Client;
    private final OperandPropertiesProvider operandPropertiesProvider;
    private final List<Path> bundleFilePaths;
    private final Predicate<String> contentFilter;

    private TransferDebugOperationHandler(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider,
                                          List<Path> bundleFilePaths, Predicate<String> contentFilter) {
        this.c2Client = c2Client;
        this.operandPropertiesProvider = operandPropertiesProvider;
        this.bundleFilePaths = bundleFilePaths;
        this.contentFilter = contentFilter;
    }

    public static TransferDebugOperationHandler create(C2Client c2Client, OperandPropertiesProvider operandPropertiesProvider,
                                                       List<Path> bundleFilePaths, Predicate<String> contentFilter) {
        requires(c2Client != null, "C2Client should not be null");
        requires(operandPropertiesProvider != null, "OperandPropertiesProvider should not be not null");
        requires(bundleFilePaths != null && !bundleFilePaths.isEmpty(), "BundleFilePaths should not be not null or empty");
        requires(contentFilter != null, "Content filter should not be null");
        return new TransferDebugOperationHandler(c2Client, operandPropertiesProvider, bundleFilePaths, contentFilter);
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
        String operationId = ofNullable(operation.getIdentifier()).orElse(EMPTY);

        String callbackUrl;
        try {
            callbackUrl = c2Client.getCallbackUrl(getOperationArg(operation, TARGET_ARG).orElse(EMPTY), getOperationArg(operation, RELATIVE_TARGET_ARG).orElse(EMPTY));
        } catch (Exception e) {
            LOG.error("Callback URL could not be constructed from C2 request and current configuration");
            return operationAck(operationId, operationState(NOT_APPLIED, C2_CALLBACK_URL_NOT_FOUND, e));
        }

        List<Path> preparedFiles = null;
        C2OperationState operationState;
        try {
            preparedFiles = prepareFiles(operationId, bundleFilePaths);
            operationState = createDebugBundle(preparedFiles)
                .map(bundle -> c2Client.uploadBundle(callbackUrl, bundle)
                    .map(errorMessage -> operationState(NOT_APPLIED, errorMessage))
                    .orElseGet(() -> operationState(FULLY_APPLIED, SUCCESSFUL_UPLOAD)))
                .orElseGet(() -> operationState(NOT_APPLIED, UNABLE_TO_CREATE_BUNDLE));
        } catch (Exception e) {
            LOG.error("Unexpected error happened", e);
            operationState = operationState(NOT_APPLIED, UNABLE_TO_CREATE_BUNDLE);
        } finally {
            ofNullable(preparedFiles).ifPresent(this::cleanup);
        }

        LOG.debug("Returning operation ack for operation {} with state {} and details {}", operation.getIdentifier(), operationState.getState(), operationState.getDetails());
        return operationAck(operationId, operationState);
    }

    private List<Path> prepareFiles(String operationId, List<Path> bundleFilePaths) throws IOException {
        List<Path> preparedFiles = new ArrayList<>();
        for (Path bundleFile : bundleFilePaths) {
            Path tempDirectory = createTempDirectory(operationId);
            String fileName = bundleFile.getFileName().toString();

            Path preparedFile = GzipUtils.isCompressedFileName(fileName)
                ? handleGzipFile(bundleFile, Paths.get(tempDirectory.toAbsolutePath().toString(), GzipUtils.getUncompressedFileName(fileName)))
                : handleUncompressedFile(bundleFile, Paths.get(tempDirectory.toAbsolutePath().toString(), fileName));
            preparedFiles.add(preparedFile);
        }
        return preparedFiles;
    }

    private Path handleGzipFile(Path sourceFile, Path targetFile) throws IOException {
        try (GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(sourceFile.toFile()));
             FileOutputStream fileOutputStream = new FileOutputStream(targetFile.toFile())) {
            // no content filter is applied here as flow.json.gz has encoded properties
            gzipInputStream.transferTo(fileOutputStream);
            return targetFile;
        } catch (IOException e) {
            LOG.error("Error during filtering gzip file content: {}", sourceFile.toAbsolutePath(), e);
            throw e;
        }
    }

    private Path handleUncompressedFile(Path sourceFile, Path targetFile) throws IOException {
        try (Stream<String> fileStream = lines(sourceFile, Charset.defaultCharset())) {
            Files.write(targetFile, (Iterable<String>) fileStream.filter(contentFilter)::iterator);
            return targetFile;
        } catch (IOException e) {
            LOG.error("Error during filtering uncompressed file content: {}", sourceFile.toAbsolutePath(), e);
            throw e;
        }
    }

    private Optional<byte[]> createDebugBundle(List<Path> filePaths) {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        try (GzipCompressorOutputStream gzipCompressorOutputStream = new GzipCompressorOutputStream(byteOutputStream);
             TarArchiveOutputStream tarOutputStream = new TarArchiveOutputStream(gzipCompressorOutputStream)) {
            for (Path filePath : filePaths) {
                tarOutputStream.setBigNumberMode(TarArchiveOutputStream.BIGNUMBER_POSIX);
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
