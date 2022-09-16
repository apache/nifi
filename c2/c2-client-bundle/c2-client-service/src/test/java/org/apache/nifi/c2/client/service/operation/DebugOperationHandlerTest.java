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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.write;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;
import static org.apache.nifi.c2.client.service.operation.DebugOperationHandler.NEW_LINE;
import static org.apache.nifi.c2.client.service.operation.DebugOperationHandler.TARGET_ARG;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.DEBUG;
import static org.apache.nifi.c2.protocol.api.OperationType.TRANSFER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.nifi.c2.client.api.C2Client;
import org.apache.nifi.c2.client.service.operation.DebugOperationHandler.ConfigFile;
import org.apache.nifi.c2.client.service.operation.DebugOperationHandler.DebugBundleFile;
import org.apache.nifi.c2.client.service.operation.DebugOperationHandler.LogFile;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class DebugOperationHandlerTest {

    private static final String OPERATION_ID = "operationId";
    private static final String C2_DEBUG_UPLOAD_ENDPOINT = "https://host/c2/api/upload";
    private static final String DEFAULT_FILE_CONTENT = "some_textual_data";
    private static final String VALID_DIRECTORY_NAME = "directory";
    private static final Predicate<String> DEFAULT_CONTENT_FILTER = text -> true;

    @Mock
    private C2Client c2Client;

    @TempDir
    private File confDir;

    @TempDir
    private File logDir;

    private static Stream<Arguments> invalidConstructorArguments() {
        C2Client mockC2Client = mock(C2Client.class);
        return Stream.of(
            Arguments.of(null, null, null, null),
            Arguments.of(null, VALID_DIRECTORY_NAME, VALID_DIRECTORY_NAME, DEFAULT_CONTENT_FILTER),
            Arguments.of(mockC2Client, null, VALID_DIRECTORY_NAME, DEFAULT_CONTENT_FILTER),
            Arguments.of(mockC2Client, VALID_DIRECTORY_NAME, null, DEFAULT_CONTENT_FILTER),
            Arguments.of(mockC2Client, VALID_DIRECTORY_NAME, VALID_DIRECTORY_NAME, null),
            Arguments.of(mockC2Client, "", VALID_DIRECTORY_NAME, DEFAULT_CONTENT_FILTER),
            Arguments.of(mockC2Client, VALID_DIRECTORY_NAME, "", DEFAULT_CONTENT_FILTER)
        );
    }

    @ParameterizedTest
    @MethodSource("invalidConstructorArguments")
    public void testAttemptingCreateWithInvalidParametersWillThrowException(C2Client c2Client, String configDir, String logDir, Predicate<String> contentFilter) {
        assertThrows(IllegalArgumentException.class, () -> DebugOperationHandler.create(c2Client, configDir, logDir, contentFilter));
    }

    @Test
    public void testOperationAndOperandTypesAreMatching() {
        // given
        DebugOperationHandler testHandler = DebugOperationHandler.create(c2Client, confDir.getAbsolutePath(), logDir.getAbsolutePath(), DEFAULT_CONTENT_FILTER);

        // when + then
        assertEquals(TRANSFER, testHandler.getOperationType());
        assertEquals(DEBUG, testHandler.getOperandType());
    }

    @Test
    public void testC2CallbackUrlIsNullInArgs() {
        // given
        DebugOperationHandler testHandler = DebugOperationHandler.create(c2Client, confDir.getAbsolutePath(), logDir.getAbsolutePath(), DEFAULT_CONTENT_FILTER);
        C2Operation c2Operation = operation(null);

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
    }

    @Test
    public void testAllPossibleFilesAreCollectedAndUploadedAsATarGzBundle() {
        // given
        DebugOperationHandler testHandler = DebugOperationHandler.create(c2Client, confDir.getAbsolutePath(), logDir.getAbsolutePath(), DEFAULT_CONTENT_FILTER);
        C2Operation c2Operation = operation(C2_DEBUG_UPLOAD_ENDPOINT);
        createAllPossibleBundleFiles();

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        ArgumentCaptor<String> uploadUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> uploadBundleCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(c2Client).uploadDebugBundle(uploadUrlCaptor.capture(), uploadBundleCaptor.capture());
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(FULLY_APPLIED, result.getOperationState().getState());
        assertEquals(C2_DEBUG_UPLOAD_ENDPOINT, uploadUrlCaptor.getValue());
        Map<String, String> resultBundle = extractBundle(uploadBundleCaptor.getValue());
        assertTrue(mapEqual(expectedBundleForAllPossibleBundleFiles(), resultBundle));
    }

    @Test
    public void testNoFilesAvailableToCollect() {
        // given
        DebugOperationHandler testHandler = DebugOperationHandler.create(c2Client, confDir.getAbsolutePath(), logDir.getAbsolutePath(), DEFAULT_CONTENT_FILTER);
        C2Operation c2Operation = operation(C2_DEBUG_UPLOAD_ENDPOINT);

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
    }

    @Test
    public void testContentIsFilteredOut() {
        // given
        String filterKeyword = "minifi";
        String fileContent = Stream.of("line one", "line two " + filterKeyword, filterKeyword + "line three", "line four").collect(joining(NEW_LINE));
        placeFileWithContent(logDir, LogFile.BOOTSTRAP_LOG_FILE.getFileName(), fileContent);
        placeFileWithContent(confDir, ConfigFile.CONFIG_YML_FILE.getFileName(), fileContent);
        Predicate<String> testContentFilter = content -> !content.contains(filterKeyword);
        DebugOperationHandler testHandler = DebugOperationHandler.create(c2Client, confDir.getAbsolutePath(), logDir.getAbsolutePath(), testContentFilter);
        C2Operation c2Operation = operation(C2_DEBUG_UPLOAD_ENDPOINT);

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        ArgumentCaptor<String> uploadUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> uploadBundleCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(c2Client).uploadDebugBundle(uploadUrlCaptor.capture(), uploadBundleCaptor.capture());
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(FULLY_APPLIED, result.getOperationState().getState());
        assertEquals(C2_DEBUG_UPLOAD_ENDPOINT, uploadUrlCaptor.getValue());
        String expectedFileContent = Stream.of("line one", "line four").collect(joining("\n"));
        Map<String, String> resultBundle = extractBundle(uploadBundleCaptor.getValue());
        assertEquals(2, resultBundle.size());
        assertEquals(expectedFileContent, resultBundle.get(LogFile.BOOTSTRAP_LOG_FILE.getFileName()));
        assertEquals(expectedFileContent, resultBundle.get(ConfigFile.CONFIG_YML_FILE.getFileName()));
    }

    private void createAllPossibleBundleFiles() {
        Stream.of(LogFile.values()).forEach(logFile -> placeFileWithContent(logDir, logFile.getFileName(), DEFAULT_FILE_CONTENT));
        Stream.of(ConfigFile.values()).forEach(configFile -> placeFileWithContent(confDir, configFile.getFileName(), DEFAULT_FILE_CONTENT));
    }

    private void placeFileWithContent(File baseDir, String fileName, String content) {
        try {
            write(Paths.get(baseDir.getAbsolutePath(), fileName), content.getBytes(UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write file to temp directory", e);
        }
    }

    private C2Operation operation(String uploadUrl) {
        C2Operation c2Operation = new C2Operation();
        c2Operation.setIdentifier(OPERATION_ID);
        c2Operation.setArgs(singletonMap(TARGET_ARG, uploadUrl));
        return c2Operation;
    }

    private Map<String, String> extractBundle(byte[] bundle) {
        Map<String, String> fileNamesWithContents = new HashMap<>();
        try (TarArchiveInputStream tarInputStream = new TarArchiveInputStream(new GzipCompressorInputStream(new ByteArrayInputStream(bundle)))) {
            TarArchiveEntry currentEntry;
            while ((currentEntry = tarInputStream.getNextTarEntry()) != null) {
                fileNamesWithContents.put(
                    currentEntry.getName(),
                    new BufferedReader(new InputStreamReader(tarInputStream)).lines().collect(joining(NEW_LINE)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to extract bundle", e);
        }
        return fileNamesWithContents;
    }

    private Map<String, String> expectedBundleForAllPossibleBundleFiles() {
        Stream<DebugBundleFile> logFiles = Stream.of(LogFile.values());
        Stream<DebugBundleFile> configFiles = Stream.of(ConfigFile.values());
        return concat(logFiles, configFiles)
            .map(DebugBundleFile::getFileName)
            .collect(toMap(identity(), __ -> DEFAULT_FILE_CONTENT));
    }

    private boolean mapEqual(Map<String, String> first, Map<String, String> second) {
        if (first.size() != second.size()) {
            return false;
        }
        return first.entrySet()
            .stream()
            .allMatch(e -> e.getValue().equals(second.get(e.getKey())));
    }
}
