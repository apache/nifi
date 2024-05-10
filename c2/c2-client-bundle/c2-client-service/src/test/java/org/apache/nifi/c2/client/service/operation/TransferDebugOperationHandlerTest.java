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
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.nifi.c2.client.service.operation.TransferDebugOperationHandler.NEW_LINE;
import static org.apache.nifi.c2.client.service.operation.TransferDebugOperationHandler.TARGET_ARG;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.FULLY_APPLIED;
import static org.apache.nifi.c2.protocol.api.C2OperationState.OperationState.NOT_APPLIED;
import static org.apache.nifi.c2.protocol.api.OperandType.DEBUG;
import static org.apache.nifi.c2.protocol.api.OperationType.TRANSFER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.nifi.c2.client.api.C2Client;
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
public class TransferDebugOperationHandlerTest {

    private static final String OPERATION_ID = "operationId";
    private static final String C2_DEBUG_UPLOAD_ENDPOINT = "https://host/c2/api/upload";
    private static final String DEFAULT_FILE_CONTENT = "some_textual_data";
    private static final List<Path> VALID_BUNDLE_FILE_LIST = singletonList(Paths.get("path_to_file"));
    private static final Predicate<String> DEFAULT_CONTENT_FILTER = text -> true;

    @Mock
    private C2Client c2Client;

    @Mock
    private OperandPropertiesProvider operandPropertiesProvider;

    @TempDir
    private File tempDir;

    private static Stream<Arguments> invalidConstructorArguments() {
        C2Client mockC2Client = mock(C2Client.class);
        return Stream.of(
            Arguments.of(null, null, null),
            Arguments.of(null, VALID_BUNDLE_FILE_LIST, DEFAULT_CONTENT_FILTER),
            Arguments.of(mockC2Client, null, DEFAULT_CONTENT_FILTER),
            Arguments.of(mockC2Client, emptyList(), DEFAULT_CONTENT_FILTER),
            Arguments.of(mockC2Client, VALID_BUNDLE_FILE_LIST, null)
        );
    }

    @ParameterizedTest(name = "c2Client={0} bundleFileList={1} contentFilter={2}")
    @MethodSource("invalidConstructorArguments")
    public void testAttemptingCreateWithInvalidParametersWillThrowException(C2Client c2Client, List<Path> bundleFilePaths, Predicate<String> contentFilter) {
        assertThrows(IllegalArgumentException.class, () -> TransferDebugOperationHandler.create(c2Client, operandPropertiesProvider, bundleFilePaths, contentFilter));
    }

    @Test
    public void testOperationAndOperandTypesAreMatching() {
        // given
        TransferDebugOperationHandler testHandler = TransferDebugOperationHandler.create(c2Client, operandPropertiesProvider, VALID_BUNDLE_FILE_LIST, DEFAULT_CONTENT_FILTER);

        // when + then
        assertEquals(TRANSFER, testHandler.getOperationType());
        assertEquals(DEBUG, testHandler.getOperandType());
    }

    @Test
    public void testC2CallbackUrlIsNullInArgs() {
        // given
        TransferDebugOperationHandler testHandler = TransferDebugOperationHandler.create(c2Client, operandPropertiesProvider, VALID_BUNDLE_FILE_LIST, DEFAULT_CONTENT_FILTER);
        C2Operation c2Operation = operation(null);

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
    }

    @Test
    public void testFilesAreCollectedAndUploadedAsATarGzBundle() {
        // given
        Map<String, String> bundleFileNamesWithContents = Stream.of("file.log", "application.conf", "default.properties")
            .collect(toMap(identity(), __ -> DEFAULT_FILE_CONTENT));
        List<Path> createBundleFiles = bundleFileNamesWithContents.entrySet().stream()
            .map(entry -> placeFileWithContent(entry.getKey(), entry.getValue()))
            .collect(toList());
        TransferDebugOperationHandler testHandler = TransferDebugOperationHandler.create(c2Client, operandPropertiesProvider, createBundleFiles, DEFAULT_CONTENT_FILTER);
        C2Operation c2Operation = operation(C2_DEBUG_UPLOAD_ENDPOINT);
        when(c2Client.getCallbackUrl(any(), any())).thenReturn(Optional.of(C2_DEBUG_UPLOAD_ENDPOINT));

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        ArgumentCaptor<String> uploadUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> uploadBundleCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(c2Client).uploadBundle(uploadUrlCaptor.capture(), uploadBundleCaptor.capture());
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(FULLY_APPLIED, result.getOperationState().getState());
        assertEquals(C2_DEBUG_UPLOAD_ENDPOINT, uploadUrlCaptor.getValue());
        Map<String, String> resultBundle = extractBundle(uploadBundleCaptor.getValue());
        assertTrue(mapEqual(bundleFileNamesWithContents, resultBundle));
    }

    @Test
    public void testFileToCollectDoesNotExist() {
        // given
        TransferDebugOperationHandler testHandler = TransferDebugOperationHandler.create(c2Client, operandPropertiesProvider,
            singletonList(Paths.get(tempDir.getAbsolutePath(), "missing_file")), DEFAULT_CONTENT_FILTER);
        C2Operation c2Operation = operation(C2_DEBUG_UPLOAD_ENDPOINT);

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(NOT_APPLIED, result.getOperationState().getState());
    }

    private static Stream<Arguments> contentFilterArguments() {
        String filterKeyword = "minifi";
        return Stream.of(
            Arguments.of(
                "files_containing_keyword_filtered_out.file",
                filterKeyword,
                Stream.of("line one", "line two " + filterKeyword, filterKeyword + "line three", "line four", "line " + filterKeyword + " five").collect(joining(NEW_LINE)),
                Stream.of("line one", "line four").collect(joining(NEW_LINE))),
            Arguments.of(
                "all_content_filtered_out.file",
                filterKeyword,
                Stream.of("line one " + filterKeyword, filterKeyword, filterKeyword + "line three",
                    filterKeyword + "line four" + filterKeyword, "line " + filterKeyword + " five").collect(joining(NEW_LINE)),
                ""),
            Arguments.of(
                "all_content_kept.file",
                filterKeyword,
                Stream.of("line one", "line two", "line three", "line four", "line five").collect(joining(NEW_LINE)),
                Stream.of("line one", "line two", "line three", "line four", "line five").collect(joining(NEW_LINE)))
        );
    }

    @ParameterizedTest(name = "file={0}")
    @MethodSource("contentFilterArguments")
    public void testContentIsFilteredOut(String fileName, String filterKeyword, String inputContent, String expectedContent) {
        // given
        Path bundleFile = placeFileWithContent(fileName, inputContent);
        Predicate<String> testContentFilter = content -> !content.contains(filterKeyword);
        TransferDebugOperationHandler testHandler = TransferDebugOperationHandler.create(c2Client, operandPropertiesProvider, singletonList(bundleFile), testContentFilter);
        C2Operation c2Operation = operation(C2_DEBUG_UPLOAD_ENDPOINT);
        when(c2Client.getCallbackUrl(any(), any())).thenReturn(Optional.of(C2_DEBUG_UPLOAD_ENDPOINT));

        // when
        C2OperationAck result = testHandler.handle(c2Operation);

        // then
        ArgumentCaptor<String> uploadUrlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<byte[]> uploadBundleCaptor = ArgumentCaptor.forClass(byte[].class);
        verify(c2Client).uploadBundle(uploadUrlCaptor.capture(), uploadBundleCaptor.capture());
        assertEquals(OPERATION_ID, result.getOperationId());
        assertEquals(FULLY_APPLIED, result.getOperationState().getState());
        assertEquals(C2_DEBUG_UPLOAD_ENDPOINT, uploadUrlCaptor.getValue());
        Map<String, String> resultBundle = extractBundle(uploadBundleCaptor.getValue());
        assertEquals(1, resultBundle.size());
        assertEquals(expectedContent, resultBundle.get(fileName));
    }

    private Path placeFileWithContent(String fileName, String content) {
        Path filePath = Paths.get(tempDir.getAbsolutePath(), fileName);
        try {
            write(filePath, content.getBytes(UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write file to temp directory", e);
        }
        return filePath;
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
            while ((currentEntry = tarInputStream.getNextEntry()) != null) {
                fileNamesWithContents.put(
                    currentEntry.getName(),
                    new BufferedReader(new InputStreamReader(tarInputStream)).lines().collect(joining(NEW_LINE)));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to extract bundle", e);
        }
        return fileNamesWithContents;
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
