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
package org.apache.nifi.fileresource.service;

import org.apache.commons.io.FileUtils;
import org.apache.nifi.fileresource.service.api.FileResource;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StandardFileResourceServiceTest {

    private static final String TEST_NAME = StandardFileResourceServiceTest.class.getSimpleName();

    private static final byte[] TEST_DATA = "nifi".getBytes();

    private static Path directoryPath;

    private TestRunner runner;

    private StandardFileResourceService service;

    @BeforeAll
    static void createTestDirectory() throws IOException {
        directoryPath = Files.createTempDirectory(TEST_NAME);
    }

    @AfterAll
    static void removeTestDirectory() throws IOException {
        FileUtils.deleteDirectory(directoryPath.toFile());
    }

    @BeforeEach
    void setUpRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardFileResourceService();
        runner.addControllerService(TEST_NAME, service);
    }

    @Test
    void testValidFile() throws IOException {
        final Path filePath = createTestFile("validFile");

        setUpService(filePath);

        final FileResource fileResource = service.getFileResource(Collections.emptyMap());

        assertFileResource(fileResource);
    }

    @Test
    void testValidFileUsingEL() throws IOException {
        final Path filePath = createTestFile("validFileUsingEL");

        final Map<String, String> attributes = setUpServiceWithEL(filePath);

        final FileResource fileResource = service.getFileResource(attributes);

        assertFileResource(fileResource);
    }

    @Test
    void testValidFileUsingELButMissingAttribute() throws IOException {
        final Path filePath = createTestFile("testValidFileUsingELButMissingAttribute");

        runner.setValidateExpressionUsage(false);

        setUpServiceWithEL(filePath);

        assertThrows(ProcessException.class, () -> service.getFileResource(Collections.emptyMap()));
    }

    @Test
    void testNonExistingFile() {
        final Path filePath = directoryPath.resolve("nonExistingFile");

        final Map<String, String> attributes = setUpServiceWithEL(filePath);

        assertThrows(ProcessException.class, () -> service.getFileResource(attributes));
    }

    @Test
    void testNonRegularFile() {
        final Path filePath = directoryPath;

        final Map<String, String> attributes = setUpServiceWithEL(filePath);

        assertThrows(ProcessException.class, () -> service.getFileResource(attributes));
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    void testNonReadableFile() throws IOException {
        final Path filePath = createTestFile("nonReadableFile");

        Files.setPosixFilePermissions(filePath, EnumSet.noneOf(PosixFilePermission.class));

        final Map<String, String> attributes = setUpServiceWithEL(filePath);

        assertThrows(ProcessException.class, () -> service.getFileResource(attributes));
    }

    private Path createTestFile(final String filenamePrefix) throws IOException {
        final Path filePath = Files.createTempFile(directoryPath, filenamePrefix, "");
        Files.write(filePath, TEST_DATA);
        return filePath;
    }

    private void setUpService(final Path filePath) {
        setUpService(filePath.toString());
    }

    private void setUpService(final String filePath) {
        runner.setProperty(service, StandardFileResourceService.FILE_PATH, filePath);
        runner.enableControllerService(service);
    }

    private Map<String, String> setUpServiceWithEL(final Path filePath) {
        final String attributeName = "file.path";
        Map<String, String> attributes = Collections.singletonMap(attributeName, filePath.toString());

        setUpService(String.format("${%s}", attributeName));

        return attributes;
    }

    private void assertFileResource(final FileResource fileResource) throws  IOException {
        assertNotNull(fileResource);
        assertEquals(TEST_DATA.length, fileResource.getSize());
        try (final InputStream inputStream = fileResource.getInputStream()) {
            assertArrayEquals(TEST_DATA, inputStream.readAllBytes());
        }
    }
}
