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
package org.apache.nifi.registry.client.impl;

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ClientUtilsTest {

    private static final byte[] CONTENT = "bundle-content".getBytes(StandardCharsets.UTF_8);

    @TempDir
    private Path outputDirectory;

    @Test
    void testWriteBundleContent() throws Exception {
        final Response response = getResponse("attachment; filename = example-1.0.nar");

        final File bundleFile = ClientUtils.getExtensionBundleVersionContent(response, outputDirectory.toFile());

        assertEquals(outputDirectory.resolve("example-1.0.nar"), bundleFile.toPath());
        assertArrayEquals(CONTENT, Files.readAllBytes(bundleFile.toPath()));
    }

    @Test
    void testWriteBundleContentReplacesExistingFile() throws Exception {
        final Path bundlePath = outputDirectory.resolve("example-1.0.nar");
        Files.writeString(bundlePath, "existing");
        final Response response = getResponse("attachment; filename=\"example-1.0.nar\"");

        ClientUtils.getExtensionBundleVersionContent(response, outputDirectory.toFile());

        assertArrayEquals(CONTENT, Files.readAllBytes(bundlePath));
    }

    @Test
    void testRejectTraversalBeforeReadingContent() {
        final Response response = getResponse("attachment; filename=../outside.nar");
        final Path outsidePath = outputDirectory.resolve("../outside.nar").normalize();

        assertThrows(IllegalStateException.class, () -> ClientUtils.getExtensionBundleVersionContent(response, outputDirectory.toFile()));

        assertFalse(Files.exists(outsidePath));
        verify(response, never()).readEntity(ByteArrayInputStream.class);
        verify(response, never()).readEntity(java.io.InputStream.class);
    }

    @Test
    void testRejectWindowsTraversal() {
        final Response response = getResponse("attachment; filename=..\\outside.nar");

        assertThrows(IllegalStateException.class, () -> ClientUtils.getExtensionBundleVersionContent(response, outputDirectory.toFile()));
    }

    @Test
    void testRejectAbsolutePath() {
        final Response response = getResponse("attachment; filename=/outside.nar");

        assertThrows(IllegalStateException.class, () -> ClientUtils.getExtensionBundleVersionContent(response, outputDirectory.toFile()));
    }

    @Test
    void testRejectControlCharacter() {
        final Response response = getResponse("attachment; filename=control\u0001.nar");

        assertThrows(IllegalStateException.class, () -> ClientUtils.getExtensionBundleVersionContent(response, outputDirectory.toFile()));
    }

    @Test
    void testRejectMissingHeader() {
        final Response response = mock(Response.class);

        assertThrows(IllegalStateException.class, () -> ClientUtils.getExtensionBundleVersionContent(response, outputDirectory.toFile()));
    }

    private Response getResponse(final String contentDisposition) {
        final Response response = mock(Response.class);
        when(response.getHeaderString("Content-Disposition")).thenReturn(contentDisposition);
        when(response.readEntity(java.io.InputStream.class)).thenReturn(new ByteArrayInputStream(CONTENT));
        return response;
    }
}
