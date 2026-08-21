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
package org.apache.nifi.toolkit.client.impl;

import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JerseyControllerClientTest {

    private static final String IDENTIFIER = "nar-id";

    private static final byte[] CONTENT = "nar-content".getBytes(StandardCharsets.UTF_8);

    @TempDir
    private Path outputDirectory;

    private Invocation.Builder requestBuilder;

    private JerseyControllerClient client;

    @BeforeEach
    void setUp() {
        final WebTarget baseTarget = mock(WebTarget.class);
        final WebTarget controllerTarget = mock(WebTarget.class);
        final WebTarget contentTarget = mock(WebTarget.class);
        requestBuilder = mock(Invocation.Builder.class);

        when(baseTarget.path("/controller")).thenReturn(controllerTarget);
        when(controllerTarget.path("nar-manager/nars/{identifier}/content")).thenReturn(contentTarget);
        when(contentTarget.resolveTemplate("identifier", IDENTIFIER)).thenReturn(contentTarget);
        when(contentTarget.request()).thenReturn(requestBuilder);
        when(requestBuilder.accept(MediaType.APPLICATION_OCTET_STREAM_TYPE)).thenReturn(requestBuilder);
        client = new JerseyControllerClient(baseTarget);
    }

    @Test
    void testDownloadNarReplacesExistingFile() throws Exception {
        final Path narPath = outputDirectory.resolve("example-1.0.nar");
        Files.writeString(narPath, "existing");
        final Response response = getResponse("attachment; filename=\"example-1.0.nar\"");
        when(requestBuilder.get()).thenReturn(response);

        final File downloaded = client.downloadNar(IDENTIFIER, outputDirectory.toFile());

        assertArrayEquals(CONTENT, Files.readAllBytes(downloaded.toPath()));
    }

    @Test
    void testDownloadNarRejectsTraversalBeforeReadingContent() {
        final Response response = getResponse("attachment; filename=../outside.nar");
        when(requestBuilder.get()).thenReturn(response);
        final Path outsidePath = outputDirectory.resolve("../outside.nar").normalize();

        assertThrows(Exception.class, () -> client.downloadNar(IDENTIFIER, outputDirectory.toFile()));

        assertFalse(Files.exists(outsidePath));
        verify(response, never()).readEntity(java.io.InputStream.class);
    }

    private Response getResponse(final String contentDisposition) {
        final Response response = mock(Response.class);
        when(response.getHeaderString("Content-Disposition")).thenReturn(contentDisposition);
        when(response.readEntity(java.io.InputStream.class)).thenReturn(new ByteArrayInputStream(CONTENT));
        return response;
    }
}
