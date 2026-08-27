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

import jakarta.ws.rs.core.Response;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AbstractJerseyClientTest {

    @TempDir
    private Path outputDirectory;

    private final TestJerseyClient client = new TestJerseyClient();

    @Test
    void testGetContentDispositionFile() {
        final Response response = getResponse("attachment; filename=\"example file-1.0.nar\"");

        final File destination = client.getContentDispositionFile(response, outputDirectory.toFile());

        assertEquals(outputDirectory.resolve("example file-1.0.nar"), destination.toPath());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "../outside.nar",
            "..\\outside.nar",
            "nested/outside.nar",
            "nested\\outside.nar",
            "/outside.nar",
            ".",
            "..",
            "control\u0001.nar"
    })
    void testRejectUnsafeContentDispositionFilename(final String filename) {
        final Response response = getResponse("attachment; filename=\"%s\"".formatted(filename));

        assertThrows(IllegalStateException.class, () -> client.getContentDispositionFile(response, outputDirectory.toFile()));
    }

    @Test
    void testRejectMissingFilename() {
        final Response response = getResponse("attachment");

        assertThrows(IllegalStateException.class, () -> client.getContentDispositionFile(response, outputDirectory.toFile()));
    }

    @Test
    void testRejectMissingHeader() {
        final Response response = mock(Response.class);

        assertThrows(IllegalStateException.class, () -> client.getContentDispositionFile(response, outputDirectory.toFile()));
    }

    @Test
    void testGetContentDispositionFileUnquoted() {
        final Response response = getResponse("attachment; filename = example-1.0.nar");

        final File destination = client.getContentDispositionFile(response, outputDirectory.toFile());

        assertEquals(outputDirectory.resolve("example-1.0.nar"), destination.toPath());
    }

    private Response getResponse(final String contentDisposition) {
        final Response response = mock(Response.class);
        when(response.getHeaderString("Content-Disposition")).thenReturn(contentDisposition);
        return response;
    }

    private static class TestJerseyClient extends AbstractJerseyClient {

        TestJerseyClient() {
            super(null);
        }

        @Override
        protected File getContentDispositionFile(final Response response, final File outputDirectory) {
            return super.getContentDispositionFile(response, outputDirectory);
        }
    }
}
