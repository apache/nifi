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
package org.apache.nifi.runtime.command;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.diagnostics.DiagnosticsDump;
import org.apache.nifi.diagnostics.DiagnosticsFactory;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DiagnosticsCommandTest {
    private static final String DIRECTORY_MAX_SIZE = "1 MB";

    private static final int MAX_FILE_COUNT = 1;

    private static final String DUMP_CONTENT = StandardDiagnosticsDump.class.getSimpleName();

    @Mock
    private NiFiServer server;

    @Mock
    private NiFiProperties properties;

    @Mock
    private DiagnosticsFactory diagnosticsFactory;

    private DiagnosticsCommand command;

    @BeforeEach
    void setCommand() {
        command = new DiagnosticsCommand(properties, server);
    }

    @Test
    void testRunDiagnosticsDisabled() {
        when(properties.isDiagnosticsOnShutdownEnabled()).thenReturn(false);

        command.run();
    }

    @Test
    void testRunDiagnosticsEnabled(@TempDir final File tempDir) throws IOException {
        when(properties.isDiagnosticsOnShutdownEnabled()).thenReturn(true);
        when(properties.getDiagnosticsOnShutdownDirectory()).thenReturn(tempDir.getAbsolutePath());
        when(properties.getDiagnosticsOnShutdownDirectoryMaxSize()).thenReturn(DIRECTORY_MAX_SIZE);
        when(properties.getDiagnosticsOnShutdownMaxFileCount()).thenReturn(MAX_FILE_COUNT);

        when(server.getDiagnosticsFactory()).thenReturn(diagnosticsFactory);
        when(diagnosticsFactory.create(anyBoolean())).thenReturn(new StandardDiagnosticsDump());

        command.run();

        final File[] files = tempDir.listFiles();

        assertNotNull(files);
        assertEquals(MAX_FILE_COUNT, files.length);

        final File file = files[0];
        final String content = Files.readString(file.toPath());
        assertEquals(DUMP_CONTENT, content);
    }

    private static class StandardDiagnosticsDump implements DiagnosticsDump {

        @Override
        public void writeTo(final OutputStream outputStream) throws IOException {
            outputStream.write(DUMP_CONTENT.getBytes(StandardCharsets.UTF_8));
        }
    }
}
