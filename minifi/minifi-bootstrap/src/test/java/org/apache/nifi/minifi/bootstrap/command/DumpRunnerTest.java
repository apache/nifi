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

package org.apache.nifi.minifi.bootstrap.command;

import static org.apache.nifi.minifi.bootstrap.Status.ERROR;
import static org.apache.nifi.minifi.bootstrap.Status.MINIFI_NOT_RUNNING;
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.apache.nifi.minifi.bootstrap.command.DumpRunner.DUMP_CMD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import org.apache.nifi.minifi.bootstrap.service.CurrentPortProvider;
import org.apache.nifi.minifi.bootstrap.service.MiNiFiCommandSender;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DumpRunnerTest {

    private static final int MINIFI_PORT = 1337;
    private static final String DUMP_CONTENT = "dump_content";

    @Mock
    private MiNiFiCommandSender miNiFiCommandSender;
    @Mock
    private CurrentPortProvider currentPortProvider;

    @InjectMocks
    private DumpRunner dumpRunner;

    @Test
    void testRunCommandShouldDumpToConsoleIfNoFileDefined() throws IOException {
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(miNiFiCommandSender.sendCommand(DUMP_CMD, MINIFI_PORT)).thenReturn(Optional.of(DUMP_CONTENT));

        int statusCode = dumpRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), statusCode);
        verifyNoMoreInteractions(currentPortProvider, miNiFiCommandSender);
    }

    @Test
    void testRunCommandShouldDumpToFileIfItIsDefined() throws IOException {
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(miNiFiCommandSender.sendCommand(DUMP_CMD, MINIFI_PORT)).thenReturn(Optional.of(DUMP_CONTENT));
        File file = Files.createTempFile(null, null).toFile();
        file.deleteOnExit();
        String tmpFilePath = file.getAbsolutePath();

        int statusCode = dumpRunner.runCommand(new  String[] {DUMP_CMD, tmpFilePath});

        assertEquals(OK.getStatusCode(), statusCode);
        assertEquals(DUMP_CONTENT, getDumpContent(file));
        verifyNoMoreInteractions(currentPortProvider, miNiFiCommandSender);
    }

    @Test
    void testRunCommandShouldReturnNotRunningStatusCodeIfPortReturnsNull() {
        when(currentPortProvider.getCurrentPort()).thenReturn(null);

        int statusCode = dumpRunner.runCommand(new String[]{});

        assertEquals(MINIFI_NOT_RUNNING.getStatusCode(), statusCode);
        verifyNoMoreInteractions(currentPortProvider);
        verifyNoInteractions(miNiFiCommandSender);
    }

    @Test
    void testRunCommandShouldReturnErrorStatusCodeIfSendCommandThrowsException() throws IOException {
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(miNiFiCommandSender.sendCommand(DUMP_CMD, MINIFI_PORT)).thenThrow(new IOException());

        int statusCode = dumpRunner.runCommand(new String[]{});

        assertEquals(ERROR.getStatusCode(), statusCode);
        verifyNoMoreInteractions(currentPortProvider, miNiFiCommandSender);
    }

    @Test
    void testRunCommandShouldReturnErrorStatusCodeIfFileWriteFailureHappens() throws IOException {
        when(currentPortProvider.getCurrentPort()).thenReturn(MINIFI_PORT);
        when(miNiFiCommandSender.sendCommand(DUMP_CMD, MINIFI_PORT)).thenReturn(Optional.ofNullable(DUMP_CONTENT));
        File file = Files.createTempFile(null, null).toFile();
        file.deleteOnExit();
        file.setReadOnly();
        String tmpFilePath = file.getAbsolutePath();

        int statusCode = dumpRunner.runCommand(new  String[] {DUMP_CMD, tmpFilePath});

        assertEquals(ERROR.getStatusCode(), statusCode);
        verifyNoMoreInteractions(currentPortProvider, miNiFiCommandSender);
    }

    private String getDumpContent(File dumpFile) {
        String fileLines = null;
        if (dumpFile.exists()) {
            try {
                fileLines = new String(Files.readAllBytes(dumpFile.toPath()));
            } catch (IOException e) {
                fileLines = null;
            }
        }
        return fileLines;
    }

}