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

package org.apache.nifi.minifi.bootstrap.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;
import org.apache.nifi.c2.protocol.api.C2Operation;
import org.apache.nifi.minifi.bootstrap.RunMiNiFi;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeCoordinator;
import org.apache.nifi.minifi.bootstrap.configuration.ConfigurationChangeListener;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BootstrapCodecTest {

    private static final int VALID_PORT = 1;
    private static final String SECRET = "secret";
    private static final String OK = "OK";
    private static final String EMPTY_STRING = "";

    @Mock
    private RunMiNiFi runner;
    @Mock
    private BootstrapFileProvider bootstrapFileProvider;
    @Mock
    private ConfigurationChangeListener configurationChangeListener;
    @Mock
    private UpdateConfigurationService updateConfigurationService;
    @Mock
    private UpdatePropertiesService updatePropertiesService;

    @InjectMocks
    private BootstrapCodec bootstrapCodec;

    @BeforeEach
    void setup() throws IllegalAccessException, NoSuchFieldException {
        mockFinal("updateConfigurationService", updateConfigurationService);
        mockFinal("updatePropertiesService", updatePropertiesService);
    }

    @Test
    void testCommunicateShouldThrowIOExceptionIfThereIsNoCommand() {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThrows(IOException.class, () -> bootstrapCodec.communicate(inputStream, outputStream));
        assertEquals(EMPTY_STRING, outputStream.toString().trim());
        verifyNoInteractions(runner);
    }

    @Test
    void testCommunicateShouldInvalidCommandThrowIoException() {
        String unknown = "unknown";
        InputStream inputStream = new ByteArrayInputStream(unknown.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThrows(IOException.class, () -> bootstrapCodec.communicate(inputStream, outputStream));
        assertEquals(EMPTY_STRING, outputStream.toString().trim());
        verifyNoInteractions(runner);
    }

    @Test
    void testCommunicateShouldSetMiNiFiParametersAndWriteOk() throws IOException {
        String command = "PORT " + VALID_PORT + " " + SECRET;
        InputStream inputStream = new ByteArrayInputStream(command.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        bootstrapCodec.communicate(inputStream, outputStream);

        verify(runner).setMiNiFiParameters(VALID_PORT, SECRET);
        assertEquals(OK, outputStream.toString().trim());
    }

    @ParameterizedTest(name = "{index} => command={0}, expectedExceptionMessage={1}")
    @MethodSource("portCommandValidationInputs")
    void testCommunicateShouldFailWhenReceivesPortCommand(String command) {
        InputStream inputStream = new ByteArrayInputStream(command.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThrows(IOException.class, () -> bootstrapCodec.communicate(inputStream, outputStream));
        assertEquals(EMPTY_STRING, outputStream.toString().trim());
        verifyNoInteractions(runner);
    }

    private static Stream<Arguments> portCommandValidationInputs() {
        return Stream.of(
            Arguments.of("PORT"),
            Arguments.of("PORT invalid secretKey"),
            Arguments.of("PORT 0 secretKey")
        );
    }

    @Test
    void testCommunicateShouldFailIfStartedCommandHasOtherThanOneArg() {
        InputStream inputStream = new ByteArrayInputStream("STARTED".getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThrows(IOException.class, () -> bootstrapCodec.communicate(inputStream, outputStream));
        assertEquals(EMPTY_STRING, outputStream.toString().trim());
        verifyNoInteractions(runner);
    }

    @Test
    void testCommunicateShouldFailIfStartedCommandFirstArgIsNotBoolean() {
        InputStream inputStream = new ByteArrayInputStream("STARTED yes".getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThrows(IOException.class, () -> bootstrapCodec.communicate(inputStream, outputStream));
        assertEquals(EMPTY_STRING, outputStream.toString().trim());
        verifyNoInteractions(runner);
    }

    @Test
    void testCommunicateShouldHandleStartedCommand() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("STARTED true".getBytes(StandardCharsets.UTF_8));
        PeriodicStatusReporterManager periodicStatusReporterManager = mock(PeriodicStatusReporterManager.class);
        ConfigurationChangeCoordinator configurationChangeCoordinator = mock(ConfigurationChangeCoordinator.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        when(runner.getPeriodicStatusReporterManager()).thenReturn(periodicStatusReporterManager);
        when(runner.getConfigurationChangeCoordinator()).thenReturn(configurationChangeCoordinator);

        bootstrapCodec.communicate(inputStream, outputStream);

        assertEquals(OK, outputStream.toString().trim());
        verify(runner, times(2)).getPeriodicStatusReporterManager();
        verify(periodicStatusReporterManager).shutdownPeriodicStatusReporters();
        verify(periodicStatusReporterManager).startPeriodicNotifiers();
        verify(runner).getConfigurationChangeCoordinator();
        verify(configurationChangeCoordinator).start();
        verify(runner).setNiFiStarted(true);
    }

    @Test
    void testCommunicateShouldHandleShutdownCommand() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("SHUTDOWN".getBytes(StandardCharsets.UTF_8));

        PeriodicStatusReporterManager periodicStatusReporterManager = mock(PeriodicStatusReporterManager.class);
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        when(runner.getPeriodicStatusReporterManager()).thenReturn(periodicStatusReporterManager);

        bootstrapCodec.communicate(inputStream, outputStream);

        assertEquals(OK, outputStream.toString().trim());
        verify(runner).getPeriodicStatusReporterManager();
        verify(runner).shutdownChangeNotifier();
        verify(periodicStatusReporterManager).shutdownPeriodicStatusReporters();
    }

    @Test
    void testCommunicateShouldHandleReloadCommand() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("RELOAD".getBytes(StandardCharsets.UTF_8));

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        bootstrapCodec.communicate(inputStream, outputStream);

        assertEquals(OK, outputStream.toString().trim());
    }

    @Test
    void testUpdateConfigurationCommandShouldHandleUpdateConfiguration() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("UPDATE_CONFIGURATION".getBytes(StandardCharsets.UTF_8));
        C2Operation c2Operation = new C2Operation();
        c2Operation.setIdentifier("id");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        bootstrapCodec.communicate(inputStream, outputStream);

        assertEquals(OK, outputStream.toString().trim());
        verify(updateConfigurationService).handleUpdate();
    }

    @Test
    void testUpdatePropertiesCommandShouldHandleUpdateProperties() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("UPDATE_PROPERTIES".getBytes(StandardCharsets.UTF_8));
        C2Operation c2Operation = new C2Operation();
        c2Operation.setIdentifier("id");
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        bootstrapCodec.communicate(inputStream, outputStream);

        assertEquals(OK, outputStream.toString().trim());
        verify(updatePropertiesService).handleUpdate();
    }

    private void mockFinal(String fieldName, Object value) throws NoSuchFieldException, IllegalAccessException {
        Field updateConfigurationServiceField = BootstrapCodec.class.getDeclaredField(fieldName);
        updateConfigurationServiceField.setAccessible(true);
        updateConfigurationServiceField.set(bootstrapCodec, value);
    }
}