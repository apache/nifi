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
package org.apache.nifi.py4j;

import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonProcessConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PythonProcessTest {

    private static final String UNIX_BIN_DIR = "bin";

    private static final String WINDOWS_SCRIPTS_DIR = "Scripts";

    private static final String PYTHON_CMD = "python";

    private PythonProcess pythonProcess;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    private File virtualEnvHome;

    @Mock
    private PythonProcessConfig pythonProcessConfig;

    @Mock
    private ControllerServiceTypeLookup controllerServiceTypeLookup;

    @BeforeEach
    public void setUp() {
        this.pythonProcess = new PythonProcess(this.pythonProcessConfig, this.controllerServiceTypeLookup, virtualEnvHome, false, "Controller", "Controller");
    }

    @Test
    void testUsesConfiguredValueWhenPackagedWithDependencies() throws IOException {
        when(pythonProcessConfig.getPythonCommand()).thenReturn(PYTHON_CMD);
        final PythonProcess process = new PythonProcess(this.pythonProcessConfig, this.controllerServiceTypeLookup, virtualEnvHome, true, "Controller", "Controller");
        assertEquals(PYTHON_CMD, process.resolvePythonCommand());
    }

    @Test
    void testResolvePythonCommandWindows() throws IOException {
        final File scriptsDir = new File(virtualEnvHome, WINDOWS_SCRIPTS_DIR);
        assertTrue(scriptsDir.mkdir());

        when(pythonProcessConfig.getPythonCommand()).thenReturn(PYTHON_CMD);
        final String result = this.pythonProcess.resolvePythonCommand();

        final String expected = getExpectedBinaryPath(WINDOWS_SCRIPTS_DIR);
        assertEquals(expected, result);
    }

    @Test
    void testResolvePythonCommandUnix() throws IOException {
        final File binDir = new File(virtualEnvHome, UNIX_BIN_DIR);
        assertTrue(binDir.mkdir());

        when(pythonProcessConfig.getPythonCommand()).thenReturn(PYTHON_CMD);
        final String result = this.pythonProcess.resolvePythonCommand();

        final String expected = getExpectedBinaryPath(UNIX_BIN_DIR);
        assertEquals(expected, result);
    }

    @Test
    void testResolvePythonCommandFindCommand() throws IOException {
        final File binDir = new File(virtualEnvHome, UNIX_BIN_DIR);
        assertTrue(binDir.mkdir());
        final File scriptsDir = new File(virtualEnvHome, WINDOWS_SCRIPTS_DIR);
        assertTrue(scriptsDir.mkdir());

        final File fakeWindowsPythonExe = new File(scriptsDir, PYTHON_CMD + ".exe");
        assertTrue(fakeWindowsPythonExe.createNewFile());

        when(pythonProcessConfig.getPythonCommand()).thenReturn(PYTHON_CMD);
        final String result = this.pythonProcess.resolvePythonCommand();

        final String expected = getExpectedBinaryPath(WINDOWS_SCRIPTS_DIR);
        assertEquals(expected, result);
    }

    @Test
    void testResolvePythonCommandFindCommandMissingPythonCmd() throws IOException {
        final File binDir = new File(virtualEnvHome, UNIX_BIN_DIR);
        assertTrue(binDir.mkdir());
        final File scriptsDir = new File(virtualEnvHome, WINDOWS_SCRIPTS_DIR);
        assertTrue(scriptsDir.mkdir());

        when(pythonProcessConfig.getPythonCommand()).thenReturn(PYTHON_CMD);

        assertThrows(IOException.class, () -> this.pythonProcess.resolvePythonCommand());
    }

    @Test
    void testResolvePythonCommandNone() {
        when(pythonProcessConfig.getPythonCommand()).thenReturn(PYTHON_CMD);
        assertThrows(IOException.class, () -> this.pythonProcess.resolvePythonCommand());
    }

    private String getExpectedBinaryPath(String binarySubDirectoryName) {
        return this.virtualEnvHome.getAbsolutePath() + File.separator + binarySubDirectoryName + File.separator + PYTHON_CMD;
    }

    /**
     * Tests that the PythonProcess can be shutdown even if it hasn't been started.
     */
    @Test
    void testShutdownBeforeStart() {
        // Should not throw any exception
        pythonProcess.shutdown();
        assertTrue(pythonProcess.isShutdown(), "Process should be marked as shutdown");
    }

    /**
     * Tests that isShutdown() returns correct values.
     */
    @Test
    void testIsShutdownInitialState() {
        assertFalse(pythonProcess.isShutdown(), "Process should not be shutdown initially");
    }

    /**
     * Tests that shutdown can be called multiple times without issues.
     */
    @Test
    void testMultipleShutdownCalls() {
        pythonProcess.shutdown();
        assertTrue(pythonProcess.isShutdown());

        // Second shutdown should not throw
        pythonProcess.shutdown();
        assertTrue(pythonProcess.isShutdown());

        // Third shutdown should still work
        pythonProcess.shutdown();
        assertTrue(pythonProcess.isShutdown());
    }


    /**
     * Tests that shutdown properly cleans up even when called during initialization.
     * This simulates the scenario where NAR deletion triggers shutdown during venv creation.
     */
    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void testShutdownDuringInitializationPreparation() {
        // Shutdown immediately - this simulates early cancellation
        pythonProcess.shutdown();

        // Process should be marked as shutdown
        assertTrue(pythonProcess.isShutdown(),
                "Process should be marked as shutdown immediately after shutdown() is called");
    }
}
