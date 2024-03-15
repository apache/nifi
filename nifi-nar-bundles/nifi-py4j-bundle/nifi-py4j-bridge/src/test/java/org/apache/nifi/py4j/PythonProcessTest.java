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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;

import org.apache.nifi.python.ControllerServiceTypeLookup;
import org.apache.nifi.python.PythonProcessConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class PythonProcessTest {

    private PythonProcess pythonProcess;

    @TempDir(cleanup = CleanupMode.ON_SUCCESS)
    private File virtualEnvHome;

    @Mock
    private PythonProcessConfig pythonProcessConfig;

    @Mock
    private ControllerServiceTypeLookup controllerServiceTypeLookup;

    @BeforeEach
    public void setUp() {
        this.pythonProcess = new PythonProcess(this.pythonProcessConfig, this.controllerServiceTypeLookup, virtualEnvHome, "Controller", "Controller");
    }
    @Test
    void testResolvePythonCommandWindows() throws IOException {
        File scriptsDir = new File(virtualEnvHome, "Scripts");
        scriptsDir.mkdir();
        when(pythonProcessConfig.getPythonCommand()).thenReturn("python");
        String result = this.pythonProcess.resolvePythonCommand();
        assertEquals(this.virtualEnvHome.getAbsolutePath() + File.separator + "Scripts" + File.separator + "python", result);
    }

    @Test
    void testResolvePythonCommandUnix() throws IOException {
        File binDir = new File(virtualEnvHome, "bin");
        binDir.mkdir();
        when(pythonProcessConfig.getPythonCommand()).thenReturn("python");
        String result = this.pythonProcess.resolvePythonCommand();
        assertEquals(this.virtualEnvHome.getAbsolutePath() + File.separator + "bin" + File.separator + "python", result);
    }

    @Test
    void testResolvePythonCommandPreferBin() throws IOException {
        File binDir = new File(virtualEnvHome, "bin");
        binDir.mkdir();
        File scriptsDir = new File(virtualEnvHome, "Scripts");
        scriptsDir.mkdir();
        when(pythonProcessConfig.getPythonCommand()).thenReturn("python");
        String result = this.pythonProcess.resolvePythonCommand();
        String expected = this.virtualEnvHome.getAbsolutePath() + File.separator + "bin" + File.separator + "python";
        assertEquals(expected, result);
    }

    @Test
    void testResolvePythonCommandNone() throws IOException {
        when(pythonProcessConfig.getPythonCommand()).thenReturn("python");
        assertThrows(IOException.class, ()-> this.pythonProcess.resolvePythonCommand());
    }

}
