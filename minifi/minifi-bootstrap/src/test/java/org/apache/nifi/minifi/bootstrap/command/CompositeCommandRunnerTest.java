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
import static org.apache.nifi.minifi.bootstrap.Status.OK;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CompositeCommandRunnerTest {

    @Mock
    private CommandRunner startRunner;
    @Mock
    private CommandRunner stopRunner;
    private CompositeCommandRunner compositeCommandRunner;

    @BeforeEach
    void setup() {
        compositeCommandRunner = new CompositeCommandRunner(Arrays.asList(startRunner, stopRunner));
    }

    @Test
    void testRunCommandShouldExecuteCommandsTillFirstNonSuccessStatusCode() {
        when(startRunner.runCommand(any())).thenReturn(ERROR.getStatusCode());

        int statusCode = compositeCommandRunner.runCommand(new String[0]);

        assertEquals(ERROR.getStatusCode(), statusCode);
        verify(startRunner).runCommand(any());
        verifyNoInteractions(stopRunner);
    }

    @Test
    void testRunCommandShouldExecuteCommandsAndReturnOKWhenThereWasNoError() {
        when(startRunner.runCommand(any())).thenReturn(OK.getStatusCode());
        when(stopRunner.runCommand(any())).thenReturn(OK.getStatusCode());

        int statusCode = compositeCommandRunner.runCommand(new String[0]);

        assertEquals(OK.getStatusCode(), statusCode);
        verify(startRunner).runCommand(any());
        verify(stopRunner).runCommand(any());
        verifyNoMoreInteractions(startRunner, stopRunner);
    }
}