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
package org.apache.nifi.controller;

import org.apache.nifi.controller.scheduling.auto.AutoSchedulingDiagnostics;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FlowControllerTest {

    @Test
    void testPlatformAutomaticDiagnosticsUseProcessorActiveCount() {
        final FlowController flowController = mock(FlowController.class, CALLS_REAL_METHODS);
        final ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processorNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.AUTO);
        when(processorNode.getActiveThreadCount()).thenReturn(7);

        final AutoSchedulingDiagnostics diagnostics = flowController.getAutoSchedulingDiagnostics(processorNode);
        assertEquals(7, diagnostics.activeProcessorInvocations());
    }
}
