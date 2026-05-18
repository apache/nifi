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
package org.apache.nifi.components.connector.facades.standalone;

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.Backlog;
import org.apache.nifi.components.BacklogReportingException;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.processor.ProcessContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StandaloneProcessorFacadeTest {

    private ProcessorNode processorNode;
    private ComponentContextProvider componentContextProvider;
    private ParameterContext parameterContext;
    private ProcessContext processContext;
    private StandaloneProcessorFacade facade;

    @BeforeEach
    public void setUp() {
        processorNode = mock(ProcessorNode.class);
        final VersionedProcessor versionedProcessor = mock(VersionedProcessor.class);
        final ProcessScheduler scheduler = mock(ProcessScheduler.class);
        parameterContext = mock(ParameterContext.class);
        componentContextProvider = mock(ComponentContextProvider.class);
        final ComponentLog connectorLogger = mock(ComponentLog.class);
        final ExtensionManager extensionManager = mock(ExtensionManager.class);
        final AssetManager assetManager = mock(AssetManager.class);

        processContext = mock(ProcessContext.class);
        when(componentContextProvider.createProcessContext(processorNode, parameterContext)).thenReturn(processContext);

        facade = new StandaloneProcessorFacade(processorNode, versionedProcessor, scheduler, parameterContext,
                componentContextProvider, connectorLogger, extensionManager, assetManager);
    }

    @Test
    public void testReportsBacklogDelegatesToProcessorNode() {
        when(processorNode.supportsBacklogReporting()).thenReturn(true);
        assertTrue(facade.reportsBacklog());

        when(processorNode.supportsBacklogReporting()).thenReturn(false);
        assertFalse(facade.reportsBacklog());
    }

    @Test
    public void testGetBacklogDelegatesToProcessorNode() throws BacklogReportingException {
        final Optional<Backlog> reported = Optional.of(Backlog.builder().records(11L).precision(Backlog.Precision.EXACT).build());
        when(processorNode.getReportedBacklog(any(ProcessContext.class))).thenReturn(reported);

        assertSame(reported, facade.getBacklog());
        verify(processorNode).getReportedBacklog(processContext);
        verify(componentContextProvider).createProcessContext(processorNode, parameterContext);
    }

    @Test
    public void testGetBacklogPropagatesBacklogReportingException() throws BacklogReportingException {
        final BacklogReportingException failure = new BacklogReportingException("Source unreachable");
        when(processorNode.getReportedBacklog(any(ProcessContext.class))).thenThrow(failure);

        final BacklogReportingException thrown = assertThrows(BacklogReportingException.class, facade::getBacklog);
        assertSame(failure, thrown);
    }
}
