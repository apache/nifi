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

package org.apache.nifi.web.dao.impl;

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.FlowRegistryClientUserContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.FlowVersionLocation;
import org.apache.nifi.web.NiFiCoreException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardFlowRegistryDAOTest {

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private FlowRegistryClientUserContext userContext;

    private StandardFlowRegistryDAO dao;

    @BeforeEach
    void setUp() {
        dao = new StandardFlowRegistryDAO();
        dao.setFlowController(flowController);

        when(flowController.getFlowManager()).thenReturn(flowManager);
    }

    @Test
    void testCreateBranchDelegatesToRegistryClient() throws IOException, FlowRegistryException {
        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        when(flowManager.getFlowRegistryClient("registry-1")).thenReturn(clientNode);

        final FlowVersionLocation sourceLocation = new FlowVersionLocation("main", "bucket", "flow", "1");

        dao.createBranchForUser(userContext, "registry-1", sourceLocation, "feature");

        ArgumentCaptor<FlowVersionLocation> locationCaptor = ArgumentCaptor.forClass(FlowVersionLocation.class);
        verify(clientNode).createBranch(eq(userContext), locationCaptor.capture(), eq("feature"));

        final FlowVersionLocation capturedLocation = locationCaptor.getValue();
        assertEquals("main", capturedLocation.getBranch());
        assertEquals("bucket", capturedLocation.getBucketId());
        assertEquals("flow", capturedLocation.getFlowId());
        assertEquals("1", capturedLocation.getVersion());
    }

    @Test
    void testCreateBranchUnsupported() throws IOException, FlowRegistryException {
        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        when(flowManager.getFlowRegistryClient("registry-1")).thenReturn(clientNode);
        final FlowVersionLocation sourceLocation = new FlowVersionLocation("main", "bucket", "flow", "1");

        doThrow(new UnsupportedOperationException("not supported"))
                .when(clientNode)
                .createBranch(userContext, sourceLocation, "feature");

        assertThrows(UnsupportedOperationException.class,
                () -> dao.createBranchForUser(userContext, "registry-1", sourceLocation, "feature"));
    }

    @Test
    void testCreateBranchUnknownRegistry() {
        when(flowManager.getFlowRegistryClient("missing")).thenReturn(null);

        final FlowVersionLocation sourceLocation = new FlowVersionLocation("main", "bucket", "flow", "1");
        assertThrows(IllegalArgumentException.class,
                () -> dao.createBranchForUser(userContext, "missing", sourceLocation, "feature"));
    }

    @Test
    void testCreateBranchFlowRegistryExceptionWrapped() throws IOException, FlowRegistryException {
        final FlowRegistryClientNode clientNode = mock(FlowRegistryClientNode.class);
        when(flowManager.getFlowRegistryClient("registry-1")).thenReturn(clientNode);

        final FlowVersionLocation sourceLocation = new FlowVersionLocation("main", "bucket", "flow", "1");
        final FlowRegistryException cause = new FlowRegistryException("Branch [feature] already exists");
        doThrow(cause)
                .when(clientNode)
                .createBranch(userContext, sourceLocation, "feature");

        final NiFiCoreException exception = assertThrows(NiFiCoreException.class,
                () -> dao.createBranchForUser(userContext, "registry-1", sourceLocation, "feature"));

        assertEquals("Unable to create branch [feature] in registry with ID registry-1: Branch [feature] already exists", exception.getMessage());
        assertEquals(cause, exception.getCause());
    }
}
