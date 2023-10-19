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
package org.apache.nifi.util;

import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.StandardFlowDifference;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFlowDifferenceFilters {

    @Test
    public void testFilterAddedRemotePortsWithRemoteInputPortAsComponentB() {
        VersionedRemoteGroupPort remoteGroupPort = new VersionedRemoteGroupPort();
        remoteGroupPort.setComponentType(ComponentType.REMOTE_INPUT_PORT);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, null, remoteGroupPort, null, null, "");

        // predicate should return false because we don't want to include changes for adding a remote input port
        assertFalse(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }

    @Test
    public void testFilterAddedRemotePortsWithRemoteInputPortAsComponentA() {
        VersionedRemoteGroupPort remoteGroupPort = new VersionedRemoteGroupPort();
        remoteGroupPort.setComponentType(ComponentType.REMOTE_INPUT_PORT);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, remoteGroupPort, null, null, null, "");

        // predicate should return false because we don't want to include changes for adding a remote input port
        assertFalse(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }

    @Test
    public void testFilterAddedRemotePortsWithRemoteOutputPort() {
        VersionedRemoteGroupPort remoteGroupPort = new VersionedRemoteGroupPort();
        remoteGroupPort.setComponentType(ComponentType.REMOTE_OUTPUT_PORT);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, null, remoteGroupPort, null, null, "");

        // predicate should return false because we don't want to include changes for adding a remote input port
        assertFalse(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }

    @Test
    public void testFilterAddedRemotePortsWithNonRemoteInputPort() {
        VersionedProcessor versionedProcessor = new VersionedProcessor();
        versionedProcessor.setComponentType(ComponentType.PROCESSOR);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, null, versionedProcessor, null, null, "");

        // predicate should return true because we do want to include changes for adding a non-port
        assertTrue(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }


    @Test
    public void testFilterPublicPortNameChangeWhenNotNameChange() {
        final VersionedPort portA = new VersionedPort();
        final VersionedPort portB = new VersionedPort();

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED,
                portA, portB,
                "http://localhost:18080", "http://localhost:17080",
                "");

        assertTrue(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES.test(flowDifference));
    }

    @Test
    public void testFilterPublicPortNameChangeWhenNotAllowRemoteAccess() {
        final VersionedPort portA = new VersionedPort();
        final VersionedPort portB = new VersionedPort();

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.NAME_CHANGED,
                portA, portB,
                "Port A", "Port B",
                "");

        assertTrue(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES.test(flowDifference));
    }

    @Test
    public void testFilterPublicPortNameChangeWhenAllowRemoteAccess() {
        final VersionedPort portA = new VersionedPort();
        portA.setAllowRemoteAccess(Boolean.TRUE);

        final VersionedPort portB = new VersionedPort();
        portB.setAllowRemoteAccess(Boolean.FALSE);

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.NAME_CHANGED,
                portA, portB,
                "Port A", "Port B",
                "");

        assertFalse(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES.test(flowDifference));
    }

    @Test
    public void testFilterControllerServiceStatusChangeWhenNewStateIntroduced() {
        final VersionedControllerService controllerServiceA = new VersionedControllerService();
        final VersionedControllerService controllerServiceB = new VersionedControllerService();
        controllerServiceA.setScheduledState(null);
        controllerServiceB.setScheduledState(ScheduledState.DISABLED);

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.SCHEDULED_STATE_CHANGED,
                controllerServiceA, controllerServiceB,
                controllerServiceA.getScheduledState(), controllerServiceB.getScheduledState(),
                "");

        assertTrue(FlowDifferenceFilters.isScheduledStateNew(flowDifference));
    }
}

