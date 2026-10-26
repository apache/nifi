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

package org.apache.nifi.registry.flow.diff;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConfigurableComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

class RebaseHandlerUtils {

    private RebaseHandlerUtils() {
    }

    static VersionedComponent findComponentById(final VersionedProcessGroup group, final String identifier) {
        if (identifier.equals(group.getIdentifier())) {
            return group;
        }

        for (final VersionedProcessor processor : group.getProcessors()) {
            if (identifier.equals(processor.getIdentifier())) {
                return processor;
            }
        }
        for (final VersionedConnection connection : group.getConnections()) {
            if (identifier.equals(connection.getIdentifier())) {
                return connection;
            }
        }
        for (final VersionedLabel label : group.getLabels()) {
            if (identifier.equals(label.getIdentifier())) {
                return label;
            }
        }
        for (final VersionedFunnel funnel : group.getFunnels()) {
            if (identifier.equals(funnel.getIdentifier())) {
                return funnel;
            }
        }
        for (final VersionedPort inputPort : group.getInputPorts()) {
            if (identifier.equals(inputPort.getIdentifier())) {
                return inputPort;
            }
        }
        for (final VersionedPort outputPort : group.getOutputPorts()) {
            if (identifier.equals(outputPort.getIdentifier())) {
                return outputPort;
            }
        }
        for (final VersionedControllerService service : group.getControllerServices()) {
            if (identifier.equals(service.getIdentifier())) {
                return service;
            }
        }
        for (final VersionedRemoteProcessGroup remoteProcessGroup : group.getRemoteProcessGroups()) {
            if (identifier.equals(remoteProcessGroup.getIdentifier())) {
                return remoteProcessGroup;
            }
            final VersionedRemoteGroupPort remoteInputPort = findRemoteGroupPortById(remoteProcessGroup.getInputPorts(), identifier);
            if (remoteInputPort != null) {
                return remoteInputPort;
            }
            final VersionedRemoteGroupPort remoteOutputPort = findRemoteGroupPortById(remoteProcessGroup.getOutputPorts(), identifier);
            if (remoteOutputPort != null) {
                return remoteOutputPort;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedComponent result = findComponentById(childGroup, identifier);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    private static VersionedRemoteGroupPort findRemoteGroupPortById(final Set<VersionedRemoteGroupPort> remotePorts, final String identifier) {
        if (remotePorts == null) {
            return null;
        }
        for (final VersionedRemoteGroupPort remotePort : remotePorts) {
            if (identifier.equals(remotePort.getIdentifier())) {
                return remotePort;
            }
        }
        return null;
    }

    /**
     * Finds an upstream difference of the same type on the same component whose new value differs from the local new value.
     */
    static Optional<FlowDifference> findConflictingUpstreamDifference(final FlowDifference localDifference, final Set<FlowDifference> upstreamDifferences) {
        final String componentIdentifier = localDifference.getComponentB().getIdentifier();
        for (final FlowDifference upstreamDifference : upstreamDifferences) {
            if (upstreamDifference.getDifferenceType() == localDifference.getDifferenceType()
                    && upstreamDifference.getComponentA() != null
                    && componentIdentifier.equals(upstreamDifference.getComponentA().getIdentifier())
                    && !Objects.equals(localDifference.getValueB(), upstreamDifference.getValueB())) {
                return Optional.of(upstreamDifference);
            }
        }
        return Optional.empty();
    }

    static VersionedProcessGroup findProcessGroupById(final VersionedProcessGroup group, final String identifier) {
        if (identifier.equals(group.getIdentifier()) || identifier.equals(group.getInstanceIdentifier())) {
            return group;
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedProcessGroup result = findProcessGroupById(childGroup, identifier);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    static VersionedConnection findConnectionById(final VersionedProcessGroup group, final String identifier) {
        for (final VersionedConnection connection : group.getConnections()) {
            if (identifier.equals(connection.getIdentifier())) {
                return connection;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedConnection result = findConnectionById(childGroup, identifier);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

    static VersionedConfigurableComponent findConfigurableComponentById(final VersionedProcessGroup group, final String identifier) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (identifier.equals(processor.getIdentifier())) {
                return processor;
            }
        }
        for (final VersionedControllerService service : group.getControllerServices()) {
            if (identifier.equals(service.getIdentifier())) {
                return service;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            final VersionedConfigurableComponent result = findConfigurableComponentById(childGroup, identifier);
            if (result != null) {
                return result;
            }
        }

        return null;
    }

}
