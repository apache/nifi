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
package org.apache.nifi.controller.inheritance;

import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowComparison;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * Determines whether or not the proposed flow can be inherited based on whether or not it has all of the Connections that locally have data queued.
 * If the local flow has any connection in which data is queued, that connection must exist in the proposed flow, or else the flow will be considered uninheritable.
 */
public class ConnectionMissingCheck implements FlowInheritabilityCheck {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionMissingCheck.class);

    private final FlowComparison flowComparison;

    public ConnectionMissingCheck(final FlowComparison flowComparison) {
        this.flowComparison = flowComparison;
    }

    @Override
    public FlowInheritability checkInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController flowController) {
        return checkInheritability(existingFlow.getVersionedDataflow(), proposedFlow.getVersionedDataflow(), flowController);
    }

    private FlowInheritability checkInheritability(final VersionedDataflow existingFlow, final VersionedDataflow proposedFlow, final FlowController flowController) {
        // Check for any connections that have been removed. The flow is not inheritable if any connection containing data is removed.
        final FlowManager flowManager = flowController.getFlowManager();

        // If FlowController has not yet been initialized, we need to determine which FlowFiles have queues by checking the FlowFile Repository.
        // If the FlowController has already been initialized, we can just determine which connections have data by checking the connection itself.
        final Set<String> queuesWithFlowFiles;
        if (flowController.isInitialized()) {
            queuesWithFlowFiles = Collections.emptySet();
        } else {
            final FlowFileRepository flowFileRepository = flowController.getRepositoryContextFactory().getFlowFileRepository();

            try {
                queuesWithFlowFiles = flowFileRepository.findQueuesWithFlowFiles(flowController.createSwapManager());
            } catch (final IOException ioe) {
                throw new FlowSynchronizationException("Failed to determine which connections have FlowFiles queued", ioe);
            }

            logger.debug("The following {} Connections/Queues have data queued up currently: {}", queuesWithFlowFiles.size(), queuesWithFlowFiles);
        }

        for (final FlowDifference difference : flowComparison.getDifferences()) {
            final VersionedComponent component = difference.getComponentA();
            if (DifferenceType.COMPONENT_REMOVED == difference.getDifferenceType() && component.getComponentType() == ComponentType.CONNECTION) {
                // Flow Difference indicates that a connection was removed. Need to check if the Connection has data. Depending on whether or not the FlowController has been
                // initialized, the method for doing this is either to check our Set Connection ID's that indicate that a Connection has data, or to just get the Connection itself
                // and check if it has data.
                if (queuesWithFlowFiles.contains(component.getInstanceIdentifier())) {
                    return FlowInheritability.notInheritable("Inheriting cluster's flow would mean removing Connection with ID " + component.getInstanceIdentifier()
                        + ", and the connection has data queued");
                }

                final Connection connection = flowManager.getConnection(component.getInstanceIdentifier());
                if (connection == null) {
                    continue;
                }

                final boolean queueEmpty = connection.getFlowFileQueue().isEmpty();
                if (!queueEmpty) {
                    return FlowInheritability.notInheritable("Inheriting cluster's flow would mean removing Connection with ID " + component.getInstanceIdentifier()
                        + ", and the connection has data queued");
                }
            }
        }

        return FlowInheritability.inheritable();
    }
}
