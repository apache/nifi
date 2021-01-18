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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.serialization.FlowEncodingVersion;
import org.apache.nifi.controller.serialization.FlowFromDOMFactory;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Determines whether or not the proposed flow can be inherited based on whether or not it has all of the Connections that locally have data queued.
 * If the local flow has any connection in which data is queued, that connection must exist in the proposed flow, or else the flow will be considered uninheritable.
 */
public class ConnectionMissingCheck implements FlowInheritabilityCheck {
    private static final Logger logger = LoggerFactory.getLogger(ConnectionMissingCheck.class);

    @Override
    public FlowInheritability checkInheritability(final DataFlow existingFlow, final DataFlow proposedFlow, final FlowController flowController) {
        final Document flowDocument = proposedFlow.getFlowDocument();
        final Element rootGroupElement = (Element) flowDocument.getDocumentElement().getElementsByTagName("rootGroup").item(0);
        final FlowEncodingVersion encodingVersion = FlowEncodingVersion.parse(flowDocument.getDocumentElement());

        final ProcessGroupDTO rootGroupDto = FlowFromDOMFactory.getProcessGroup(null, rootGroupElement, null, encodingVersion);
        final Set<String> connectionIds = findAllConnectionIds(rootGroupDto);

        final FlowFileRepository flowFileRepository = flowController.getRepositoryContextFactory().getFlowFileRepository();

        final Set<String> queuesWithFlowFiles;
        try {
            queuesWithFlowFiles = flowFileRepository.findQueuesWithFlowFiles(flowController.createSwapManager());
        } catch (final IOException ioe) {
            throw new FlowSynchronizationException("Failed to determine which connections have FlowFiles queued", ioe);
        }

        logger.debug("The following {} Connections/Queues have data queued up currently: {}", queuesWithFlowFiles.size(), queuesWithFlowFiles);

        for (final String queueId : queuesWithFlowFiles) {
            if (!connectionIds.contains(queueId)) {
                return FlowInheritability.notInheritable("Proposed Flow does not contain a Connection with ID " + queueId + " but this instance has data queued in that connection");
            }
        }

        return FlowInheritability.inheritable();
    }

    private Set<String> findAllConnectionIds(final ProcessGroupDTO group) {
        final Set<String> connectionIds = new HashSet<>();
        findAllConnectionIds(group, connectionIds);
        return connectionIds;
    }

    private void findAllConnectionIds(final ProcessGroupDTO group, final Set<String> ids) {
        for (final ConnectionDTO connectionDTO : group.getContents().getConnections()) {
            ids.add(connectionDTO.getId());
        }

        for (final ProcessGroupDTO childGroup : group.getContents().getProcessGroups()) {
            findAllConnectionIds(childGroup, ids);
        }
    }
}
