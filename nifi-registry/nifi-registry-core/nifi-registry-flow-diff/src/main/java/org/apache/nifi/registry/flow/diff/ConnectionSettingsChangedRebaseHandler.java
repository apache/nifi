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
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Handles back pressure threshold, FlowFile expiration and prioritizer changes on connections. An upstream change to the
 * same setting of the same connection conflicts with the local change unless it results in the same value.
 */
public class ConnectionSettingsChangedRebaseHandler implements RebaseHandler {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionSettingsChangedRebaseHandler.class);

    private static final Set<DifferenceType> SUPPORTED_TYPES = Set.of(
            DifferenceType.BACKPRESSURE_OBJECT_THRESHOLD_CHANGED,
            DifferenceType.BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED,
            DifferenceType.FLOWFILE_EXPIRATION_CHANGED,
            DifferenceType.PRIORITIZERS_CHANGED);

    private final DifferenceType supportedType;

    public ConnectionSettingsChangedRebaseHandler(final DifferenceType supportedType) {
        if (!SUPPORTED_TYPES.contains(supportedType)) {
            throw new IllegalArgumentException("Unsupported difference type " + supportedType);
        }
        this.supportedType = supportedType;
    }

    @Override
    public DifferenceType getSupportedType() {
        return supportedType;
    }

    @Override
    public RebaseAnalysis.ClassifiedDifference classify(final FlowDifference localDifference, final Set<FlowDifference> upstreamDifferences,
                                                        final VersionedProcessGroup targetSnapshot) {
        final VersionedComponent localComponent = localDifference.getComponentB();
        final String componentIdentifier = localComponent.getIdentifier();
        if (!(localComponent instanceof VersionedConnection)) {
            return RebaseAnalysis.ClassifiedDifference.unsupported(localDifference, RebaseConflictCode.UNSUPPORTED_COMPONENT_TYPE,
                    "Connection setting change is not supported for component %s of type %s".formatted(componentIdentifier, localComponent.getComponentType()));
        }
        if (RebaseHandlerUtils.findConflictingUpstreamDifference(localDifference, upstreamDifferences).isPresent()) {
            return RebaseAnalysis.ClassifiedDifference.conflicting(localDifference, RebaseConflictCode.SAME_CONNECTION_SETTING,
                    "Both local and upstream flows modified the same setting of connection " + componentIdentifier);
        }
        return RebaseAnalysis.ClassifiedDifference.compatible(localDifference);
    }

    @Override
    public void apply(final FlowDifference localDifference, final VersionedProcessGroup mergedFlow) {
        final String componentIdentifier = localDifference.getComponentB().getIdentifier();
        final VersionedConnection connection = RebaseHandlerUtils.findConnectionById(mergedFlow, componentIdentifier);
        if (connection == null) {
            logger.warn("Unable to apply connection setting change: connection [{}] not found in merged flow", componentIdentifier);
            return;
        }

        final VersionedConnection localConnection = (VersionedConnection) localDifference.getComponentB();
        switch (supportedType) {
            case BACKPRESSURE_OBJECT_THRESHOLD_CHANGED -> connection.setBackPressureObjectThreshold(localConnection.getBackPressureObjectThreshold());
            case BACKPRESSURE_DATA_SIZE_THRESHOLD_CHANGED -> connection.setBackPressureDataSizeThreshold(localConnection.getBackPressureDataSizeThreshold());
            case FLOWFILE_EXPIRATION_CHANGED -> connection.setFlowFileExpiration(localConnection.getFlowFileExpiration());
            case PRIORITIZERS_CHANGED -> {
                final List<String> prioritizers = localConnection.getPrioritizers();
                connection.setPrioritizers(prioritizers == null ? null : new ArrayList<>(prioritizers));
            }
            default -> throw new IllegalStateException("Unsupported difference type " + supportedType);
        }
    }
}
