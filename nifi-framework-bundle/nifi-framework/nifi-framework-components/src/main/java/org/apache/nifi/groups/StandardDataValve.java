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

package org.apache.nifi.groups;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class StandardDataValve implements DataValve {
    private static final Logger logger = LoggerFactory.getLogger(StandardDataValve.class);
    private static final String GROUPS_WITH_DATA_FLOWING_IN_STATE_KEY = "groupsWithDataFlowingIn";
    private static final String GROUPS_WITH_DATA_FLOWING_OUT_STATE_KEY = "groupsWithDataFlowingOut";

    private final ProcessGroup processGroup;
    private final StateManager stateManager;

    private final Set<String> groupsWithDataFlowingIn = new HashSet<>();
    private final Set<String> groupsWithDataFlowingOut = new HashSet<>();

    private boolean leftOpenDueToDataQueued = false;


    public StandardDataValve(final ProcessGroup processGroup, final StateManager stateManager) {
        this.processGroup = processGroup;
        this.stateManager = stateManager;

        recoverState();
    }


    @Override
    public synchronized boolean tryOpenFlowIntoGroup(final ProcessGroup destinationGroup) {
        final boolean flowingIn = groupsWithDataFlowingIn.contains(destinationGroup.getIdentifier());
        if (flowingIn) {
            logger.debug("Allowing data to flow into {} because valve is already open", destinationGroup);

            // Data is already flowing into the Process Group.
            return true;
        }

        final FlowInForbiddenReason reasonForNotAllowing = getReasonFlowIntoGroupNotAllowed(destinationGroup);

        // If we are forbidding data to flow into the group due to the fact that data is currently allowed to flow out of the group,
        // and the valve was left open due to data being queued, let's verify that there is actually data queued up to flow out at the moment.
        // If there is not, remove the group from those that are currently allowing data to flow out. This can happen in the following situation:
        // - A FlowFile comes into the group
        // - The FlowFile is split into two FlowFiles
        // - One of the FlowFiles is routed to the Output Port, while the other is routed elsewhere
        // - The Output Port is triggered. It opens the valve to allow data to flow out of the group.
        // - The Output Port goes to close flow out of the group. However, the group is not empty, so the valve is not closed.
        // - The other FlowFile is never routed to an output port. Instead, it is auto-terminated by some processor.
        // Now, the valve has been left open.
        // In this case, though, when the Output Port failed to close the valve, this.leftOpenDueToDataQueued was set to true. If that is the case,
        // we can go ahead and close the valve now, if there's no more data queued.
        if (reasonForNotAllowing == FlowInForbiddenReason.OPEN_FOR_OUTPUT && leftOpenDueToDataQueued && !destinationGroup.isDataQueued()) {
            groupsWithDataFlowingOut.remove(destinationGroup.getIdentifier());
        }

        if (reasonForNotAllowing != null) {
            // Since there is a reason not to allow it, return false. The reason has already been logged at a DEBUG level.
            return false;
        }

        logger.debug("Opening valve to allow data to flow into {}", destinationGroup);
        groupsWithDataFlowingIn.add(destinationGroup.getIdentifier());
        storeState();
        return true;
    }

    private FlowInForbiddenReason getReasonFlowIntoGroupNotAllowed(final ProcessGroup destinationGroup) {
        if (destinationGroup.isDataQueued()) {
            // If the destination group already has data queued up, and the valve is not already open, do not allow data to
            // flow into the group. If we did, we would end up mixing together two different batches of data.
            logger.trace("Will not allow data to flow into {} because valve is not already open and the Process Group has data queued", destinationGroup);
            return FlowInForbiddenReason.DATA_QUEUED;
        }

        if (destinationGroup.getFlowFileOutboundPolicy() == FlowFileOutboundPolicy.BATCH_OUTPUT && groupsWithDataFlowingOut.contains(destinationGroup.getIdentifier())) {
            logger.trace("Will not allow data to flow into {} because Outbound Policy is Batch Output and valve is already open to allow data to flow out of group", destinationGroup);
            return FlowInForbiddenReason.OPEN_FOR_OUTPUT;
        }

        for (final Port port : destinationGroup.getInputPorts()) {
            for (final Connection connection : port.getIncomingConnections()) {
                final Connectable sourceConnectable = connection.getSource();
                if (sourceConnectable.getConnectableType() != ConnectableType.OUTPUT_PORT) {
                    continue;
                }

                final ProcessGroup sourceGroup = sourceConnectable.getProcessGroup();
                if (sourceGroup.getFlowFileOutboundPolicy() != FlowFileOutboundPolicy.BATCH_OUTPUT) {
                    continue;
                }

                final boolean flowingOutOfSourceGroup = groupsWithDataFlowingOut.contains(sourceGroup.getIdentifier());
                if (Boolean.TRUE.equals(flowingOutOfSourceGroup)) {
                    logger.trace("Will not allow data to flow into {} because port {} has an incoming connection from {} and that Process Group is currently allowing data to flow out",
                        destinationGroup, port, sourceConnectable);
                    return FlowInForbiddenReason.SOURCE_FLOWING_OUT;
                }
            }
        }

        return null;
    }

    @Override
    public synchronized void closeFlowIntoGroup(final ProcessGroup destinationGroup) {
        // If data is not already flowing in, nothing to do.
        if (!groupsWithDataFlowingIn.contains(destinationGroup.getIdentifier())) {
            return;
        }

        if (destinationGroup.getFlowFileConcurrency() == FlowFileConcurrency.SINGLE_BATCH_PER_NODE) {
            for (final Port port : destinationGroup.getInputPorts()) {
                for (final Connection connection : port.getIncomingConnections()) {
                    if (!connection.getFlowFileQueue().isEmpty()) {
                        logger.debug("Triggered to close flow of data into group {} but Input Port has incoming Connection {}, which is not empty, so will not close valve",
                            destinationGroup, connection);

                        return;
                    }
                }
            }
        }

        logger.debug("Closed valve so that data can no longer flow into {}", destinationGroup);
        storeState();
        groupsWithDataFlowingIn.remove(destinationGroup.getIdentifier());
    }

    @Override
    public synchronized boolean tryOpenFlowOutOfGroup(final ProcessGroup sourceGroup) {
        final boolean flowingOut = groupsWithDataFlowingOut.contains(sourceGroup.getIdentifier());
        if (flowingOut) {
            logger.debug("Allowing data to flow out of {} because valve is already open", sourceGroup);

            // Data is already flowing out of the Process Group.
            return true;
        }

        final String reasonNotAllowedToFlowIn = getReasonFlowOutOfGroupNotAllowed(sourceGroup);
        if (reasonNotAllowedToFlowIn != null) {
            // Data cannot flow into the Process Group. The reason has already been logged at a DEBUG level.
            return false;
        }

        logger.debug("Opening valve to allow data to flow out of {}", sourceGroup);
        groupsWithDataFlowingOut.add(sourceGroup.getIdentifier());
        storeState();

        // Note that the valve has not been left open due to data being queued. This prevents an Input Port from closing the valve
        // when the data is no longer queued, but while the Output Port is still processing the data.
        leftOpenDueToDataQueued = false;
        return true;
    }

    private String getReasonFlowOutOfGroupNotAllowed(final ProcessGroup sourceGroup) {
        // If we allow data to move out of the Process Group, but there is already data queued up in the output, then we will end up mixing
        // together batches of data. To avoid that, we do not allow data to flow out of the Process Group unless the destination connection of
        // all Output Ports are empty. This requirement is only relevant, though, for connections whose destination is a Port whose group is
        // configured to process data in batches.
        for (final Port port : sourceGroup.getOutputPorts()) {
            for (final Connection connection : port.getConnections()) {
                final Connectable destinationConnectable = connection.getDestination();
                if (destinationConnectable.getConnectableType() != ConnectableType.INPUT_PORT) {
                    continue;
                }

                final ProcessGroup destinationProcessGroup = destinationConnectable.getProcessGroup();
                if (destinationProcessGroup.getFlowFileConcurrency() != FlowFileConcurrency.SINGLE_BATCH_PER_NODE) {
                    continue;
                }

                if (!connection.getFlowFileQueue().isEmpty()) {
                    logger.trace("Not allowing data to flow out of {} because {} has a destination of {}, which has data queued and its Process Group is "
                        + "configured with a FlowFileConcurrency of Batch Per Node.", sourceGroup, port, connection);
                    return "Output Connection already has data queued";
                }

                final boolean dataFlowingIntoDestination = groupsWithDataFlowingIn.contains(destinationProcessGroup.getIdentifier());
                if (dataFlowingIntoDestination) {
                    logger.trace("Not allowing data to flow out of {} because {} has a destination of {}, and its Process Group is "
                        + "currently allowing data to flow in", sourceGroup, port, connection);
                    return "Destination Process Group is allowing data to flow in";
                }
            }
        }

        return null;
    }

    @Override
    public synchronized void closeFlowOutOfGroup(final ProcessGroup sourceGroup) {
        // If not already flowing, nothing to do.
        if (!groupsWithDataFlowingOut.contains(sourceGroup.getIdentifier())) {
            return;
        }

        final boolean dataQueued = sourceGroup.isDataQueued();
        if (dataQueued) {
            logger.debug("Triggered to close flow of data out of group {} but group is not empty so will not close valve", sourceGroup);

            // Denote that the valve was left open due to data being queued. This way, we can close the valve when the data is no longer queued.
            leftOpenDueToDataQueued = true;
            return;
        }

        logger.debug("Closed valve so that data can no longer flow out of {}", sourceGroup);
        groupsWithDataFlowingOut.remove(sourceGroup.getIdentifier());
        storeState();
    }

    @Override
    public synchronized DataValveDiagnostics getDiagnostics() {
        final Set<ProcessGroup> dataFlowingIn = groupsWithDataFlowingIn.stream()
            .map(processGroup::getProcessGroup)
            .collect(Collectors.toSet());

        final Set<ProcessGroup> dataFlowingOut = groupsWithDataFlowingOut.stream()
            .map(processGroup::getProcessGroup)
            .collect(Collectors.toSet());

        final Map<String, List<ProcessGroup>> reasonInputNotAllowed = new HashMap<>();
        final Map<String, List<ProcessGroup>> reasonOutputNotAllowed = new HashMap<>();
        for (final ProcessGroup group : processGroup.getProcessGroups()) {
            if (group.getFlowFileConcurrency() == FlowFileConcurrency.SINGLE_BATCH_PER_NODE) {
                final FlowInForbiddenReason forbiddenReason = getReasonFlowIntoGroupNotAllowed(group);
                final String inputReason = forbiddenReason == null ? "Input is Allowed" : forbiddenReason.getExplanation();

                final List<ProcessGroup> inputGroupsAffected = reasonInputNotAllowed.computeIfAbsent(inputReason, k -> new ArrayList<>());
                inputGroupsAffected.add(group);
            } else {
                final List<ProcessGroup> groupsAffected = reasonInputNotAllowed.computeIfAbsent("FlowFile Concurrency is " + group.getFlowFileConcurrency(), k -> new ArrayList<>());
                groupsAffected.add(group);
            }

            if (group.getFlowFileOutboundPolicy() == FlowFileOutboundPolicy.BATCH_OUTPUT) {
                String outputReason = getReasonFlowOutOfGroupNotAllowed(group);
                if (outputReason == null) {
                    outputReason = "Output is Allowed";
                }

                final List<ProcessGroup> outputGroupsAffected = reasonOutputNotAllowed.computeIfAbsent(outputReason, k -> new ArrayList<>());
                outputGroupsAffected.add(group);
            } else {
                final List<ProcessGroup> groupsAffected = reasonOutputNotAllowed.computeIfAbsent("FlowFile Outbound Policy is " + group.getFlowFileOutboundPolicy(), k -> new ArrayList<>());
                groupsAffected.add(group);
            }
        }

        return new DataValveDiagnostics() {
            @Override
            public Set<ProcessGroup> getGroupsWithDataFlowingIn() {
                return dataFlowingIn;
            }

            @Override
            public Set<ProcessGroup> getGroupsWithDataFlowingOut() {
                return dataFlowingOut;
            }

            @Override
            public Map<String, List<ProcessGroup>> getReasonForInputNotAllowed() {
                return reasonInputNotAllowed;
            }

            @Override
            public Map<String, List<ProcessGroup>> getReasonForOutputNotAllowed() {
                return reasonOutputNotAllowed;
            }
        };
    }

    private synchronized void recoverState() {
        final StateMap stateMap;
        try {
            stateMap = stateManager.getState(Scope.LOCAL);
        } catch (final Exception e) {
            logger.error("Failed to recover state for {}. This could result in Process Groups configured with a FlowFile Concurrency of SINGLE_BATCH_PER_NODE to get data from " +
                "multiple batches concurrently or stop ingesting data", this, e);
            return;
        }

        if (stateMap.getStateVersion().isEmpty()) {
            logger.debug("No state to recover for {}", this);
            return;
        }

        final List<String> dataFlowingInIds = getIdsForKey(stateMap, GROUPS_WITH_DATA_FLOWING_IN_STATE_KEY);
        final List<String> dataFlowingOutIds = getIdsForKey(stateMap, GROUPS_WITH_DATA_FLOWING_OUT_STATE_KEY);
        logger.debug("Recovered state for {}; {} Process Groups have data flowing in ({}); {} Process Groups have data flowing out ({})", this, dataFlowingInIds.size(),
            dataFlowingInIds, dataFlowingOutIds.size(), dataFlowingOutIds);

        groupsWithDataFlowingIn.addAll(dataFlowingInIds);
        groupsWithDataFlowingOut.addAll(dataFlowingOutIds);
    }

    private List<String> getIdsForKey(final StateMap stateMap, final String key) {
        final String concatenated = stateMap.get(key);
        if (concatenated == null || concatenated.isEmpty()) {
            return Collections.emptyList();
        }

        final String[] split = concatenated.split(",");
        return Arrays.asList(split);
    }

    private void storeState() {
        final String dataFlowingIn = StringUtils.join(groupsWithDataFlowingIn, ",");
        final String dataFlowingOut = StringUtils.join(groupsWithDataFlowingOut, ",");
        final Map<String, String> stateValues = new HashMap<>();
        stateValues.put(GROUPS_WITH_DATA_FLOWING_IN_STATE_KEY, dataFlowingIn);
        stateValues.put(GROUPS_WITH_DATA_FLOWING_OUT_STATE_KEY, dataFlowingOut);

        try {
            stateManager.setState(stateValues, Scope.LOCAL);
        } catch (final Exception e) {
            logger.error("Failed to store state for {}. If NiFi is restarted before state is properly stored, this could result Process Groups configured with a " +
                "FlowFile Concurrency of SINGLE_BATCH_PER_NODE to get data from multiple batches concurrently or stop ingesting data", this, e);
        }
    }

    @Override
    public String toString() {
        return "StandardDataValve[group=" + processGroup + "]";
    }

    public enum FlowInForbiddenReason {
        DATA_QUEUED("Process Group already has data queued and valve is not already open to allow data to flow in"),

        OPEN_FOR_OUTPUT("Data Valve is already open to allow data to flow out of group"),

        SOURCE_FLOWING_OUT("Port has an incoming connection from a Process Group that is currently allowing data to flow out");

        private final String explanation;
        FlowInForbiddenReason(final String explanation) {
            this.explanation = explanation;
        }

        public String getExplanation() {
            return explanation;
        }
    }
}
