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

package org.apache.nifi.reporting;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.repository.FlowFileEvent;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.RepositoryStatusReport;
import org.apache.nifi.controller.repository.metrics.EmptyFlowFileEvent;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.LoadBalanceStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessingPerformanceStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.RunStatus;
import org.apache.nifi.controller.status.TransmissionStatus;
import org.apache.nifi.controller.status.analytics.ConnectionStatusPredictions;
import org.apache.nifi.controller.status.analytics.StatusAnalytics;
import org.apache.nifi.controller.status.analytics.StatusAnalyticsEngine;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

public abstract class AbstractEventAccess implements EventAccess {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEventAccess.class);

    private static final Predicate<Authorizable> AUTHORIZATION_APPROVED = authorizable -> true;
    private static final Predicate<Authorizable> AUTHORIZATION_DENIED = authorizable -> false;

    private final ProcessScheduler processScheduler;
    private final StatusAnalyticsEngine statusAnalyticsEngine;
    private final FlowManager flowManager;
    private final FlowFileEventRepository flowFileEventRepository;

    public AbstractEventAccess(final ProcessScheduler processScheduler, final StatusAnalyticsEngine analyticsEngine, final FlowManager flowManager,
                               final FlowFileEventRepository flowFileEventRepository) {
        this.processScheduler = processScheduler;
        this.statusAnalyticsEngine = analyticsEngine;
        this.flowManager = flowManager;
        this.flowFileEventRepository = flowFileEventRepository;
    }

    /**
     * Returns the status of all components in the specified group. This request
     * is not in the context of a user so the results will be unfiltered.
     *
     * @param groupId group id
     * @return the component status
     */
    @Override
    public ProcessGroupStatus getGroupStatus(final String groupId) {
        final RepositoryStatusReport statusReport = generateRepositoryStatusReport();
        final ProcessGroup group = flowManager.getGroup(groupId);
        return getGroupStatus(group, statusReport, AUTHORIZATION_APPROVED, Integer.MAX_VALUE, 1, true);
    }

    /**
     * Returns the status for the components in the specified group with the
     * specified report. This request is not in the context of a user so the
     * results will be unfiltered.
     *
     * @param groupId group id
     * @param statusReport report
     * @return the component status
     */
    public ProcessGroupStatus getGroupStatus(final String groupId, final RepositoryStatusReport statusReport) {
        final ProcessGroup group = flowManager.getGroup(groupId);

        // this was invoked with no user context so the results will be unfiltered... necessary for aggregating status history
        return getGroupStatus(group, statusReport, AUTHORIZATION_APPROVED, Integer.MAX_VALUE, 1, false);
    }

    protected RepositoryStatusReport generateRepositoryStatusReport() {
        return flowFileEventRepository.reportTransferEvents(System.currentTimeMillis());
    }


    /**
     * Returns the status for the components in the specified group with the
     * specified report. The results will be filtered by executing the specified
     * predicate.
     *
     * @param group group id
     * @param statusReport report
     * @param checkAuthorization is authorized check
     * @param recursiveStatusDepth the number of levels deep we should recurse and still include the the processors' statuses, the groups' statuses, etc. in the returned ProcessGroupStatus
     * @param currentDepth the current number of levels deep that we have recursed
     * @param includeConnectionDetails whether or not to include the details of the connections that may be expensive to calculate and/or require locks be obtained
     * @return the component status
     */
    ProcessGroupStatus getGroupStatus(final ProcessGroup group, final RepositoryStatusReport statusReport, final Predicate<Authorizable> checkAuthorization,
                                      final int recursiveStatusDepth, final int currentDepth, final boolean includeConnectionDetails) {
        if (group == null) {
            return null;
        }

        final ProcessGroupStatus status = new ProcessGroupStatus();
        status.setId(group.getIdentifier());
        status.setName(checkAuthorization.test(group) ? group.getName() : group.getIdentifier());
        int activeGroupThreads = 0;
        int terminatedGroupThreads = 0;
        long bytesRead = 0L;
        long bytesWritten = 0L;
        int queuedCount = 0;
        long queuedContentSize = 0L;
        int flowFilesIn = 0;
        long bytesIn = 0L;
        int flowFilesOut = 0;
        long bytesOut = 0L;
        int flowFilesReceived = 0;
        long bytesReceived = 0L;
        int flowFilesSent = 0;
        long bytesSent = 0L;
        int flowFilesTransferred = 0;
        long bytesTransferred = 0;
        long processingNanos = 0;
        final ProcessingPerformanceStatus performanceStatus = new ProcessingPerformanceStatus();
        performanceStatus.setIdentifier(group.getIdentifier());

        final boolean populateChildStatuses = currentDepth <= recursiveStatusDepth;

        // Set Authorization predicate based on whether to populate child component status avoiding unnecessary calls to Authorizer
        final Predicate<Authorizable> isAuthorized;
        if (populateChildStatuses) {
            isAuthorized = checkAuthorization;
        } else {
            isAuthorized = AUTHORIZATION_DENIED;
        }

        // set status for processors
        final Collection<ProcessorStatus> processorStatusCollection = new ArrayList<>();
        status.setProcessorStatus(processorStatusCollection);
        for (final ProcessorNode procNode : group.getProcessors()) {
            final ProcessorStatus procStat = getProcessorStatus(statusReport, procNode, isAuthorized);
            if (populateChildStatuses) {
                processorStatusCollection.add(procStat);
            }
            activeGroupThreads += procStat.getActiveThreadCount();
            terminatedGroupThreads += procStat.getTerminatedThreadCount();
            bytesRead += procStat.getBytesRead();
            bytesWritten += procStat.getBytesWritten();

            flowFilesReceived += procStat.getFlowFilesReceived();
            bytesReceived += procStat.getBytesReceived();
            flowFilesSent += procStat.getFlowFilesSent();
            bytesSent += procStat.getBytesSent();

            processingNanos += procStat.getProcessingNanos();

            final ProcessingPerformanceStatus processorPerformanceStatus = procStat.getProcessingPerformanceStatus();

            if (processorPerformanceStatus != null) {
                performanceStatus.setCpuDuration(performanceStatus.getCpuDuration() + processorPerformanceStatus.getCpuDuration());
                performanceStatus.setContentReadDuration(performanceStatus.getContentReadDuration() + processorPerformanceStatus.getContentReadDuration());
                performanceStatus.setContentWriteDuration(performanceStatus.getContentWriteDuration() + processorPerformanceStatus.getContentWriteDuration());
                performanceStatus.setSessionCommitDuration(performanceStatus.getSessionCommitDuration() + processorPerformanceStatus.getSessionCommitDuration());
                performanceStatus.setGarbageCollectionDuration(performanceStatus.getGarbageCollectionDuration() + processorPerformanceStatus.getGarbageCollectionDuration());
            }
        }

        // set status for local child groups
        final Collection<ProcessGroupStatus> localChildGroupStatusCollection = new ArrayList<>();
        status.setProcessGroupStatus(localChildGroupStatusCollection);
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final ProcessGroupStatus childGroupStatus;
            if (populateChildStatuses) {
                childGroupStatus = getGroupStatus(childGroup, statusReport, isAuthorized, recursiveStatusDepth, currentDepth + 1, includeConnectionDetails);
                localChildGroupStatusCollection.add(childGroupStatus);
            } else {
                // In this case, we don't want to include any of the recursive components' individual statuses. As a result, we can
                // avoid performing any sort of authorizations. Because we only care about the numbers that come back, we can just indicate
                // that the user is not authorized. This allows us to avoid the expense of both performing the authorization and calculating
                // things that we would otherwise need to calculate if the user were in fact authorized.
                childGroupStatus = getGroupStatus(childGroup, statusReport, AUTHORIZATION_DENIED, recursiveStatusDepth, currentDepth + 1, includeConnectionDetails);
            }

            activeGroupThreads += childGroupStatus.getActiveThreadCount();
            terminatedGroupThreads += childGroupStatus.getTerminatedThreadCount();
            bytesRead += childGroupStatus.getBytesRead();
            bytesWritten += childGroupStatus.getBytesWritten();
            queuedCount += childGroupStatus.getQueuedCount();
            queuedContentSize += childGroupStatus.getQueuedContentSize();

            flowFilesReceived += childGroupStatus.getFlowFilesReceived();
            bytesReceived += childGroupStatus.getBytesReceived();
            flowFilesSent += childGroupStatus.getFlowFilesSent();
            bytesSent += childGroupStatus.getBytesSent();

            flowFilesTransferred += childGroupStatus.getFlowFilesTransferred();
            bytesTransferred += childGroupStatus.getBytesTransferred();

            processingNanos += childGroupStatus.getProcessingNanos();

            final ProcessingPerformanceStatus childGroupPerformanceStatus = childGroupStatus.getProcessingPerformanceStatus();

            if (childGroupPerformanceStatus != null) {
                performanceStatus.setCpuDuration(performanceStatus.getCpuDuration() + childGroupPerformanceStatus.getCpuDuration());
                performanceStatus.setContentReadDuration(performanceStatus.getContentReadDuration() + childGroupPerformanceStatus.getContentReadDuration());
                performanceStatus.setContentWriteDuration(performanceStatus.getContentWriteDuration() + childGroupPerformanceStatus.getContentWriteDuration());
                performanceStatus.setSessionCommitDuration(performanceStatus.getSessionCommitDuration() + childGroupPerformanceStatus.getSessionCommitDuration());
                performanceStatus.setGarbageCollectionDuration(performanceStatus.getGarbageCollectionDuration() + childGroupPerformanceStatus.getGarbageCollectionDuration());
            }
        }

        // set status for remote child groups
        final Collection<RemoteProcessGroupStatus> remoteProcessGroupStatusCollection = new ArrayList<>();
        status.setRemoteProcessGroupStatus(remoteProcessGroupStatusCollection);
        for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
            final RemoteProcessGroupStatus remoteStatus = createRemoteGroupStatus(remoteGroup, statusReport, isAuthorized);
            if (remoteStatus != null) {
                if (populateChildStatuses) {
                    remoteProcessGroupStatusCollection.add(remoteStatus);
                }

                flowFilesReceived += remoteStatus.getReceivedCount();
                bytesReceived += remoteStatus.getReceivedContentSize();
                flowFilesSent += remoteStatus.getSentCount();
                bytesSent += remoteStatus.getSentContentSize();
            }
        }

        // connection status
        final Collection<ConnectionStatus> connectionStatusCollection = new ArrayList<>();
        status.setConnectionStatus(connectionStatusCollection);

        long now = System.currentTimeMillis();

        // get the connection and remote port status
        for (final Connection conn : group.getConnections()) {
            final boolean isConnectionAuthorized = isAuthorized.test(conn);
            final boolean isSourceAuthorized = isAuthorized.test(conn.getSource());
            final boolean isDestinationAuthorized = isAuthorized.test(conn.getDestination());

            final ConnectionStatus connStatus = new ConnectionStatus();
            connStatus.setId(conn.getIdentifier());
            connStatus.setGroupId(conn.getProcessGroup().getIdentifier());
            connStatus.setSourceId(conn.getSource().getIdentifier());
            connStatus.setSourceName(isSourceAuthorized ? conn.getSource().getName() : conn.getSource().getIdentifier());
            connStatus.setDestinationId(conn.getDestination().getIdentifier());
            connStatus.setDestinationName(isDestinationAuthorized ? conn.getDestination().getName() : conn.getDestination().getIdentifier());
            connStatus.setBackPressureDataSizeThreshold(conn.getFlowFileQueue().getBackPressureDataSizeThreshold());
            connStatus.setBackPressureObjectThreshold(conn.getFlowFileQueue().getBackPressureObjectThreshold());
            if (includeConnectionDetails) {
                connStatus.setTotalQueuedDuration(conn.getFlowFileQueue().getTotalQueuedDuration(now));
                long minLastQueueDate = conn.getFlowFileQueue().getMinLastQueueDate();
                connStatus.setMaxQueuedDuration(minLastQueueDate == 0 ? 0 : now - minLastQueueDate);
            } else {
                connStatus.setTotalQueuedDuration(0L);
                connStatus.setMaxQueuedDuration(0L);
            }
            connStatus.setFlowFileAvailability(conn.getFlowFileQueue().getFlowFileAvailability());

            final FlowFileEvent connectionStatusReport = statusReport.getReportEntry(conn.getIdentifier());
            if (connectionStatusReport != null) {
                connStatus.setInputBytes(connectionStatusReport.getContentSizeIn());
                connStatus.setInputCount(connectionStatusReport.getFlowFilesIn());
                connStatus.setOutputBytes(connectionStatusReport.getContentSizeOut());
                connStatus.setOutputCount(connectionStatusReport.getFlowFilesOut());

                flowFilesTransferred += connectionStatusReport.getFlowFilesIn() + connectionStatusReport.getFlowFilesOut();
                bytesTransferred += connectionStatusReport.getContentSizeIn() + connectionStatusReport.getContentSizeOut();
            }

            if (statusAnalyticsEngine != null) {
                StatusAnalytics statusAnalytics =  statusAnalyticsEngine.getStatusAnalytics(conn.getIdentifier());
                if (statusAnalytics != null) {
                    Map<String, Long> predictionValues = statusAnalytics.getPredictions();
                    ConnectionStatusPredictions predictions = new ConnectionStatusPredictions();
                    connStatus.setPredictions(predictions);
                    predictions.setPredictedTimeToBytesBackpressureMillis(predictionValues.get("timeToBytesBackpressureMillis"));
                    predictions.setPredictedTimeToCountBackpressureMillis(predictionValues.get("timeToCountBackpressureMillis"));
                    predictions.setNextPredictedQueuedBytes(predictionValues.get("nextIntervalBytes"));
                    predictions.setNextPredictedQueuedCount(predictionValues.get("nextIntervalCount").intValue());
                    predictions.setPredictedPercentCount(predictionValues.get("nextIntervalPercentageUseCount").intValue());
                    predictions.setPredictedPercentBytes(predictionValues.get("nextIntervalPercentageUseBytes").intValue());
                    predictions.setPredictionIntervalMillis(predictionValues.get("intervalTimeMillis"));
                }
            } else {
                connStatus.setPredictions(null);
            }

            if (isConnectionAuthorized) {
                if (StringUtils.isNotBlank(conn.getName())) {
                    connStatus.setName(conn.getName());
                } else if (conn.getRelationships() != null && !conn.getRelationships().isEmpty()) {
                    final Collection<String> relationships = new ArrayList<>(conn.getRelationships().size());
                    for (final Relationship relationship : conn.getRelationships()) {
                        relationships.add(relationship.getName());
                    }
                    connStatus.setName(StringUtils.join(relationships, ", "));
                }
            } else {
                connStatus.setName(conn.getIdentifier());
            }

            final QueueSize queueSize = conn.getFlowFileQueue().size();
            final int connectionQueuedCount = queueSize.getObjectCount();
            final long connectionQueuedBytes = queueSize.getByteCount();
            if (connectionQueuedCount > 0) {
                connStatus.setQueuedBytes(connectionQueuedBytes);
                connStatus.setQueuedCount(connectionQueuedCount);
            }

            final FlowFileQueue flowFileQueue = conn.getFlowFileQueue();
            final LoadBalanceStrategy loadBalanceStrategy = flowFileQueue.getLoadBalanceStrategy();
            if (loadBalanceStrategy == LoadBalanceStrategy.DO_NOT_LOAD_BALANCE) {
                connStatus.setLoadBalanceStatus(LoadBalanceStatus.LOAD_BALANCE_NOT_CONFIGURED);
            } else if (flowFileQueue.isActivelyLoadBalancing()) {
                connStatus.setLoadBalanceStatus(LoadBalanceStatus.LOAD_BALANCE_ACTIVE);
            } else {
                connStatus.setLoadBalanceStatus(LoadBalanceStatus.LOAD_BALANCE_INACTIVE);
            }

            if (populateChildStatuses) {
                connectionStatusCollection.add(connStatus);
            }

            queuedCount += connectionQueuedCount;
            queuedContentSize += connectionQueuedBytes;

            final Connectable source = conn.getSource();
            if (ConnectableType.REMOTE_OUTPUT_PORT.equals(source.getConnectableType())) {
                final RemoteGroupPort remoteOutputPort = (RemoteGroupPort) source;
                activeGroupThreads += processScheduler.getActiveThreadCount(remoteOutputPort);
            }

            final Connectable destination = conn.getDestination();
            if (ConnectableType.REMOTE_INPUT_PORT.equals(destination.getConnectableType())) {
                final RemoteGroupPort remoteInputPort = (RemoteGroupPort) destination;
                activeGroupThreads += processScheduler.getActiveThreadCount(remoteInputPort);
            }
        }

        // status for input ports
        final Collection<PortStatus> inputPortStatusCollection = new ArrayList<>();
        status.setInputPortStatus(inputPortStatusCollection);

        final Set<Port> inputPorts = group.getInputPorts();
        for (final Port port : inputPorts) {
            final boolean isInputPortAuthorized = isAuthorized.test(port);

            final PortStatus portStatus = new PortStatus();
            portStatus.setId(port.getIdentifier());
            portStatus.setGroupId(port.getProcessGroup().getIdentifier());
            portStatus.setName(isInputPortAuthorized ? port.getName() : port.getIdentifier());
            portStatus.setActiveThreadCount(processScheduler.getActiveThreadCount(port));

            // determine the run status
            if (ScheduledState.RUNNING.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Running);
            } else if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Disabled);
            } else if (!port.isValid()) {
                portStatus.setRunStatus(RunStatus.Invalid);
            } else {
                portStatus.setRunStatus(RunStatus.Stopped);
            }

            // special handling for public ports
            if (port instanceof PublicPort) {
                portStatus.setTransmitting(((PublicPort) port).isTransmitting());
            }

            final FlowFileEvent entry = statusReport.getReportEntries().get(port.getIdentifier());
            if (entry == null) {
                portStatus.setInputBytes(0L);
                portStatus.setInputCount(0);
                portStatus.setOutputBytes(0L);
                portStatus.setOutputCount(0);
            } else {
                final int processedCount = entry.getFlowFilesOut();
                final long numProcessedBytes = entry.getContentSizeOut();
                portStatus.setOutputBytes(numProcessedBytes);
                portStatus.setOutputCount(processedCount);

                final int inputCount = entry.getFlowFilesIn();
                final long inputBytes = entry.getContentSizeIn();
                portStatus.setInputBytes(inputBytes);
                portStatus.setInputCount(inputCount);

                flowFilesIn += port instanceof PublicPort ? entry.getFlowFilesReceived() : inputCount;
                bytesIn += port instanceof PublicPort ? entry.getBytesReceived() : inputBytes;

                bytesWritten += entry.getBytesWritten();

                flowFilesReceived += entry.getFlowFilesReceived();
                bytesReceived += entry.getBytesReceived();
            }

            if (populateChildStatuses) {
                inputPortStatusCollection.add(portStatus);
            }

            activeGroupThreads += portStatus.getActiveThreadCount();
        }

        // status for output ports
        final Collection<PortStatus> outputPortStatusCollection = new ArrayList<>();
        status.setOutputPortStatus(outputPortStatusCollection);

        final Set<Port> outputPorts = group.getOutputPorts();
        for (final Port port : outputPorts) {
            final boolean isOutputPortAuthorized = isAuthorized.test(port);

            final PortStatus portStatus = new PortStatus();
            portStatus.setId(port.getIdentifier());
            portStatus.setGroupId(port.getProcessGroup().getIdentifier());
            portStatus.setName(isOutputPortAuthorized ? port.getName() : port.getIdentifier());
            portStatus.setActiveThreadCount(processScheduler.getActiveThreadCount(port));

            // determine the run status
            if (ScheduledState.RUNNING.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Running);
            } else if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
                portStatus.setRunStatus(RunStatus.Disabled);
            } else if (!port.isValid()) {
                portStatus.setRunStatus(RunStatus.Invalid);
            } else {
                portStatus.setRunStatus(RunStatus.Stopped);
            }

            // special handling for public ports
            if (port instanceof PublicPort) {
                portStatus.setTransmitting(((PublicPort) port).isTransmitting());
            }

            final FlowFileEvent entry = statusReport.getReportEntries().get(port.getIdentifier());
            if (entry == null) {
                portStatus.setInputBytes(0L);
                portStatus.setInputCount(0);
                portStatus.setOutputBytes(0L);
                portStatus.setOutputCount(0);
            } else {
                final int processedCount = entry.getFlowFilesOut();
                final long numProcessedBytes = entry.getContentSizeOut();
                portStatus.setOutputBytes(numProcessedBytes);
                portStatus.setOutputCount(processedCount);

                final int inputCount = entry.getFlowFilesIn();
                final long inputBytes = entry.getContentSizeIn();
                portStatus.setInputBytes(inputBytes);
                portStatus.setInputCount(inputCount);

                bytesRead += entry.getBytesRead();

                flowFilesOut += port instanceof PublicPort ? entry.getFlowFilesSent() : entry.getFlowFilesOut();
                bytesOut += port instanceof PublicPort ? entry.getBytesSent() : entry.getContentSizeOut();

                flowFilesSent = entry.getFlowFilesSent();
                bytesSent += entry.getBytesSent();
            }

            if (populateChildStatuses) {
                outputPortStatusCollection.add(portStatus);
            }

            activeGroupThreads += portStatus.getActiveThreadCount();
        }

        for (final Funnel funnel : group.getFunnels()) {
            activeGroupThreads += processScheduler.getActiveThreadCount(funnel);
        }

        final int statelessActiveThreadCount;
        if (group.resolveExecutionEngine() == ExecutionEngine.STATELESS) {
            statelessActiveThreadCount = processScheduler.getActiveThreadCount(group);
            activeGroupThreads = statelessActiveThreadCount;
        } else {
            statelessActiveThreadCount = 0;
        }

        status.setActiveThreadCount(activeGroupThreads);
        status.setStatelessActiveThreadCount(statelessActiveThreadCount);
        status.setTerminatedThreadCount(terminatedGroupThreads);
        status.setBytesRead(bytesRead);
        status.setBytesWritten(bytesWritten);
        status.setQueuedCount(queuedCount);
        status.setQueuedContentSize(queuedContentSize);
        status.setInputContentSize(bytesIn);
        status.setInputCount(flowFilesIn);
        status.setOutputContentSize(bytesOut);
        status.setOutputCount(flowFilesOut);
        status.setFlowFilesReceived(flowFilesReceived);
        status.setBytesReceived(bytesReceived);
        status.setFlowFilesSent(flowFilesSent);
        status.setBytesSent(bytesSent);
        status.setFlowFilesTransferred(flowFilesTransferred);
        status.setBytesTransferred(bytesTransferred);
        status.setProcessingNanos(processingNanos);
        status.setProcessingPerformanceStatus(performanceStatus);

        final VersionControlInformation vci = group.getVersionControlInformation();
        if (vci != null) {
            final RegisteredFlowSnapshotMetadata registeredFlowSnapshotMetadata = new RegisteredFlowSnapshotMetadata();
            registeredFlowSnapshotMetadata.setBranch(vci.getBranch());
            registeredFlowSnapshotMetadata.setBucketIdentifier(vci.getBucketIdentifier());
            registeredFlowSnapshotMetadata.setFlowIdentifier(vci.getFlowIdentifier());
            registeredFlowSnapshotMetadata.setVersion(vci.getVersion());
            registeredFlowSnapshotMetadata.setFlowName(vci.getFlowName());
            registeredFlowSnapshotMetadata.setRegistryIdentifier(vci.getRegistryIdentifier());
            registeredFlowSnapshotMetadata.setRegistryName(vci.getRegistryName());
            status.setRegisteredFlowSnapshotMetadata(registeredFlowSnapshotMetadata);
            try {
                final VersionedFlowStatus flowStatus = vci.getStatus();
                if (flowStatus != null && flowStatus.getState() != null) {
                    status.setVersionedFlowState(flowStatus.getState());
                }
            } catch (final Exception e) {
                logger.warn("Failed to determine Version Control State for {}. Will consider state to be SYNC_FAILURE", group, e);
                status.setVersionedFlowState(VersionedFlowState.SYNC_FAILURE);
            }
        }

        return status;
    }

    private RemoteProcessGroupStatus createRemoteGroupStatus(final RemoteProcessGroup remoteGroup, final RepositoryStatusReport statusReport, final Predicate<Authorizable> isAuthorized) {
        final boolean isRemoteProcessGroupAuthorized = isAuthorized.test(remoteGroup);

        int receivedCount = 0;
        long receivedContentSize = 0L;
        int sentCount = 0;
        long sentContentSize = 0L;
        int activeThreadCount = 0;
        int activePortCount = 0;
        int inactivePortCount = 0;

        final RemoteProcessGroupStatus status = new RemoteProcessGroupStatus();
        status.setGroupId(remoteGroup.getProcessGroup().getIdentifier());
        status.setName(isRemoteProcessGroupAuthorized ? remoteGroup.getName() : remoteGroup.getIdentifier());
        status.setComments(isRemoteProcessGroupAuthorized ? remoteGroup.getComments() : null);
        status.setAuthorizationIssue(remoteGroup.getAuthorizationIssue());
        status.setLastRefreshTime(remoteGroup.getLastRefreshTime());
        status.setTargetUri(isRemoteProcessGroupAuthorized ? remoteGroup.getTargetUri() : null);

        long lineageMillis = 0L;
        int flowFilesRemoved = 0;
        int flowFilesTransferred = 0;
        for (final Port port : remoteGroup.getInputPorts()) {
            // determine if this input port is connected
            final boolean isConnected = port.hasIncomingConnection();

            // we only want to consider remote ports that we are connected to
            if (isConnected) {
                if (port.isRunning()) {
                    activePortCount++;
                } else {
                    inactivePortCount++;
                }

                activeThreadCount += processScheduler.getActiveThreadCount(port);

                final FlowFileEvent portEvent = statusReport.getReportEntry(port.getIdentifier());
                if (portEvent != null) {
                    lineageMillis += portEvent.getAggregateLineageMillis();
                    flowFilesRemoved += portEvent.getFlowFilesRemoved();
                    flowFilesTransferred += portEvent.getFlowFilesOut();
                    sentCount += portEvent.getFlowFilesSent();
                    sentContentSize += portEvent.getBytesSent();
                }
            }
        }

        for (final Port port : remoteGroup.getOutputPorts()) {
            // determine if this output port is connected
            final boolean isConnected = !port.getConnections().isEmpty();

            // we only want to consider remote ports that we are connected from
            if (isConnected) {
                if (port.isRunning()) {
                    activePortCount++;
                } else {
                    inactivePortCount++;
                }

                activeThreadCount += processScheduler.getActiveThreadCount(port);

                final FlowFileEvent portEvent = statusReport.getReportEntry(port.getIdentifier());
                if (portEvent != null) {
                    receivedCount += portEvent.getFlowFilesReceived();
                    receivedContentSize += portEvent.getBytesReceived();
                }
            }
        }

        status.setId(remoteGroup.getIdentifier());
        status.setTransmissionStatus(remoteGroup.isConfiguredToTransmit() ? TransmissionStatus.Transmitting : TransmissionStatus.NotTransmitting);
        status.setActiveThreadCount(activeThreadCount);
        status.setReceivedContentSize(receivedContentSize);
        status.setReceivedCount(receivedCount);
        status.setSentContentSize(sentContentSize);
        status.setSentCount(sentCount);
        status.setActiveRemotePortCount(activePortCount);
        status.setInactiveRemotePortCount(inactivePortCount);

        final int flowFilesOutOrRemoved = flowFilesTransferred + flowFilesRemoved;
        status.setAverageLineageDuration(flowFilesOutOrRemoved == 0 ? 0 : lineageMillis / flowFilesOutOrRemoved, TimeUnit.MILLISECONDS);

        return status;
    }

    private ProcessorStatus getProcessorStatus(final RepositoryStatusReport report, final ProcessorNode procNode, final Predicate<Authorizable> isAuthorized) {
        final FlowFileEvent entry = report.getReportEntries().get(procNode.getIdentifier());
        return getProcessorStatus(entry, procNode, isAuthorized);
    }

    protected ProcessorStatus getProcessorStatus(final FlowFileEvent flowFileEvent, final ProcessorNode procNode, final Predicate<Authorizable> isAuthorized) {
        final boolean isProcessorAuthorized = isAuthorized.test(procNode);

        final ProcessorStatus status = new ProcessorStatus();
        status.setId(procNode.getIdentifier());
        status.setGroupId(procNode.getProcessGroup().getIdentifier());
        status.setName(isProcessorAuthorized ? procNode.getName() : procNode.getIdentifier());
        status.setType(isProcessorAuthorized ? procNode.getComponentType() : "Processor");

        if (flowFileEvent != null && flowFileEvent != EmptyFlowFileEvent.INSTANCE) {
            final int processedCount = flowFileEvent.getFlowFilesOut();
            final long numProcessedBytes = flowFileEvent.getContentSizeOut();
            status.setOutputBytes(numProcessedBytes);
            status.setOutputCount(processedCount);

            final int inputCount = flowFileEvent.getFlowFilesIn();
            final long inputBytes = flowFileEvent.getContentSizeIn();
            status.setInputBytes(inputBytes);
            status.setInputCount(inputCount);

            final long readBytes = flowFileEvent.getBytesRead();
            status.setBytesRead(readBytes);

            final long writtenBytes = flowFileEvent.getBytesWritten();
            status.setBytesWritten(writtenBytes);

            status.setProcessingNanos(flowFileEvent.getProcessingNanoseconds());
            status.setInvocations(flowFileEvent.getInvocations());

            status.setAverageLineageDuration(flowFileEvent.getAverageLineageMillis());

            status.setFlowFilesReceived(flowFileEvent.getFlowFilesReceived());
            status.setBytesReceived(flowFileEvent.getBytesReceived());
            status.setFlowFilesSent(flowFileEvent.getFlowFilesSent());
            status.setBytesSent(flowFileEvent.getBytesSent());
            status.setFlowFilesRemoved(flowFileEvent.getFlowFilesRemoved());

            if (isProcessorAuthorized) {
                status.setCounters(flowFileEvent.getCounters());
            }

            final ProcessingPerformanceStatus perfStatus = createProcessingPerformanceStatus(flowFileEvent, procNode);
            status.setProcessingPerformanceStatus(perfStatus);
        }

        // Determine the run status and get any validation error... only validating while STOPPED
        // is a trade-off we are willing to make, even though processor validity could change due to
        // environmental conditions (property configured with a file path and the file being externally
        // removed). This saves on validation costs that would be unnecessary most of the time.
        status.setRunStatus(determineRunStatus(procNode));

        status.setExecutionNode(procNode.getExecutionNode());
        status.setTerminatedThreadCount(procNode.getTerminatedThreadCount());
        status.setActiveThreadCount(procNode.getActiveThreadCount());

        return status;
    }

    private ProcessingPerformanceStatus createProcessingPerformanceStatus(final FlowFileEvent flowFileEvent, final ProcessorNode procNode) {
        final ProcessingPerformanceStatus perfStatus = new ProcessingPerformanceStatus();
        perfStatus.setIdentifier(procNode.getIdentifier());
        perfStatus.setCpuDuration(flowFileEvent.getCpuNanoseconds());
        perfStatus.setContentReadDuration(flowFileEvent.getContentReadNanoseconds());
        perfStatus.setContentWriteDuration(flowFileEvent.getContentWriteNanoseconds());
        perfStatus.setSessionCommitDuration(flowFileEvent.getSessionCommitNanoseconds());
        perfStatus.setGarbageCollectionDuration(flowFileEvent.getGargeCollectionMillis());
        return perfStatus;
    }

    private RunStatus determineRunStatus(final ProcessorNode procNode) {
        if (ScheduledState.DISABLED.equals(procNode.getScheduledState())) {
            return RunStatus.Disabled;
        } else if (ScheduledState.RUNNING.equals(procNode.getScheduledState())) {
            return RunStatus.Running;
        } else if (procNode.getValidationStatus() == ValidationStatus.VALIDATING) {
            return RunStatus.Validating;
        } else if (procNode.getValidationStatus() == ValidationStatus.INVALID && procNode.getActiveThreadCount() == 0) {
            return RunStatus.Invalid;
        }

        return RunStatus.Stopped;
    }

    /**
     * Returns the status of all components in the controller. This request is
     * not in the context of a user so the results will be unfiltered.
     *
     * @return the component status
     */
    @Override
    public ProcessGroupStatus getControllerStatus() {
        final String rootGroupId = flowManager.getRootGroupId();
        final ProcessGroup group = flowManager.getGroup(rootGroupId);
        final RepositoryStatusReport statusReport = generateRepositoryStatusReport();

        return getGroupStatus(group, statusReport, AUTHORIZATION_APPROVED, Integer.MAX_VALUE, 1, true);
    }

    @Override
    public List<ProvenanceEventRecord> getProvenanceEvents(final long firstEventId, final int maxRecords) throws IOException {
        return new ArrayList<>(getProvenanceRepository().getEvents(firstEventId, maxRecords));
    }

    /**
     * Returns the total number of bytes read by this instance (at the root process group level, i.e. all events) since the instance started
     *
     * @return the total number of bytes read by this instance
     */
    @Override
    public long getTotalBytesRead() {
        return flowFileEventRepository.reportAggregateEvent().getBytesRead();
    }

    /**
     * Returns the total number of bytes written by this instance (at the root process group level, i.e. all events) since the instance started
     *
     * @return the total number of bytes written by this instance
     */
    @Override
    public long getTotalBytesWritten() {
        return flowFileEventRepository.reportAggregateEvent().getBytesWritten();
    }

    /**
     * Returns the total number of bytes sent by this instance (at the root process group level) since the instance started
     *
     * @return the total number of bytes sent by this instance
     */
    @Override
    public long getTotalBytesSent() {
        return flowFileEventRepository.reportAggregateEvent().getBytesSent();
    }

    /**
     * Returns the total number of bytes received by this instance (at the root process group level) since the instance started
     *
     * @return the total number of bytes received by this instance
     */
    @Override
    public long getTotalBytesReceived() {
        return flowFileEventRepository.reportAggregateEvent().getBytesReceived();
    }
}
