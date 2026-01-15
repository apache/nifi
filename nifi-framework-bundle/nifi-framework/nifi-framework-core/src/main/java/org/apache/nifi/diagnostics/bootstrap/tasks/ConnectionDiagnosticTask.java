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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.queue.LocalQueuePartitionDiagnostics;
import org.apache.nifi.controller.queue.QueueDiagnostics;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.queue.RemoteQueuePartitionDiagnostics;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.util.FormatUtils;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.List;

public class ConnectionDiagnosticTask implements DiagnosticTask {
    private static final String TABLE_FORMAT = "| %-40s | %-25s | %-25s | %20s | %20s | %20s | %-40s | %15s | %20s | %-19s | %-45s |";
    private static final String TABLE_HEADER = String.format(TABLE_FORMAT,
            "Connection ID", "Source", "Destination", "Queued", "Active", "Swap Queue", "Swap Files", "Unacknowledged", "Penalized", "FlowFile Expiration", "Load Balancing");
    private static final String TABLE_SEPARATOR = "+" + "-".repeat(42) + "+" + "-".repeat(27) + "+" + "-".repeat(27) + "+"
            + "-".repeat(22) + "+" + "-".repeat(22) + "+" + "-".repeat(22) + "+" + "-".repeat(42) + "+" + "-".repeat(17) + "+" + "-".repeat(22) + "+" + "-".repeat(21) + "+" + "-".repeat(47) + "+";

    private final FlowController flowController;

    public ConnectionDiagnosticTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        details.add("=== Connections in Primary Flow ===");
        captureConnectionDiagnostics(rootGroup, details);

        details.add("");
        final ConnectorRepository connectorRepository = flowController.getConnectorRepository();
        final List<ConnectorNode> connectors = connectorRepository != null ? connectorRepository.getConnectors() : List.of();
        if (connectors.isEmpty()) {
            details.add("This instance has no Connectors.");
        } else {
            details.add("=== Connections in Connectors ===");
            for (final ConnectorNode connector : connectors) {
                details.add("");
                details.add("Connector: " + connector.getName() + " (ID: " + connector.getIdentifier() + ", State: " + connector.getCurrentState() + ")");
                final ProcessGroup managedGroup = connector.getActiveFlowContext().getManagedProcessGroup();
                captureConnectionDiagnostics(managedGroup, details);
            }
        }

        return new StandardDiagnosticsDumpElement("Connection Diagnostics", details);
    }

    private void captureConnectionDiagnostics(final ProcessGroup group, final List<String> details) {
        final List<Connection> connections = group.findAllConnections();

        if (connections.isEmpty()) {
            details.add("No connections");
            return;
        }

        details.add(TABLE_SEPARATOR);
        details.add(TABLE_HEADER);
        details.add(TABLE_SEPARATOR);

        for (final Connection connection : connections) {
            captureConnectionRow(connection, details);
            capturePartitionRows(connection, details);
        }

        details.add(TABLE_SEPARATOR);
        details.add("Total: " + connections.size() + " connections");
    }

    private void captureConnectionRow(final Connection connection, final List<String> details) {
        final FlowFileQueue queue = connection.getFlowFileQueue();
        final String connectionId = connection.getIdentifier();
        final String sourceName = truncate(connection.getSource().getName(), 25);
        final String destName = truncate(connection.getDestination().getName(), 25);

        final QueueSize totalSize = queue.size();
        final String queued = formatQueueSize(totalSize);
        final String loadBalancing = formatLoadBalancing(queue);

        final String flowFileExpiration = queue.getFlowFileExpiration();
        final String expirationDisplay = "0 sec".equals(flowFileExpiration) ? "No Expiration" : flowFileExpiration;

        // Gather totals from all partitions
        String active = "-";
        String swapQueue = "-";
        String swapFiles = "-";
        String unack = "-";
        String penalized = "0";

        final QueueDiagnostics diagnostics = queue.getQueueDiagnostics();
        if (diagnostics != null) {
            int totalActive = 0;
            long totalActiveBytes = 0;
            int totalSwapQueue = 0;
            long totalSwapQueueBytes = 0;
            int totalSwapFileCount = 0;
            int totalSwapFlowFiles = 0;
            long totalSwapBytes = 0;
            int totalUnack = 0;
            long totalUnackBytes = 0;
            int totalPenalizedCount = 0;
            long totalPenalizedBytes = 0;

            final LocalQueuePartitionDiagnostics localDiagnostics = diagnostics.getLocalQueuePartitionDiagnostics();
            if (localDiagnostics != null) {
                totalActive += localDiagnostics.getActiveQueueSize().getObjectCount();
                totalActiveBytes += localDiagnostics.getActiveQueueSize().getByteCount();
                totalSwapQueue += localDiagnostics.getSwapQueueSize().getObjectCount();
                totalSwapQueueBytes += localDiagnostics.getSwapQueueSize().getByteCount();
                totalSwapFileCount += localDiagnostics.getSwapFileCount();
                totalSwapFlowFiles += localDiagnostics.getTotalSwapFileQueueSize().getObjectCount();
                totalSwapBytes += localDiagnostics.getTotalSwapFileQueueSize().getByteCount();
                totalUnack += localDiagnostics.getUnacknowledgedQueueSize().getObjectCount();
                totalUnackBytes += localDiagnostics.getUnacknowledgedQueueSize().getByteCount();
                totalPenalizedCount += localDiagnostics.getPenalizedQueueSize().getObjectCount();
                totalPenalizedBytes += localDiagnostics.getPenalizedQueueSize().getByteCount();
            }

            final List<RemoteQueuePartitionDiagnostics> remotePartitions = diagnostics.getRemoteQueuePartitionDiagnostics();
            if (remotePartitions != null) {
                for (final RemoteQueuePartitionDiagnostics remoteDiagnostics : remotePartitions) {
                    totalActive += remoteDiagnostics.getActiveQueueSize().getObjectCount();
                    totalActiveBytes += remoteDiagnostics.getActiveQueueSize().getByteCount();
                    totalSwapQueue += remoteDiagnostics.getSwapQueueSize().getObjectCount();
                    totalSwapQueueBytes += remoteDiagnostics.getSwapQueueSize().getByteCount();
                    totalSwapFileCount += remoteDiagnostics.getSwapFileCount();
                    totalUnack += remoteDiagnostics.getUnacknowledgedQueueSize().getObjectCount();
                    totalUnackBytes += remoteDiagnostics.getUnacknowledgedQueueSize().getByteCount();
                }
            }

            active = formatQueueSize(new QueueSize(totalActive, totalActiveBytes));
            swapQueue = formatQueueSize(new QueueSize(totalSwapQueue, totalSwapQueueBytes));
            swapFiles = formatSwapFiles(totalSwapFileCount, totalSwapFlowFiles, totalSwapBytes);
            unack = formatQueueSize(new QueueSize(totalUnack, totalUnackBytes));
            penalized = formatQueueSize(new QueueSize(totalPenalizedCount, totalPenalizedBytes));
        }

        details.add(String.format(TABLE_FORMAT, connectionId, sourceName, destName, queued, active, swapQueue, swapFiles, unack, penalized, expirationDisplay, loadBalancing));
    }

    private void capturePartitionRows(final Connection connection, final List<String> details) {
        final FlowFileQueue queue = connection.getFlowFileQueue();

        // Only show partition rows if load balancing is enabled
        if (queue.getLoadBalanceStrategy() == LoadBalanceStrategy.DO_NOT_LOAD_BALANCE) {
            return;
        }

        final QueueDiagnostics diagnostics = queue.getQueueDiagnostics();
        if (diagnostics == null) {
            return;
        }

        final LocalQueuePartitionDiagnostics localDiagnostics = diagnostics.getLocalQueuePartitionDiagnostics();
        if (localDiagnostics != null) {
            final String active = formatQueueSize(localDiagnostics.getActiveQueueSize());
            final String swapQueue = formatQueueSize(localDiagnostics.getSwapQueueSize());
            final int swapFileCount = localDiagnostics.getSwapFileCount();
            final QueueSize swapFileQueueSize = localDiagnostics.getTotalSwapFileQueueSize();
            final String swapFiles = formatSwapFiles(swapFileCount, swapFileQueueSize.getObjectCount(), swapFileQueueSize.getByteCount());
            final QueueSize unacknowledgedSize = localDiagnostics.getUnacknowledgedQueueSize();
            final String unack = unacknowledgedSize.getObjectCount() > 0 ? formatQueueSize(unacknowledgedSize) : "-";
            final String penalized = formatQueueSize(localDiagnostics.getPenalizedQueueSize());

            details.add(String.format(TABLE_FORMAT, "    - Local Partition", "", "", "", active, swapQueue, swapFiles, unack, penalized, "", ""));
        }

        final List<RemoteQueuePartitionDiagnostics> remotePartitions = diagnostics.getRemoteQueuePartitionDiagnostics();
        if (remotePartitions != null && !remotePartitions.isEmpty()) {
            for (final RemoteQueuePartitionDiagnostics remoteDiagnostics : remotePartitions) {
                final String nodeId = remoteDiagnostics.getNodeIdentifier();
                final String active = formatQueueSize(remoteDiagnostics.getActiveQueueSize());
                final String swapQueue = formatQueueSize(remoteDiagnostics.getSwapQueueSize());
                final String swapFiles = String.valueOf(remoteDiagnostics.getSwapFileCount());
                final QueueSize unacknowledgedSize = remoteDiagnostics.getUnacknowledgedQueueSize();
                final String unack = unacknowledgedSize.getObjectCount() > 0 ? formatQueueSize(unacknowledgedSize) : "-";

                details.add(String.format(TABLE_FORMAT, "    - Remote Partition - " + nodeId, "", "", "", active, swapQueue, swapFiles, unack, "-", "", ""));
            }
        }
    }

    private String formatLoadBalancing(final FlowFileQueue queue) {
        final LoadBalanceStrategy strategy = queue.getLoadBalanceStrategy();
        if (strategy == LoadBalanceStrategy.DO_NOT_LOAD_BALANCE) {
            return "None";
        }

        final StringBuilder builder = new StringBuilder();
        builder.append(strategy.name());

        if (strategy == LoadBalanceStrategy.PARTITION_BY_ATTRIBUTE) {
            final String partitioningAttribute = queue.getPartitioningAttribute();
            if (partitioningAttribute != null && !partitioningAttribute.isEmpty()) {
                builder.append(" (").append(partitioningAttribute).append(")");
            }
        }

        final LoadBalanceCompression compression = queue.getLoadBalanceCompression();
        if (compression != LoadBalanceCompression.DO_NOT_COMPRESS) {
            builder.append(", ").append(compression.name());
        }

        return builder.toString();
    }

    private String formatQueueSize(final QueueSize queueSize) {
        if (queueSize.getObjectCount() == 0) {
            return "0";
        }
        final String formattedCount = NumberFormat.getIntegerInstance().format(queueSize.getObjectCount());
        return formattedCount + " (" + FormatUtils.formatDataSize(queueSize.getByteCount()) + ")";
    }

    private String formatSwapFiles(final int fileCount, final int flowFileCount, final long totalBytes) {
        if (fileCount == 0) {
            return "0";
        }
        final String files = fileCount == 1 ? "1 File" : NumberFormat.getIntegerInstance().format(fileCount) + " Files";
        final String flowFiles = NumberFormat.getIntegerInstance().format(flowFileCount) + " FlowFiles";
        final String size = FormatUtils.formatDataSize(totalBytes);
        return files + " - " + flowFiles + " (" + size + ")";
    }

    private String truncate(final String value, final int maxLength) {
        if (value == null) {
            return "";
        }
        if (value.length() <= maxLength) {
            return value;
        }
        return value.substring(0, maxLength - 3) + "...";
    }
}
