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

package org.apache.nifi.connectable;

import org.apache.nifi.groups.ProcessGroup;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessGroupFlowFileActivity implements FlowFileActivity {
    private final ProcessGroup group;

    public ProcessGroupFlowFileActivity(final ProcessGroup group) {
        this.group = group;
    }

    @Override
    public void updateLatestActivityTime() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateTransferCounts(final int receivedCount, final long receivedBytes, final int sentCount, final long sentBytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
    }

    @Override
    public OptionalLong getLatestActivityTime() {
        final AtomicLong latestActivityTime = new AtomicLong(-1L);

        // Check all processors
        for (final Connectable connectable : group.getProcessors()) {
            final OptionalLong activityTime = connectable.getFlowFileActivity().getLatestActivityTime();
            activityTime.ifPresent(time -> {
                if (time > latestActivityTime.get()) {
                    latestActivityTime.set(time);
                }
            });
        }

        // Check all input ports
        for (final Connectable connectable : group.getInputPorts()) {
            final OptionalLong activityTime = connectable.getFlowFileActivity().getLatestActivityTime();
            activityTime.ifPresent(time -> {
                if (time > latestActivityTime.get()) {
                    latestActivityTime.set(time);
                }
            });
        }

        // Check all output ports
        for (final Connectable connectable : group.getOutputPorts()) {
            final OptionalLong activityTime = connectable.getFlowFileActivity().getLatestActivityTime();
            activityTime.ifPresent(time -> {
                if (time > latestActivityTime.get()) {
                    latestActivityTime.set(time);
                }
            });
        }

        // Check all funnels
        for (final Connectable connectable : group.getFunnels()) {
            final OptionalLong activityTime = connectable.getFlowFileActivity().getLatestActivityTime();
            activityTime.ifPresent(time -> {
                if (time > latestActivityTime.get()) {
                    latestActivityTime.set(time);
                }
            });
        }

        // Check stateless group node if present
        group.getStatelessGroupNode().ifPresent(statelessGroupNode -> {
            final OptionalLong activityTime = statelessGroupNode.getFlowFileActivity().getLatestActivityTime();
            activityTime.ifPresent(time -> {
                if (time > latestActivityTime.get()) {
                    latestActivityTime.set(time);
                }
            });
        });

        final long result = latestActivityTime.get();
        return result == -1L ? OptionalLong.empty() : OptionalLong.of(result);
    }

    @Override
    public FlowFileTransferCounts getTransferCounts() {
        long totalReceivedCount = 0L;
        long totalReceivedBytes = 0L;
        long totalSentCount = 0L;
        long totalSentBytes = 0L;

        // Aggregate transfer counts from all processors
        for (final Connectable connectable : group.getProcessors()) {
            final FlowFileTransferCounts counts = connectable.getFlowFileActivity().getTransferCounts();
            totalReceivedCount += counts.getReceivedCount();
            totalReceivedBytes += counts.getReceivedBytes();
            totalSentCount += counts.getSentCount();
            totalSentBytes += counts.getSentBytes();
        }

        // Aggregate transfer counts from all input ports
        for (final Connectable connectable : group.getInputPorts()) {
            final FlowFileTransferCounts counts = connectable.getFlowFileActivity().getTransferCounts();
            totalReceivedCount += counts.getReceivedCount();
            totalReceivedBytes += counts.getReceivedBytes();
            totalSentCount += counts.getSentCount();
            totalSentBytes += counts.getSentBytes();
        }

        // Aggregate transfer counts from all output ports
        for (final Connectable connectable : group.getOutputPorts()) {
            final FlowFileTransferCounts counts = connectable.getFlowFileActivity().getTransferCounts();
            totalReceivedCount += counts.getReceivedCount();
            totalReceivedBytes += counts.getReceivedBytes();
            totalSentCount += counts.getSentCount();
            totalSentBytes += counts.getSentBytes();
        }

        // Aggregate transfer counts from all funnels
        for (final Connectable connectable : group.getFunnels()) {
            final FlowFileTransferCounts counts = connectable.getFlowFileActivity().getTransferCounts();
            totalReceivedCount += counts.getReceivedCount();
            totalReceivedBytes += counts.getReceivedBytes();
            totalSentCount += counts.getSentCount();
            totalSentBytes += counts.getSentBytes();
        }

        return new FlowFileTransferCounts(totalReceivedCount, totalReceivedBytes, totalSentCount, totalSentBytes);
    }
}
