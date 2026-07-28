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
package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.repository.FlowFileRecord;

import java.util.List;
import java.util.Objects;

/**
 * An atomic, point-in-time view of a {@link FlowFileQueue}'s state, returned by
 * {@link FlowFileQueue#getQueueSnapshot()}. Bundles the queue's total {@link QueueSize} with the
 * {@link FlowFileRecord}s held in this node's active in-memory queue at the moment the snapshot was
 * taken. Both fields are captured while every partition that contributes to the {@link QueueSize} is
 * locked, so callers can correlate them without race conditions: when {@code activeFlowFiles().size()}
 * equals {@code queueSize().getObjectCount()}, the active list is exhaustive.
 *
 * <p>
 *     The {@link #activeFlowFiles()} list is the in-memory active subset in poll order. It excludes
 *     FlowFiles that are swapped to disk or pending swap-out, held by the destination, or — for
 *     load-balanced connections — assigned to remote partitions (destined for other cluster nodes) or
 *     in the rebalancing partition (being redistributed). All of those FlowFiles are still counted in
 *     {@link #queueSize()}. The {@code activeFlowFiles} list is defensively copied into an immutable
 *     list, so callers may safely retain the snapshot beyond the original lock acquisition.
 * </p>
 *
 * @param queueSize the total {@link QueueSize} of the queue at the time the snapshot was taken,
 *                  including any FlowFiles excluded from {@link #activeFlowFiles()}
 * @param activeFlowFiles the active in-memory FlowFiles in poll order — the order in which the queue's
 *                        configured {@link org.apache.nifi.flowfile.FlowFilePrioritizer}s would surface
 *                        them to a consumer — never {@code null}; may be empty
 */
public record FlowFileQueueSnapshot(QueueSize queueSize, List<FlowFileRecord> activeFlowFiles) {

    public FlowFileQueueSnapshot {
        Objects.requireNonNull(queueSize, "queueSize");
        Objects.requireNonNull(activeFlowFiles, "activeFlowFiles");
        activeFlowFiles = List.copyOf(activeFlowFiles);
    }
}
