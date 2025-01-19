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
package org.apache.nifi.controller.status.history.questdb;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StatusSnapshot;

import java.util.Collection;
import java.util.Date;
import java.util.List;

interface StatusHistoryStorage {

    default void init() { }
    default void close() { }

    List<StatusSnapshot> getConnectionSnapshots(final String componentId, final Date start, final Date end);
    List<StatusSnapshot> getProcessGroupSnapshots(final String componentId, final Date start, final Date end);
    List<StatusSnapshot> getRemoteProcessGroupSnapshots(final String componentId, final Date start, final Date end);
    List<StatusSnapshot> getProcessorSnapshots(final String componentId, final Date start, final Date end);
    List<StatusSnapshot> getProcessorSnapshotsWithCounters(final String componentId, final Date start, final Date end);
    List<StatusSnapshot> getNodeStatusSnapshots(final Date start, final Date end);
    List<GarbageCollectionStatus> getGarbageCollectionSnapshots(final Date start, final Date end);

    void storeNodeStatuses(final Collection<CapturedStatus<NodeStatus>> statuses);
    void storeGarbageCollectionStatuses(final Collection<CapturedStatus<GarbageCollectionStatus>> statuses);
    void storeProcessGroupStatuses(final Collection<CapturedStatus<ProcessGroupStatus>> statuses);
    void storeConnectionStatuses(final Collection<CapturedStatus<ConnectionStatus>> statuses);
    void storeRemoteProcessorGroupStatuses(final Collection<CapturedStatus<RemoteProcessGroupStatus>> statuses);
    void storeProcessorStatuses(final Collection<CapturedStatus<ProcessorStatus>> statuses);
}
