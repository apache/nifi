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

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ConnectionUtils {
    /**
     * Creates the FlowFileRecords and the RepositoryRecords necessary for transferring the given FlowFile to all of the given destinations.
     * If only a single destination is given, the returned result will contain only the original FlowFile and a RepositoryRecord that represents that
     * FlowFile. If multiple destinations are given, the returned result will contain the original FlowFile, as well as a cloned FlowFile for each of
     * the destinations. If one or more clones are created, and a Content Repository is provided, the Claimant Count will be incremented for each clone
     * (but not the original FlowFile). If the Content Repository is <code>null</code>, the Claimant Counts will not be incremented.
     *
     * @param flowFile the FlowFile to clone as necessary
     * @param destinations the destinations that the FlowFile will be transferred to
     * @param flowFileRepository the FlowFile Repository to use to generate the FlowFile ID for each clone
     * @param contentRepository the Content Repository to use to increment the Claimant Count for each clone, or <code>null</code> if the caller will
     * take on the responsibility of incrementing the Claimant Counts
     * @return a FlowFileCloneResult that contains the FlowFiles to enqueue and the RepositoryRecords to create
     */
    public static FlowFileCloneResult clone(final FlowFileRecord flowFile, final Collection<Connection> destinations,
                             final FlowFileRepository flowFileRepository, final ContentRepository contentRepository) {

        final Map<FlowFileQueue, List<FlowFileRecord>> flowFilesToEnqueue = new HashMap<>();

        // If only a single destination, we can simply add the FlowFile to the destination.
        if (destinations.size() == 1) {
            final Connection firstConnection = destinations.iterator().next();
            flowFilesToEnqueue.put(firstConnection.getFlowFileQueue(), Collections.singletonList(flowFile));
            final RepositoryRecord repoRecord = createRepositoryRecord(flowFile, firstConnection.getFlowFileQueue());
            return new FlowFileCloneResult(flowFilesToEnqueue, Collections.singletonList(repoRecord));
        }

        final List<RepositoryRecord> repositoryRecords = new ArrayList<>();

        final Iterator<Connection> itr = destinations.iterator();
        final Connection firstConnection = itr.next();

        // Clone the FlowFile to all other destinations.
        while (itr.hasNext()) {
            final Connection destination = itr.next();
            final StandardFlowFileRecord.Builder builder = new StandardFlowFileRecord.Builder().fromFlowFile(flowFile);
            final long id = flowFileRepository.getNextFlowFileSequence();
            builder.id(id);

            final String newUuid = UUID.randomUUID().toString();
            builder.addAttribute(CoreAttributes.UUID.key(), newUuid);

            final FlowFileRecord clone = builder.build();

            final ContentClaim claim = clone.getContentClaim();
            if (claim != null && contentRepository != null) {
                contentRepository.incrementClaimaintCount(claim);
            }

            final RepositoryRecord repoRecord = createRepositoryRecord(clone, destination.getFlowFileQueue());
            repositoryRecords.add(repoRecord);

            final List<FlowFileRecord> flowFiles = flowFilesToEnqueue.computeIfAbsent(destination.getFlowFileQueue(), k -> new ArrayList<>());
            flowFiles.add(clone);
        }

        // Enqueue the FlowFile into the first connection
        flowFilesToEnqueue.put(firstConnection.getFlowFileQueue(), Collections.singletonList(flowFile));
        repositoryRecords.add(createRepositoryRecord(flowFile, firstConnection.getFlowFileQueue()));

        return new FlowFileCloneResult(flowFilesToEnqueue, repositoryRecords);
    }

    private static RepositoryRecord createRepositoryRecord(final FlowFileRecord flowFile, final FlowFileQueue destinationQueue) {
        final StandardRepositoryRecord repoRecord = new StandardRepositoryRecord(null, flowFile);
        repoRecord.setWorking(flowFile, Collections.emptyMap(), false);
        repoRecord.setDestination(destinationQueue);
        return repoRecord;
    }

    public static class FlowFileCloneResult {
        private final Map<FlowFileQueue, List<FlowFileRecord>> flowFilesToEnqueue;
        private final List<RepositoryRecord> repositoryRecords;

        private FlowFileCloneResult(final Map<FlowFileQueue, List<FlowFileRecord>> flowFilesToEnqueue, final List<RepositoryRecord> repositoryRecords) {
            this.flowFilesToEnqueue = flowFilesToEnqueue;
            this.repositoryRecords = repositoryRecords;
        }

        public List<RepositoryRecord> getRepositoryRecords() {
            return repositoryRecords;
        }

        public int distributeFlowFiles() {
            if (flowFilesToEnqueue.isEmpty()) {
                return 0;
            }

            int enqueued = 0;
            for (final Map.Entry<FlowFileQueue, List<FlowFileRecord>> entry : flowFilesToEnqueue.entrySet()) {
                final FlowFileQueue queue = entry.getKey();
                final List<FlowFileRecord> flowFiles = entry.getValue();
                if (!flowFiles.isEmpty()) {
                    queue.putAll(flowFiles);
                    enqueued += flowFiles.size();
                }
            }

            return enqueued;
        }

        public Map<FlowFileQueue, List<FlowFileRecord>> getFlowFilesToEnqueue() {
            return flowFilesToEnqueue;
        }
    }
}
