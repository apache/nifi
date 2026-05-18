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

import org.apache.nifi.connectable.ConnectionUtils.FlowFileCloneResult;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

public class TestConnectionUtils {

    private FlowFileRepository flowFileRepository;
    private ContentRepository contentRepository;

    @BeforeEach
    public void setup() {
        flowFileRepository = Mockito.mock(FlowFileRepository.class);
        contentRepository = Mockito.mock(ContentRepository.class);

        final AtomicLong nextId = new AtomicLong(1000L);
        when(flowFileRepository.getNextFlowFileSequence()).thenAnswer(invocation -> nextId.getAndIncrement());
    }

    @Test
    public void testCloneSingleDestinationProducesCreateRecord() {
        final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", "test-uuid-1")
                .build();

        final FlowFileQueue destinationQueue = Mockito.mock(FlowFileQueue.class);
        final Connection destination = mockConnection(destinationQueue);

        final FlowFileCloneResult result = ConnectionUtils.clone(flowFile, List.of(destination), flowFileRepository, contentRepository);

        final List<RepositoryRecord> records = result.getRepositoryRecords();
        assertEquals(1, records.size());
        final RepositoryRecord record = records.get(0);
        assertEquals(RepositoryRecordType.CREATE, record.getType());
        assertEquals(destinationQueue, record.getDestination());
        assertNotNull(record.getCurrent());
        assertEquals(flowFile.getId(), record.getCurrent().getId());
    }

    @Test
    public void testCloneMultipleDestinationsAllProduceCreateRecords() {
        final ContentClaim contentClaim = Mockito.mock(ContentClaim.class);
        final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                .id(1L)
                .addAttribute("uuid", "test-uuid-1")
                .contentClaim(contentClaim)
                .size(1024L)
                .build();

        final FlowFileQueue queueOne = Mockito.mock(FlowFileQueue.class);
        final FlowFileQueue queueTwo = Mockito.mock(FlowFileQueue.class);
        final FlowFileQueue queueThree = Mockito.mock(FlowFileQueue.class);
        final List<Connection> destinations = List.of(
                mockConnection(queueOne),
                mockConnection(queueTwo),
                mockConnection(queueThree));

        final FlowFileCloneResult result = ConnectionUtils.clone(flowFile, destinations, flowFileRepository, contentRepository);

        final List<RepositoryRecord> records = result.getRepositoryRecords();
        assertEquals(destinations.size(), records.size());

        // Each record produced by clone() represents a FlowFile being introduced to the repository,
        // whether it is the original routed FlowFile or a sibling clone.
        assertTrue(records.stream().allMatch(record -> record.getType() == RepositoryRecordType.CREATE));

        // The clones (one per additional destination beyond the first) should each have triggered a
        // claimant count increment on the shared Content Claim.
        Mockito.verify(contentRepository, Mockito.times(destinations.size() - 1)).incrementClaimaintCount(contentClaim);

        final List<FlowFileQueue> destinationQueues = new ArrayList<>();
        for (final RepositoryRecord record : records) {
            destinationQueues.add(record.getDestination());
        }
        assertTrue(destinationQueues.contains(queueOne));
        assertTrue(destinationQueues.contains(queueTwo));
        assertTrue(destinationQueues.contains(queueThree));
    }

    private Connection mockConnection(final FlowFileQueue queue) {
        final Connection connection = Mockito.mock(Connection.class);
        when(connection.getFlowFileQueue()).thenReturn(queue);
        return connection;
    }
}
