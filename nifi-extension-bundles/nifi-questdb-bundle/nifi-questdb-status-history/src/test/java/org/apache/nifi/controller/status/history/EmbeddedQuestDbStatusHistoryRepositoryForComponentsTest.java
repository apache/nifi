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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.controller.status.NodeStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EmbeddedQuestDbStatusHistoryRepositoryForComponentsTest extends AbstractEmbeddedQuestDbStatusHistoryRepositoryTest {

    @Test
    public void testReadingEmptyRepository() {
        final StatusHistory result = repository.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END, PREFERRED_DATA_POINTS);

        assertStatusHistoryIsEmpty(result);
    }

    @Test
    public void testWritingThenReadingComponents() throws Exception {
        repository.capture(new NodeStatus(), givenRootProcessGroupStatus(), new ArrayList<>(), INSERTED_AT);
        waitUntilPersisted();

        // reading root processor group
        final StatusHistory rootGroupStatus = repository.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(rootGroupStatus, ROOT_GROUP_ID, "Root");
        assertRootProcessGroupStatusSnapshot(rootGroupStatus.getStatusSnapshots().get(0));

        // reading child processor group
        final StatusHistory childGroupStatus = repository.getProcessGroupStatusHistory(CHILD_GROUP_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(childGroupStatus, CHILD_GROUP_ID, "Child");
        assertChildProcessGroupStatusSnapshot(childGroupStatus.getStatusSnapshots().get(0));

        // reading processor (no-counter processor)
        final StatusHistory processorStatus = repository.getProcessorStatusHistory(PROCESSOR_ID, START, END, PREFERRED_DATA_POINTS, false);
        assertCorrectStatusHistory(processorStatus, PROCESSOR_ID, "Processor");
        assertProcessorStatusSnapshot(processorStatus.getStatusSnapshots().get(0));

        // reading processor (with-counter processor)
        final StatusHistory processorWithCounterStatus1 = repository.getProcessorStatusHistory(PROCESSOR_WITH_COUNTER_ID, START, END, PREFERRED_DATA_POINTS, false);
        assertCorrectStatusHistory(processorWithCounterStatus1, PROCESSOR_WITH_COUNTER_ID, "ProcessorWithCounter");
        assertProcessorWithCounterStatusSnapshot(processorWithCounterStatus1.getStatusSnapshots().get(0), false);

        // reading processor (with-counter processor)
        final StatusHistory processorWithCounterStatus2 = repository.getProcessorStatusHistory(PROCESSOR_WITH_COUNTER_ID, START, END, PREFERRED_DATA_POINTS, true);
        assertCorrectStatusHistory(processorWithCounterStatus2, PROCESSOR_WITH_COUNTER_ID, "ProcessorWithCounter");
        assertProcessorWithCounterStatusSnapshot(processorWithCounterStatus2.getStatusSnapshots().get(0), true);

        // reading connection
        final StatusHistory connectionStatus = repository.getConnectionStatusHistory(CONNECTION_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(connectionStatus, CONNECTION_ID, "Connection");
        assertConnectionStatusSnapshot(connectionStatus.getStatusSnapshots().get(0));

        // reading remote process group
        final StatusHistory remoteProcessGroupStatus = repository.getRemoteProcessGroupStatusHistory(REMOTE_PROCESS_GROUP_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(remoteProcessGroupStatus, REMOTE_PROCESS_GROUP_ID, "RemoteProcessGroup");
        assertRemoteProcessGroupSnapshot(remoteProcessGroupStatus.getStatusSnapshots().get(0));

        // requesting data from out of recorded range
        final StatusHistory rootGroupStatus2 = repository.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END_EARLY, PREFERRED_DATA_POINTS);
        assertStatusHistoryIsEmpty(rootGroupStatus2);
    }

    @Test
    public void testReadingLimitedByPreferredDataPoints() throws Exception {
        repository.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(8)));
        repository.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(7)));
        repository.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(6)));
        repository.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(5)));
        waitUntilPersisted();

        final StatusHistory result = repository.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END, 3);

        // in case the value of preferred data points are lower than the number of snapshots available, the latest will added to the result
        assertEquals(3, result.getStatusSnapshots().size());
        assertEquals(new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(7)), result.getStatusSnapshots().get(0).getTimestamp());
        assertEquals(new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(6)), result.getStatusSnapshots().get(1).getTimestamp());
        assertEquals(new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(5)), result.getStatusSnapshots().get(2).getTimestamp());
    }

    private void assertCorrectStatusHistory(final StatusHistory rootGroupStatus, final String id, final String name) {
        assertEquals(id, rootGroupStatus.getComponentDetails().get("Id"));
        assertEquals(name, rootGroupStatus.getComponentDetails().get("Name"));
        assertEquals(1, rootGroupStatus.getStatusSnapshots().size());
    }
}
