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
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.TimeUnit;

public class EmbeddedQuestDbStatusHistoryRepositoryForComponentsTest extends AbstractEmbeddedQuestDbStatusHistoryRepositoryTest {

    @Test
    public void testReadingEmptyRepository() throws Exception {
        // when
        final StatusHistory result = testSubject.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END, PREFERRED_DATA_POINTS);

        // then
        assertStatusHistoryIsEmpty(result);
    }

    @Test
    public void testWritingThenReadingComponents() throws Exception {
        // given
        testSubject.capture(new NodeStatus(), givenRootProcessGroupStatus(), new ArrayList<>(), INSERTED_AT);
        givenWaitUntilPersisted();

        // when & then - reading root processor group
        final StatusHistory rootGroupStatus = testSubject.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(rootGroupStatus, ROOT_GROUP_ID, "Root");
        assertRootProcessGroupStatusSnapshot(rootGroupStatus.getStatusSnapshots().get(0));

        // when & then - reading child processor group
        final StatusHistory childGroupStatus = testSubject.getProcessGroupStatusHistory(CHILD_GROUP_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(childGroupStatus, CHILD_GROUP_ID, "Child");
        assertChildProcessGroupStatusSnapshot(childGroupStatus.getStatusSnapshots().get(0));

        //  when & then - reading processor (no-counter processor)
        final StatusHistory processorStatus = testSubject.getProcessorStatusHistory(PROCESSOR_ID, START, END, PREFERRED_DATA_POINTS, false);
        assertCorrectStatusHistory(processorStatus, PROCESSOR_ID, "Processor");
        assertProcessorStatusSnapshot(processorStatus.getStatusSnapshots().get(0));

        //  when & then - reading processor (with-counter processor)
        final StatusHistory processorWithCounterStatus1 = testSubject.getProcessorStatusHistory(PROCESSOR_WITH_COUNTER_ID, START, END, PREFERRED_DATA_POINTS, false);
        assertCorrectStatusHistory(processorWithCounterStatus1, PROCESSOR_WITH_COUNTER_ID, "ProcessorWithCounter");
        assertProcessorWithCounterStatusSnapshot(processorWithCounterStatus1.getStatusSnapshots().get(0), false);

        //  when & then - reading processor (with-counter processor)
        final StatusHistory processorWithCounterStatus2 = testSubject.getProcessorStatusHistory(PROCESSOR_WITH_COUNTER_ID, START, END, PREFERRED_DATA_POINTS, true);
        assertCorrectStatusHistory(processorWithCounterStatus2, PROCESSOR_WITH_COUNTER_ID, "ProcessorWithCounter");
        assertProcessorWithCounterStatusSnapshot(processorWithCounterStatus2.getStatusSnapshots().get(0), true);

        // when & then - reading connection
        final StatusHistory connectionStatus = testSubject.getConnectionStatusHistory(CONNECTION_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(connectionStatus, CONNECTION_ID, "Connection");
        assertConnectionStatusSnapshot(connectionStatus.getStatusSnapshots().get(0));

        // when & then - reading remote process group
        final StatusHistory remoteProcessGroupStatus = testSubject.getRemoteProcessGroupStatusHistory(REMOTE_PROCESS_GROUP_ID, START, END, PREFERRED_DATA_POINTS);
        assertCorrectStatusHistory(remoteProcessGroupStatus, REMOTE_PROCESS_GROUP_ID, "RemoteProcessGroup");
        assertRemoteProcessGroupSnapshot(remoteProcessGroupStatus.getStatusSnapshots().get(0));

        // when & then - requesting data from out of recorded range
        final StatusHistory rootGroupStatus2 = testSubject.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END_EARLY, PREFERRED_DATA_POINTS);
        assertStatusHistoryIsEmpty(rootGroupStatus2);
    }

    @Test
    public void testReadingLimitedByPreferredDataPoints() throws Exception {
        // given
        testSubject.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(8)));
        testSubject.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(7)));
        testSubject.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(6)));
        testSubject.capture(new NodeStatus(), givenSimpleRootProcessGroupStatus(), new ArrayList<>(), new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(5)));
        givenWaitUntilPersisted();

        // when
        final StatusHistory result = testSubject.getProcessGroupStatusHistory(ROOT_GROUP_ID, START, END, 3);

        // then - in case the value of preferred data points are lower than the number of snapshots available, the latest will added to the result
        Assert.assertEquals(3, result.getStatusSnapshots().size());
        Assert.assertEquals(new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(7)), result.getStatusSnapshots().get(0).getTimestamp());
        Assert.assertEquals(new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(6)), result.getStatusSnapshots().get(1).getTimestamp());
        Assert.assertEquals(new Date(INSERTED_AT.getTime() - TimeUnit.MINUTES.toMillis(5)), result.getStatusSnapshots().get(2).getTimestamp());
    }

    private void assertCorrectStatusHistory(final StatusHistory rootGroupStatus, final String id, final String name) {
        Assert.assertEquals(id, rootGroupStatus.getComponentDetails().get("Id"));
        Assert.assertEquals(name, rootGroupStatus.getComponentDetails().get("Name"));
        Assert.assertEquals(1, rootGroupStatus.getStatusSnapshots().size());
    }
}