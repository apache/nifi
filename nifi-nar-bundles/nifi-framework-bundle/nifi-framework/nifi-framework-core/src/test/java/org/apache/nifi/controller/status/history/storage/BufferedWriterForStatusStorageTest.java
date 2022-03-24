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
package org.apache.nifi.controller.status.history.storage;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.nifi.controller.status.NodeStatus;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class BufferedWriterForStatusStorageTest {
    private static final int BUFFER_SIZE = 3;

    @Mock
    StatusStorage<NodeStatus> payload;

    @Test
    public void testStoringOnlyWhenPersist() {
        // given
        final BufferedWriterForStatusStorage<NodeStatus> testSubject = new BufferedWriterForStatusStorage<>(payload, BUFFER_SIZE);
        final ArgumentCaptor<List> statusEntriesCaptor = ArgumentCaptor.forClass(List.class);
        Mockito.doNothing().when(payload).store(statusEntriesCaptor.capture());

        // when
        for (int i = 0; i <= 5; i++) {
            testSubject.collect(new ImmutablePair<>(Instant.now(), new NodeStatus()));
        }

        // then
        Mockito.verify(payload, Mockito.never()).store(Mockito.anyList());

        // when
        testSubject.flush();

        // then
        Mockito.verify(payload, Mockito.only()).store(Mockito.anyList());
        Assert.assertEquals(BUFFER_SIZE, statusEntriesCaptor.getValue().size());
    }
}