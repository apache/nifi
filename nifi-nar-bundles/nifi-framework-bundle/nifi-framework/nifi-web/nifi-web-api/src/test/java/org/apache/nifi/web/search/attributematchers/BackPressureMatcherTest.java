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
package org.apache.nifi.web.search.attributematchers;

import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class BackPressureMatcherTest extends AbstractAttributeMatcherTest {

    private final BackPressureMatcher testSubject = new BackPressureMatcher();

    @Mock
    private Connection connection;

    @Mock
    private FlowFileQueue flowFileQueue;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(connection.getFlowFileQueue()).thenReturn(flowFileQueue);
    }

    @Test
    public void testWhenNoKeywordThenNoMatching() {
        // given
        givenThereIsBackPressure();

        // when
        testSubject.match(connection, searchQuery, matches);

        // then
        thenNoMatches();
        Mockito.verify(connection, Mockito.never()).getFlowFileQueue();
    }

    @Test
    public void testKeywordMatchesAndThereIsBackPressure() {
        // given
        givenSearchTerm("presSURE");
        givenThereIsBackPressure();

        // when
        testSubject.match(connection, searchQuery, matches);

        // then
        thenMatchConsistsOf("Back pressure data size: 100 KB", "Back pressure count: 5");
        Mockito.verify(connection, Mockito.atLeastOnce()).getFlowFileQueue();
    }

    @Test
    public void testKeywordMatchesAndThereIsNoBackPressure() {
        // given
        givenSearchTerm("back pressure");
        givenThereIsNoBackPressure();

        // when
        testSubject.match(connection, searchQuery, matches);

        // then
        thenNoMatches();
        Mockito.verify(connection, Mockito.atLeastOnce()).getFlowFileQueue();
    }

    private void givenThereIsBackPressure() {
        Mockito.when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn("100 KB");
        Mockito.when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(5L);
    }

    private void givenThereIsNoBackPressure() {
        Mockito.when(flowFileQueue.getBackPressureDataSizeThreshold()).thenReturn("0 B");
        Mockito.when(flowFileQueue.getBackPressureObjectThreshold()).thenReturn(0L);
    }
}