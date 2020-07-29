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

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class SchedulingMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessorNode component;

    @Test
    public void testWhenKeywordAppearsAndEvent() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.EVENT_DRIVEN);
        givenSearchTerm("event");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Scheduling strategy: Event driven");
    }

    @Test
    public void testWhenKeywordAppearsAndNotEvent() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        givenSearchTerm("event");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenKeywordDoesNotAppearAndEvent() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        givenSearchTerm("event");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenKeywordAppearsAndTimer() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        givenSearchTerm("timer");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Scheduling strategy: Timer driven");
    }

    @Test
    public void testWhenKeywordAppearsAndPrimaryNodeOnly() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.PRIMARY_NODE_ONLY);
        givenSearchTerm("primary");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Scheduling strategy: On primary node");
    }

    private void givenSchedulingStrategy(final SchedulingStrategy schedulingStrategy) {
        Mockito.when(component.getSchedulingStrategy()).thenReturn(schedulingStrategy);
    }
}