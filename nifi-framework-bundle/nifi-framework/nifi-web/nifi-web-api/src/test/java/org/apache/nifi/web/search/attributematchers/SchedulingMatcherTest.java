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
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class SchedulingMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessorNode component;

    @Test
    public void testWhenKeywordAppearsAndNotTimer() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.CRON_DRIVEN);
        givenSearchTerm("timer");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenKeywordDoesNotAppearAndTimer() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.TIMER_DRIVEN);
        givenSearchTerm("cron");

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
    public void testWhenKeywordAppearsAndCron() {
        // given
        final SchedulingMatcher testSubject = new SchedulingMatcher();
        givenSchedulingStrategy(SchedulingStrategy.CRON_DRIVEN);
        givenSearchTerm("cron");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Scheduling strategy: CRON driven");
    }

    private void givenSchedulingStrategy(final SchedulingStrategy schedulingStrategy) {
        Mockito.when(component.getSchedulingStrategy()).thenReturn(schedulingStrategy);
    }
}