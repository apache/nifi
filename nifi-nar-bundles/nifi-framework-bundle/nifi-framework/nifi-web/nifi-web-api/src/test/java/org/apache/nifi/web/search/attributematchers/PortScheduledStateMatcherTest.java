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

import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ScheduledState;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class PortScheduledStateMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private Port component;

    @Test
    public void testDisabledMatches() {
        // given
        final PortScheduledStateMatcher testSubject = new PortScheduledStateMatcher();
        givenSearchTerm("disabled");
        givenStatus(ScheduledState.DISABLED);

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Disabled");
    }

    @Test
    public void testDisabledAndInvalidWhenSearchedForInvalid() {
        // given
        final PortScheduledStateMatcher testSubject = new PortScheduledStateMatcher();
        givenSearchTerm("invalid");
        givenInvalid();
        givenStatus(ScheduledState.DISABLED);

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testRunningMatches() {
        // given
        final PortScheduledStateMatcher testSubject = new PortScheduledStateMatcher();
        givenSearchTerm("running");
        givenStatus(ScheduledState.RUNNING);

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Running");
    }

    @Test
    public void testStoppedMatches() {
        // given
        final PortScheduledStateMatcher testSubject = new PortScheduledStateMatcher();
        givenSearchTerm("stopped");
        givenStatus(ScheduledState.STOPPED);

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Stopped");
    }

    @Test
    public void testStatusDoesNotMatch() {
        // given
        final PortScheduledStateMatcher testSubject = new PortScheduledStateMatcher();
        givenSearchTerm("stopped");
        givenStatus(ScheduledState.RUNNING);

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testInvalidMatches() {
        // given
        final PortScheduledStateMatcher testSubject = new PortScheduledStateMatcher();
        givenSearchTerm("invalid");
        givenInvalid();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Invalid");
    }

    @Test
    public void testInvalidDoesNotMatch() {
        // given
        final PortScheduledStateMatcher testSubject = new PortScheduledStateMatcher();
        givenSearchTerm("invalid");
        givenValid();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    private void givenStatus(final ScheduledState status) {
        Mockito.when(component.getScheduledState()).thenReturn(status);
    }

    private void givenValid() {
        Mockito.when(component.isValid()).thenReturn(true);
    }

    private void givenInvalid() {
        Mockito.when(component.isValid()).thenReturn(false);
    }
}