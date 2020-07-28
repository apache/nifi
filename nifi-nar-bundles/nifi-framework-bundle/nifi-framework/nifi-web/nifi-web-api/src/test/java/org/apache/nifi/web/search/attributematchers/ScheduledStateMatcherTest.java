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

import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ScheduledStateMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessorNode component;

    @Test
    public void testWhenKeywordAppearsAndDisabled() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenScheduledState(ScheduledState.DISABLED);
        givenSearchTerm("disabled");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Disabled");
    }

    @Test
    public void testWhenKeywordAppearsAndNotDisabled() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenScheduledState(ScheduledState.RUNNING);
        givenSearchTerm("disabled");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenKeywordDoesNotAppearAndDisabled() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenScheduledState(ScheduledState.DISABLED);
        givenSearchTerm("somethingElse");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenInvalidKeywordAppearsAndInvalid() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenValidationStatus(ValidationStatus.INVALID);
        givenSearchTerm("invalid");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Invalid");
    }

    @Test
    public void testWhenInvalidKeywordAppearsAnValid() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenValidationStatus(ValidationStatus.VALID);
        givenSearchTerm("invalid");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenKeywordAppearsAndValid() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenValidationStatus(ValidationStatus.VALIDATING);
        givenSearchTerm("validating");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Validating");
    }

    @Test
    public void testWhenKeywordDoesNotAppearsAndValid() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenValidationStatus(ValidationStatus.VALID);
        givenSearchTerm("invalid");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenLookingForInvalidButTheStatusIsDisabled() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenScheduledState(ScheduledState.DISABLED);
        givenValidationStatus(ValidationStatus.INVALID);
        givenSearchTerm("invalid");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenLookingForValidatingButTheStatusIsDisabled() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenScheduledState(ScheduledState.DISABLED);
        givenValidationStatus(ValidationStatus.VALIDATING);
        givenSearchTerm("validating");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenRunning() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenScheduledState(ScheduledState.RUNNING);
        givenSearchTerm("running");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Running");
    }

    @Test
    public void testWhenStopped() {
        // given
        final ScheduledStateMatcher testSubject = new ScheduledStateMatcher();
        givenScheduledState(ScheduledState.STOPPED);
        givenSearchTerm("stopped");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Run status: Stopped");
    }

    private void givenScheduledState(final ScheduledState scheduledState) {
        Mockito.when(component.getScheduledState()).thenReturn(scheduledState);
    }

    private void givenValidationStatus(final ValidationStatus validationStatus) {
        Mockito.when(component.getValidationStatus()).thenReturn(validationStatus);
    }
}