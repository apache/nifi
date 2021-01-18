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

import org.apache.nifi.groups.RemoteProcessGroup;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class TransmissionStatusMatcherTest extends AbstractAttributeMatcherTest{

    @Mock
    private RemoteProcessGroup component;

    @Test
    public void testWhenTransmittingKeywordAndIsTransmitting() {
        // given
        final TransmissionStatusMatcher testSubject = new TransmissionStatusMatcher();
        givenTransmitting();
        givenSearchTerm("transmitting");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Transmission: On");
    }

    @Test
    public void testWhenTransmittingKeywordAndIsNotTransmitting() {
        // given
        final TransmissionStatusMatcher testSubject = new TransmissionStatusMatcher();
        givenNotTransmitting();
        givenSearchTerm("transmitting");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenNotTransmittingKeywordAndIsNotTransmitting() {
        // given
        final TransmissionStatusMatcher testSubject = new TransmissionStatusMatcher();
        givenNotTransmitting();
        givenSearchTerm("not transmitting");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Transmission: Off");
    }

    @Test
    public void testWhenNotTransmittingKeywordAndIsTransmitting() {
        // given
        final TransmissionStatusMatcher testSubject = new TransmissionStatusMatcher();
        givenTransmitting();
        givenSearchTerm("not transmitting");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    private void givenTransmitting() {
        Mockito.when(component.isTransmitting()).thenReturn(true);
    }

    private void givenNotTransmitting() {
        Mockito.when(component.isTransmitting()).thenReturn(false);
    }
}