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

import java.util.concurrent.TimeUnit;

public class ExpirationMatcherTest extends AbstractAttributeMatcherTest{

    @Mock
    private Connection component;

    @Mock
    private FlowFileQueue flowFileQueue;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(component.getFlowFileQueue()).thenReturn(flowFileQueue);
    }

    @Test
    public void testWhenKeywordExpiresAppearsAndExpired() {
        // given
        final ExpirationMatcher testSubject = new ExpirationMatcher();
        givenSearchTerm("expires");
        givenExpired();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("FlowFile expiration: 5");
    }

    @Test
    public void testWhenKeywordExpirationAppearsAndExpired() {
        // given
        final ExpirationMatcher testSubject = new ExpirationMatcher();
        givenSearchTerm("expiration");
        givenExpired();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("FlowFile expiration: 5");
    }

    @Test
    public void testWhenNoKeywordAppearsAndExpired() {
        // given
        final ExpirationMatcher testSubject = new ExpirationMatcher();
        givenSearchTerm("lorem");
        givenExpired();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testWhenKeywordExpiresAppearsAndDoesNotExpired() {
        // given
        final ExpirationMatcher testSubject = new ExpirationMatcher();
        givenSearchTerm("expires");
        givenNotExpired();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    private void givenExpired() {
        Mockito.when(flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS)).thenReturn(5);
        Mockito.when(flowFileQueue.getFlowFileExpiration()).thenReturn("5");
    }

    private void givenNotExpired() {
        Mockito.when(flowFileQueue.getFlowFileExpiration(TimeUnit.MILLISECONDS)).thenReturn(0);
    }
}