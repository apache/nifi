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

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

public class ConnectivityMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private Connection component;

    @Mock
    private Connectable source;

    @Mock
    private Connectable destination;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(component.getSource()).thenReturn(source);
        Mockito.when(component.getDestination()).thenReturn(destination);

        Mockito.when(source.getIdentifier()).thenReturn("SourceId");
        Mockito.when(source.getName()).thenReturn("SourceName");
        Mockito.when(source.getComments()).thenReturn("SourceComment");

        Mockito.when(destination.getIdentifier()).thenReturn("DestinationId");
        Mockito.when(destination.getName()).thenReturn("DestinationName");
        Mockito.when(destination.getComments()).thenReturn("DestinationComment");
    }

    @Test
    public void testSourceMatching() {
        // given
        final ConnectivityMatcher testSubject = new ConnectivityMatcher();
        givenSearchTerm("source");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Source id: SourceId", //
                "Source name: SourceName", //
                "Source comments: SourceComment");
    }

    @Test
    public void testDestinationMatching() {
        // given
        final ConnectivityMatcher testSubject = new ConnectivityMatcher();
        givenSearchTerm("destination");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Destination id: DestinationId", //
                "Destination name: DestinationName", //
                "Destination comments: DestinationComment");
    }

    @Test
    public void testBothMatching() {
        // given
        final ConnectivityMatcher testSubject = new ConnectivityMatcher();
        givenSearchTerm("Name");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("Source name: SourceName", //
                "Destination name: DestinationName");
    }
}