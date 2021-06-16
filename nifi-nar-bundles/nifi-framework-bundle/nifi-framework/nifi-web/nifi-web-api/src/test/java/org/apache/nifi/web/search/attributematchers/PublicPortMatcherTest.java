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
import org.apache.nifi.remote.PublicPort;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.HashSet;

public class PublicPortMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private Port port;

    @Mock
    private PublicPort publicPort;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(publicPort.getUserAccessControl()).thenReturn(new HashSet<>(Arrays.asList("user1Lorem", "user2Lorem")));
        Mockito.when(publicPort.getGroupAccessControl()).thenReturn(new HashSet<>(Arrays.asList("group1Lorem", "group2Lorem")));
    }

    @Test
    public void testNonPublicPort() {
        // given
        final PublicPortMatcher testSubject = new PublicPortMatcher();

        // when
        testSubject.match(port, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testPublicPort() {
        // given
        final PublicPortMatcher testSubject = new PublicPortMatcher();

        // when
        testSubject.match(publicPort, searchQuery, matches);

        // then
        thenMatchConsistsOf("User access control: user1Lorem",
                "User access control: user2Lorem",
                "Group access control: group1Lorem",
                "Group access control: group1Lorem");
    }
}