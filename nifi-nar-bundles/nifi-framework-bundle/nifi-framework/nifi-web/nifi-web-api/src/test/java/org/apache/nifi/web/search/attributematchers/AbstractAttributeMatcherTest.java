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

import org.apache.nifi.web.search.query.SearchQuery;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public abstract class AbstractAttributeMatcherTest {
    private static final String SEARCH_TERM = "lorem";

    protected List<String> matches;

    @Mock
    protected SearchQuery searchQuery;

    @BeforeEach
    public void setUp() {
        matches = new ArrayList<>();
    }
    protected void givenDefaultSearchTerm() {
        givenSearchTerm(SEARCH_TERM);
    }
    protected void givenSearchTerm(final String term) {
        Mockito.when(searchQuery.getTerm()).thenReturn(term);
    }

    protected void givenFilter(final String filterName, final String filterValue) {
        Mockito.when(searchQuery.hasFilter(filterName)).thenReturn(true);
        Mockito.when(searchQuery.getFilter(filterName)).thenReturn(filterValue);
    }

    protected void thenNoMatches() {
        assertTrue(matches.isEmpty());
    }

    protected void thenMatchConsistsOf(final String... expectedMatches) {
        assertEquals(expectedMatches.length, matches.size());

        for (final String expectedMatch : expectedMatches) {
            assertTrue(matches.contains(expectedMatch), "Should contain: " + expectedMatch);
        }
    }
}
