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
package org.apache.nifi.web.search;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.apache.nifi.web.search.attributematchers.AttributeMatcher;
import org.apache.nifi.web.search.query.SearchQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
public class AttributeBasedComponentMatcherTest {
    private static final String IDENTIFIER = "lorem";
    private static final String NAME = "ipsum";

    @Mock
    private ProcessorNode component;

    @Mock
    private SearchQuery searchQuery;

    @Mock
    private AttributeMatcher<ProcessorNode> attributeMatcher1;

    @Mock
    private AttributeMatcher<ProcessorNode> attributeMatcher2;

    @Mock
    private Function<ProcessorNode, String> getIdentifier;

    @Mock
    private Function<ProcessorNode, String> getName;

    @Test
    public void testMatching() {
        // given
        final AttributeBasedComponentMatcher<ProcessorNode> testSubject = new AttributeBasedComponentMatcher<>(givenAttributeMatchers(), getIdentifier, getName);
        Mockito.when(getIdentifier.apply(Mockito.any(ProcessorNode.class))).thenReturn(IDENTIFIER);
        Mockito.when(getName.apply(Mockito.any(ProcessorNode.class))).thenReturn(NAME);
        givenAttributesAreMatching();

        // when
        final Optional<ComponentSearchResultDTO> result = testSubject.match(component, searchQuery);

        // then
        assertTrue(result.isPresent());
        assertEquals(IDENTIFIER, result.get().getId());
        assertEquals(NAME, result.get().getName());
        assertEquals(2, result.get().getMatches().size());
        assertTrue(result.get().getMatches().contains("matcher1"));
        assertTrue(result.get().getMatches().contains("matcher2"));

        Mockito.verify(attributeMatcher1, Mockito.atLeastOnce()).match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class), Mockito.anyList());
        Mockito.verify(attributeMatcher2, Mockito.atLeastOnce()).match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class), Mockito.anyList());
    }

    @Test
    public void testNotMatching() {
        // given
        final AttributeBasedComponentMatcher<ProcessorNode> testSubject = new AttributeBasedComponentMatcher<>(givenAttributeMatchers(), getIdentifier, getName);
        givenAttributesAreNotMatching();

        // when
        final Optional<ComponentSearchResultDTO> result = testSubject.match(component, searchQuery);

        // then
        assertFalse(result.isPresent());
    }

    private List<AttributeMatcher<ProcessorNode>> givenAttributeMatchers() {
        final List<AttributeMatcher<ProcessorNode>> result = new ArrayList<>();
        result.add(attributeMatcher1);
        result.add(attributeMatcher2);
        return result;
    }

    private void givenAttributesAreMatching() {
        Mockito.doAnswer(invocationOnMock -> {
            final List<String> accumulator = invocationOnMock.getArgument(2, List.class);
            accumulator.add("matcher1");
            return accumulator;
        }).when(attributeMatcher1).match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class), Mockito.anyList());

        Mockito.doAnswer(invocationOnMock -> {
            final List<String> accumulator = invocationOnMock.getArgument(2, List.class);
            accumulator.add("matcher2");
            return accumulator;
        }).when(attributeMatcher2).match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class), Mockito.anyList());
    }

    private void givenAttributesAreNotMatching() {
        Mockito.doAnswer(invocationOnMock -> invocationOnMock.getArgument(2, List.class))
                .when(attributeMatcher1).match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class), Mockito.anyList());

        Mockito.doAnswer(invocationOnMock -> invocationOnMock.getArgument(2, List.class))
                .when(attributeMatcher2).match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class), Mockito.anyList());
    }
}