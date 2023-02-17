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

import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.HashSet;

public class SearchableMatcherTest extends AbstractAttributeMatcherTest {

    @Mock
    private ProcessorNode component;

    @Mock
    private Processor nonSearchableProcessor;

    @Mock
    private SearchableProcessor searchableProcessor;

    @Mock
    private VariableRegistry variableRegistry;

    @Mock
    private FlowController flowController;

    @Mock
    private ControllerServiceProvider controllerServiceProvider;

    @Mock
    private ExtensionManager extensionManager;

    @Before
    public void setUp() {
        super.setUp();
        Mockito.when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);
        Mockito.when(flowController.getExtensionManager()).thenReturn(extensionManager);
    }

    @Test
    public void testNonSearchableProcessorHasNoMatch() {
        // given
        final SearchableMatcher testSubject = givenTestSubject();
        givenProcessorIsNotSearchable();

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenNoMatches();
    }

    @Test
    public void testSearchableProcessor() {
        // given
        final SearchableMatcher testSubject = givenTestSubject();
        givenProcessorIsSearchable();
        givenSearchResultsAreNotEmpty();
        givenSearchTerm("bbb");

        // when
        testSubject.match(component, searchQuery, matches);

        // then
        thenMatchConsistsOf("aaa: bbb", "bbb: ccc");
    }

    private void givenSearchResultsAreNotEmpty() {
        final Collection<SearchResult> searchResults = new HashSet<>();
        searchResults.add(new SearchResult.Builder().label("aaa").match("bbb").build());
        searchResults.add(new SearchResult.Builder().label("bbb").match("ccc").build());
        Mockito.when(searchableProcessor.search(Mockito.any(SearchContext.class))).thenReturn(searchResults);
    }

    private SearchableMatcher givenTestSubject() {
        final SearchableMatcher result = new SearchableMatcher();
        result.setFlowController(flowController);
        result.setVariableRegistry(variableRegistry);
        return result;
    }

    private void givenProcessorIsSearchable() {
        Mockito.when(component.getProcessor()).thenReturn(searchableProcessor);
    }

    private void givenProcessorIsNotSearchable() {
        Mockito.when(component.getProcessor()).thenReturn(nonSearchableProcessor);
    }

    private interface SearchableProcessor extends Processor, Searchable { }
}