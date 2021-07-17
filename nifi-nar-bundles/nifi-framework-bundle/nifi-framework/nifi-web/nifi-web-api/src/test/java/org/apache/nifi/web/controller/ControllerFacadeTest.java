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
package org.apache.nifi.web.controller;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.search.query.SearchQuery;
import org.apache.nifi.web.search.query.SearchQueryParser;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.AdditionalMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ControllerFacadeTest {
    private static final String ACTIVE_GROUP_ID = "activeId";
    private static final String SEARCH_LITERAL = "processor1";

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ProcessGroup rootGroup;

    @Mock
    private ProcessGroup activeGroup;

    @Mock
    private SearchQueryParser searchQueryParser;

    @Mock
    private SearchQuery searchQuery;

    @Mock
    private ControllerSearchService controllerSearchService;

    @Before
    public void setUp() {
        Mockito.when(flowController.getFlowManager()).thenReturn(flowManager);
        Mockito.when(flowManager.getRootGroup()).thenReturn(rootGroup);
        Mockito.when(flowManager.getGroup(ACTIVE_GROUP_ID)).thenReturn(activeGroup);
        // The NiFi user is null due to the production code acquires it from a static call
        Mockito.when(searchQueryParser.parse(
                Mockito.anyString(),
                AdditionalMatchers.or(Mockito.any(NiFiUser.class), Mockito.isNull()),
                Mockito.any(ProcessGroup.class),
                Mockito.any(ProcessGroup.class))
        ).thenReturn(searchQuery);
        Mockito.when(searchQuery.getTerm()).thenReturn(SEARCH_LITERAL);
    }

    @Test
    public void testExistingActiveGroupIsSentDownToSearch() {
        // given
        final ControllerFacade testSubject = givenTestSubject();

        // when
        testSubject.search(SEARCH_LITERAL, ACTIVE_GROUP_ID);

        // then
        Mockito.verify(searchQueryParser, Mockito.times(1))
                .parse(Mockito.eq(SEARCH_LITERAL), AdditionalMatchers.or(Mockito.any(NiFiUser.class), Mockito.isNull()), Mockito.same(rootGroup), Mockito.same(activeGroup));

        Mockito.verify(controllerSearchService, Mockito.times(1)).search(Mockito.same(searchQuery), Mockito.any(SearchResultsDTO.class));
        Mockito.verify(controllerSearchService, Mockito.times(1)).searchParameters(Mockito.same(searchQuery), Mockito.any(SearchResultsDTO.class));
    }

    @Test
    public void testSearchUsesRootGroupAsActiveIfNotProvided() {
        // given
        final ControllerFacade testSubject = givenTestSubject();

        // when
        testSubject.search(SEARCH_LITERAL, null);

        // then
        Mockito.verify(searchQueryParser, Mockito.times(1))
                .parse(Mockito.eq(SEARCH_LITERAL), AdditionalMatchers.or(Mockito.any(NiFiUser.class), Mockito.isNull()), Mockito.same(rootGroup), Mockito.same(rootGroup));
    }

    private ControllerFacade givenTestSubject() {
        final ControllerFacade testSubject = new ControllerFacade();
        testSubject.setFlowController(flowController);
        testSubject.setSearchQueryParser(searchQueryParser);
        testSubject.setControllerSearchService(controllerSearchService);
        return testSubject;
    }
}