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
package org.apache.nifi.web.search.resultenrichment;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class ComponentSearchResultEnricherTest {
    private static final String NAME = "name";
    private static final String IDENTIFIER = "identifier";
    private static final String PARENT_IDENTIFIER = "parentIdentifier";
    private static final String PARENT_NAME = "parentName";
    private static final String CONTEXT_IDENTIFIER = "contextIdentifier";
    private static final String CONTEXT_NAME = "contextName";

    @Mock
    private ProcessGroup processGroup;

    @Mock
    private ProcessGroup parentProcessGroup;

    @Mock
    private NiFiUser user;

    @Mock
    private Authorizer authorizer;

    @Mock
    private ParameterContext parameterContext;

    @Test
    public void testGeneralEnrichment() {
        // given
        givenProcessGroup();
        final GeneralComponentSearchResultEnricher testSubject  = new GeneralComponentSearchResultEnricher(processGroup, user, authorizer);
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();

        // when
        testSubject.enrich(result);

        // then
        assertEquals(IDENTIFIER, result.getGroupId());

        assertNotNull(result.getParentGroup());
        assertEquals(IDENTIFIER, result.getParentGroup().getId());
        assertEquals(NAME, result.getParentGroup().getName());

        assertNotNull(result.getVersionedGroup());
        assertEquals(IDENTIFIER, result.getVersionedGroup().getId());
        assertEquals(NAME, result.getVersionedGroup().getName());

    }

    @Test
    public void testProcessGroupEnrichment() {
        // given
        givenParentProcessGroup();
        final ProcessGroupSearchResultEnricher testSubject  = new ProcessGroupSearchResultEnricher(processGroup, user, authorizer);
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();

        // when
        testSubject.enrich(result);

        // then
        assertEquals(PARENT_IDENTIFIER, result.getGroupId());

        assertNotNull(result.getParentGroup());
        assertEquals(PARENT_IDENTIFIER, result.getParentGroup().getId());
        assertEquals(PARENT_NAME, result.getParentGroup().getName());

        assertNotNull(result.getVersionedGroup());
        assertEquals(PARENT_IDENTIFIER, result.getVersionedGroup().getId());
        assertEquals(PARENT_NAME, result.getVersionedGroup().getName());
    }

    @Test
    public void testParameterEnriching() {
        // given
        givenParameterContext();
        final ParameterSearchResultEnricher testSubject = new ParameterSearchResultEnricher(parameterContext);
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();

        // when
        testSubject.enrich(result);

        // then
        thenIdentifierIsNotSet(result);
        assertNotNull(result.getParentGroup());
        assertEquals(CONTEXT_IDENTIFIER, result.getParentGroup().getId());
        assertEquals(CONTEXT_NAME, result.getParentGroup().getName());

        thenVersionedGroupIsNotSet(result);
    }

    @Test
    public void testRootProcessGroupEnrichment() {
        // given
        givenRootProcessGroup();
        final ProcessGroupSearchResultEnricher testSubject  = new ProcessGroupSearchResultEnricher(processGroup, user, authorizer);
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();

        // when
        testSubject.enrich(result);

        // then
        assertEquals(IDENTIFIER, result.getGroupId());
        assertNull(result.getId());
        assertNull(result.getParentGroup());
        thenVersionedGroupIsNotSet(result);
        assertNull(result.getName());
        assertNull(result.getMatches());
    }

    private void givenProcessGroup() {
        Mockito.when(processGroup.getIdentifier()).thenReturn(IDENTIFIER);
        Mockito.when(processGroup.getName()).thenReturn(NAME);
        Mockito.when(processGroup.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(processGroup.getVersionControlInformation()).thenReturn(Mockito.mock(VersionControlInformation.class));
    }

    private void givenParentProcessGroup() {
        Mockito.when(processGroup.getParent()).thenReturn(parentProcessGroup);

        Mockito.when(parentProcessGroup.getIdentifier()).thenReturn(PARENT_IDENTIFIER);
        Mockito.when(parentProcessGroup.getName()).thenReturn(PARENT_NAME);
        Mockito.when(parentProcessGroup.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(parentProcessGroup.getVersionControlInformation()).thenReturn(Mockito.mock(VersionControlInformation.class));
    }
    private void givenParameterContext() {
        Mockito.when(parameterContext.getIdentifier()).thenReturn(CONTEXT_IDENTIFIER);
        Mockito.when(parameterContext.getName()).thenReturn(CONTEXT_NAME);
    }
    private void givenRootProcessGroup() {
        Mockito.when(processGroup.getIdentifier()).thenReturn(IDENTIFIER);
        Mockito.when(processGroup.getParent()).thenReturn(null);
    }

    private void thenIdentifierIsNotSet(final ComponentSearchResultDTO result) {
        assertNull(result.getGroupId());
    }

    private void thenVersionedGroupIsNotSet(final ComponentSearchResultDTO result) {
        assertNull(result.getVersionedGroup());
    }
}