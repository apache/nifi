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
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
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
    private ProcessGroup rootGroup;

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
        Assert.assertEquals(IDENTIFIER, result.getGroupId());

        Assert.assertNotNull(result.getParentGroup());
        Assert.assertEquals(IDENTIFIER, result.getParentGroup().getId());
        Assert.assertEquals(NAME, result.getParentGroup().getName());

        Assert.assertNotNull(result.getVersionedGroup());
        Assert.assertEquals(IDENTIFIER, result.getVersionedGroup().getId());
        Assert.assertEquals(NAME, result.getVersionedGroup().getName());

    }

    @Test
    public void testProcessGroupEnrichment() {
        // given
        givenProcessGroup();
        final ProcessGroupSearchResultEnricher testSubject  = new ProcessGroupSearchResultEnricher(processGroup, user, authorizer);
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();

        // when
        testSubject.enrich(result);

        // then
        Assert.assertEquals(PARENT_IDENTIFIER, result.getGroupId());

        Assert.assertNotNull(result.getParentGroup());
        Assert.assertEquals(PARENT_IDENTIFIER, result.getParentGroup().getId());
        Assert.assertEquals(PARENT_NAME, result.getParentGroup().getName());

        Assert.assertNotNull(result.getVersionedGroup());
        Assert.assertEquals(PARENT_IDENTIFIER, result.getVersionedGroup().getId());
        Assert.assertEquals(PARENT_NAME, result.getVersionedGroup().getName());
    }

    @Test
    public void testParameterEnriching() {
        // given
        givenProcessGroup();
        final ParameterSearchResultEnricher testSubject = new ParameterSearchResultEnricher(parameterContext);
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();

        // when
        testSubject.enrich(result);

        // then
        thenIdentifierIsNotSet(result);
        Assert.assertNotNull(result.getParentGroup());
        Assert.assertEquals(CONTEXT_IDENTIFIER, result.getParentGroup().getId());
        Assert.assertEquals(CONTEXT_NAME, result.getParentGroup().getName());

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
        Assert.assertEquals(IDENTIFIER, result.getGroupId());
        Assert.assertNull(result.getId());
        Assert.assertNull(result.getParentGroup());
        thenVersionedGroupIsNotSet(result);
        Assert.assertNull(result.getName());
        Assert.assertNull(result.getMatches());
    }

    private void givenProcessGroup() {
        Mockito.when(processGroup.getIdentifier()).thenReturn(IDENTIFIER);
        Mockito.when(processGroup.getName()).thenReturn(NAME);
        Mockito.when(processGroup.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(processGroup.getVersionControlInformation()).thenReturn(Mockito.mock(VersionControlInformation.class));
        Mockito.when(processGroup.getParent()).thenReturn(parentProcessGroup);

        Mockito.when(parentProcessGroup.getIdentifier()).thenReturn(PARENT_IDENTIFIER);
        Mockito.when(parentProcessGroup.getName()).thenReturn(PARENT_NAME);
        Mockito.when(parentProcessGroup.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(parentProcessGroup.getVersionControlInformation()).thenReturn(Mockito.mock(VersionControlInformation.class));

        Mockito.when(parameterContext.getIdentifier()).thenReturn(CONTEXT_IDENTIFIER);
        Mockito.when(parameterContext.getName()).thenReturn(CONTEXT_NAME);
    }

    private void givenRootProcessGroup() {
        Mockito.when(processGroup.getIdentifier()).thenReturn(IDENTIFIER);
        Mockito.when(processGroup.getParent()).thenReturn(null);
    }

    private void thenIdentifierIsNotSet(final ComponentSearchResultDTO result) {
        Assert.assertNull(result.getGroupId());
    }

    private void thenVersionedGroupIsNotSet(final ComponentSearchResultDTO result) {
        Assert.assertNull(result.getVersionedGroup());
    }
}