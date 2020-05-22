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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.search.ComponentMatcher;
import org.apache.nifi.web.search.query.SearchQuery;
import org.apache.nifi.web.search.resultenrichment.ComponentSearchResultEnricher;
import org.apache.nifi.web.search.resultenrichment.ComponentSearchResultEnricherFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@RunWith(MockitoJUnitRunner.class)
public class ControllerSearchServiceTest  {

    public static final String PROCESS_GROUP_SECOND_LEVEL_A = "secondLevelA";
    public static final String PROCESS_GROUP_SECOND_LEVEL_B_1 = "secondLevelB1";
    public static final String PROCESS_GROUP_SECOND_LEVEL_B_2 = "secondLevelB2";
    public static final String PROCESS_GROUP_FIRST_LEVEL_A = "firstLevelA";
    public static final String PROCESS_GROUP_FIRST_LEVEL_B = "firstLevelB";
    public static final String PROCESS_GROUP_ROOT = "root";

    @Mock
    private SearchQuery searchQuery;

    @Mock
    private NiFiUser user;

    @Mock
    private Authorizer authorizer;

    @Mock
    private ComponentSearchResultEnricherFactory resultEnricherFactory;

    @Mock
    private ComponentSearchResultEnricher resultEnricher;

    @Mock
    private FlowController flowController;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ParameterContextManager parameterContextManager;

    @Mock
    private ComponentMatcher<ProcessorNode> matcherForProcessor;

    @Mock
    private ComponentMatcher<ProcessGroup> matcherForProcessGroup;

    @Mock
    private ComponentMatcher<Connection> matcherForConnection;

    @Mock
    private ComponentMatcher<RemoteProcessGroup> matcherForRemoteProcessGroup;

    @Mock
    private ComponentMatcher<Port> matcherForPort;

    @Mock
    private ComponentMatcher<Funnel> matcherForFunnel;

    @Mock
    private ComponentMatcher<ParameterContext> matcherForParameterContext;

    @Mock
    private ComponentMatcher<Parameter> matcherForParameter;

    @Mock
    private ComponentMatcher<Label> matcherForLabel;

    private HashMap<String, ProcessGroup> processGroups;

    private ControllerSearchService testSubject;

    private SearchResultsDTO results;

    @Before
    public void setUp() {
        Mockito.when(resultEnricherFactory.getComponentResultEnricher(Mockito.any(ProcessGroup.class), Mockito.any(NiFiUser.class))).thenReturn(resultEnricher);
        Mockito.when(resultEnricherFactory.getProcessGroupResultEnricher(Mockito.any(ProcessGroup.class), Mockito.any(NiFiUser.class))).thenReturn(resultEnricher);
        Mockito.when(resultEnricherFactory.getParameterResultEnricher(Mockito.any(ParameterContext.class))).thenReturn(resultEnricher);
        Mockito.when(resultEnricher.enrich(Mockito.any(ComponentSearchResultDTO.class))).thenAnswer(invocationOnMock -> invocationOnMock.getArgument(0));

        Mockito.when(matcherForProcessor.match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForProcessGroup.match(Mockito.any(ProcessGroup.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForConnection.match(Mockito.any(Connection.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForRemoteProcessGroup.match(Mockito.any(RemoteProcessGroup.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForPort.match(Mockito.any(Port.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForFunnel.match(Mockito.any(Funnel.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForParameterContext.match(Mockito.any(ParameterContext.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForParameter.match(Mockito.any(Parameter.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));
        Mockito.when(matcherForLabel.match(Mockito.any(Label.class), Mockito.any(SearchQuery.class))).thenReturn(Optional.of(new ComponentSearchResultDTO()));

        results = new SearchResultsDTO();
        testSubject = givenTestSubject();
        processGroups = new HashMap<>();
    }

    @Test
    public void testSearchChecksEveryComponentType() {
        // given
        givenSingleProcessGroupIsSetUp();
        givenSearchQueryIsSetUp();
        givenNoFilters();

        // when
        testSubject.search(searchQuery, results);

        // then
        thenAllComponentTypeIsChecked();
        thenAllComponentResultsAreCollected();
    }

    @Test
    public void testSearchChecksChildrenGroupsToo() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenNoFilters();

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAndTheirContentsAreSearched(processGroups.keySet());
    }

    @Test
    public void testSearchWhenGroupIsNotAuthorized() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenNoFilters();
        givenProcessGroupIsNotAutorized(PROCESS_GROUP_FIRST_LEVEL_B);

        // when
        testSubject.search(searchQuery, results);
        // The authorization is not transitive, children groups might be good candidates.
        thenFollowingGroupsAreSearched(Arrays.asList(
                PROCESS_GROUP_ROOT,
                PROCESS_GROUP_FIRST_LEVEL_A,
                PROCESS_GROUP_SECOND_LEVEL_A,
                PROCESS_GROUP_SECOND_LEVEL_B_1,
                PROCESS_GROUP_SECOND_LEVEL_B_2));
        thenContentOfTheFollowingGroupsAreSearched(Arrays.asList(
                PROCESS_GROUP_ROOT,
                PROCESS_GROUP_FIRST_LEVEL_A,
                PROCESS_GROUP_SECOND_LEVEL_A,
                PROCESS_GROUP_FIRST_LEVEL_B,
                PROCESS_GROUP_SECOND_LEVEL_B_1,
                PROCESS_GROUP_SECOND_LEVEL_B_2));
    }

    @Test
    public void testSearchWhenProcessNodeIsNotAuthorized() {
        // given
        givenSingleProcessGroupIsSetUp();
        givenSearchQueryIsSetUp();
        givenProcessorIsNotAuthorized();
        givenNoFilters();

        // when
        testSubject.search(searchQuery, results);

        // then
        thenProcessorMatcherIsNotCalled();
    }

    @Test
    public void testSearchWithHereFilterShowsActualGroupAndSubgroupsOnly() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp(processGroups.get(PROCESS_GROUP_FIRST_LEVEL_A));
        givenScopeFilterIsSet();

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList(
                PROCESS_GROUP_FIRST_LEVEL_A,
                PROCESS_GROUP_SECOND_LEVEL_A));
    }

    @Test
    public void testSearchWithHereFilterAndInRoot() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenScopeFilterIsSet();

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAndTheirContentsAreSearched(processGroups.keySet());
    }


    @Test
    public void testSearchWithGroupFilterShowsPointedGroupAndSubgroupsOnly() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenGroupFilterIsSet(PROCESS_GROUP_FIRST_LEVEL_B + "Name");

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList( //
                PROCESS_GROUP_FIRST_LEVEL_B, //
                PROCESS_GROUP_SECOND_LEVEL_B_1, //
                PROCESS_GROUP_SECOND_LEVEL_B_2));
    }

    @Test
    public void testSearchGroupWithLowerCase() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenGroupFilterIsSet((PROCESS_GROUP_FIRST_LEVEL_B + "Name").toLowerCase());

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList( //
                PROCESS_GROUP_FIRST_LEVEL_B, //
                PROCESS_GROUP_SECOND_LEVEL_B_1, //
                PROCESS_GROUP_SECOND_LEVEL_B_2));
    }

    @Test
    public void testSearchGroupWithPartialMatch() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenGroupFilterIsSet((PROCESS_GROUP_FIRST_LEVEL_B + "Na"));

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList( //
                PROCESS_GROUP_FIRST_LEVEL_B, //
                PROCESS_GROUP_SECOND_LEVEL_B_1, //
                PROCESS_GROUP_SECOND_LEVEL_B_2));
    }

    @Test
    public void testSearchGroupBasedOnIdentifier() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenGroupFilterIsSet((PROCESS_GROUP_FIRST_LEVEL_B + "Id"));

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList( //
                PROCESS_GROUP_FIRST_LEVEL_B, //
                PROCESS_GROUP_SECOND_LEVEL_B_1, //
                PROCESS_GROUP_SECOND_LEVEL_B_2));
    }

    @Test
    public void testSearchWithGroupWhenRoot() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenGroupFilterIsSet(PROCESS_GROUP_ROOT + "Name");

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAndTheirContentsAreSearched(processGroups.keySet());
    }

    @Test
    public void testSearchWithGroupWhenValueIsNonExisting() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp();
        givenGroupFilterIsSet("Unknown");

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList());
    }

    @Test
    public void testWhenBothFiltersPresentAndScopeIsMoreRestricting() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp(processGroups.get(PROCESS_GROUP_SECOND_LEVEL_B_1));
        givenScopeFilterIsSet();
        givenGroupFilterIsSet(PROCESS_GROUP_FIRST_LEVEL_B + "Name");

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList(PROCESS_GROUP_SECOND_LEVEL_B_1));
    }

    @Test
    public void testWhenBothFiltersPresentAndGroupIsMoreRestricting() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp(processGroups.get(PROCESS_GROUP_FIRST_LEVEL_B));
        givenScopeFilterIsSet();
        givenGroupFilterIsSet(PROCESS_GROUP_SECOND_LEVEL_B_1 + "Name");

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList(PROCESS_GROUP_SECOND_LEVEL_B_1));
    }

    @Test
    public void testWhenBothFiltersPresentTheyAreNotOverlapping() {
        // given
        givenProcessGroupsAreSetUp();
        givenSearchQueryIsSetUp(processGroups.get(PROCESS_GROUP_FIRST_LEVEL_B));
        givenScopeFilterIsSet();
        givenGroupFilterIsSet(PROCESS_GROUP_FIRST_LEVEL_A + "Name");

        // when
        testSubject.search(searchQuery, results);

        // then
        thenFollowingGroupsAreSearched(Arrays.asList());
    }

    @Test
    public void testSearchParameterContext() {
        // given
        givenSingleProcessGroupIsSetUp();
        givenSearchQueryIsSetUp();
        givenParameterSearchIsSetUp(true);

        // when
        testSubject.searchParameters(searchQuery, results);

        // then
        thenParameterComponentTypesAreChecked();
        thenAllParameterComponentResultsAreCollected();
    }

    @Test
    public void testSearchParameterContextWhenNotAuthorized() {
        // given
        givenSingleProcessGroupIsSetUp();
        givenSearchQueryIsSetUp();
        givenParameterSearchIsSetUp(false);

        // when
        testSubject.searchParameters(searchQuery, results);

        // then
        thenParameterSpecificComponentTypesAreNotChecked();
    }

    private ControllerSearchService givenTestSubject() {
        final ControllerSearchService result = new ControllerSearchService();
        result.setAuthorizer(authorizer);
        result.setFlowController(flowController);
        result.setMatcherForProcessor(matcherForProcessor);
        result.setMatcherForProcessGroup(matcherForProcessGroup);
        result.setMatcherForConnection(matcherForConnection);
        result.setMatcherForRemoteProcessGroup(matcherForRemoteProcessGroup);
        result.setMatcherForPort(matcherForPort);
        result.setMatcherForFunnel(matcherForFunnel);
        result.setMatcherForParameterContext(matcherForParameterContext);
        result.setMatcherForParameter(matcherForParameter);
        result.setMatcherForLabel(matcherForLabel);
        result.setResultEnricherFactory(resultEnricherFactory);
        return result;
    }

    private void givenSingleProcessGroupIsSetUp() {
        final ProcessGroup root = givenProcessGroup(PROCESS_GROUP_ROOT, true, Collections.emptySet(), Collections.emptySet());

        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
        Mockito.when(processorNode.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(root.getProcessors()).thenReturn(Collections.singletonList(processorNode));

        final Connection connection = Mockito.mock(Connection.class);
        Mockito.when(connection.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(root.getConnections()).thenReturn(new HashSet<>(Arrays.asList(connection)));

        final RemoteProcessGroup remoteProcessGroup = Mockito.mock(RemoteProcessGroup.class);
        Mockito.when(remoteProcessGroup.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(root.getRemoteProcessGroups()).thenReturn(new HashSet<>(Arrays.asList(remoteProcessGroup)));

        final Port port = Mockito.mock(Port.class);
        Mockito.when(port.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(root.getInputPorts()).thenReturn(new HashSet<>(Arrays.asList(port)));
        Mockito.when(root.getOutputPorts()).thenReturn(new HashSet<>(Arrays.asList(port)));

        final Funnel funnel = Mockito.mock(Funnel.class);
        Mockito.when(funnel.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(root.getFunnels()).thenReturn(new HashSet<>(Arrays.asList(funnel)));

        final Label label = Mockito.mock(Label.class);
        Mockito.when(label.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(true);
        Mockito.when(root.getLabels()).thenReturn(new HashSet<>(Arrays.asList(label)));
    }

    private void givenProcessGroupsAreSetUp() {
        final ProcessGroup secondLevelAProcessGroup = givenProcessGroup(PROCESS_GROUP_SECOND_LEVEL_A, true, Collections.emptySet(), Collections.emptySet());
        final ProcessGroup secondLevelB1ProcessGroup = givenProcessGroup(PROCESS_GROUP_SECOND_LEVEL_B_1, true, Collections.emptySet(), Collections.emptySet());
        final ProcessGroup secondLevelB2ProcessGroup = givenProcessGroup(PROCESS_GROUP_SECOND_LEVEL_B_2, true, Collections.emptySet(), Collections.emptySet());

        final ProcessGroup firstLevelAProcessGroup = givenProcessGroup(PROCESS_GROUP_FIRST_LEVEL_A, //
                true, Collections.emptySet(), Collections.singleton(secondLevelAProcessGroup));
        final ProcessGroup firstLevelBProcessGroup = givenProcessGroup(PROCESS_GROUP_FIRST_LEVEL_B, //
                true, Collections.emptySet(), new HashSet<>(Arrays.asList(secondLevelB1ProcessGroup, secondLevelB2ProcessGroup)));

        final ProcessGroup root =  givenProcessGroup(PROCESS_GROUP_ROOT, //
                true, Collections.emptySet(), new HashSet<>(Arrays.asList(firstLevelAProcessGroup, firstLevelBProcessGroup)));
    }

    private void givenSearchQueryIsSetUp() {
        givenSearchQueryIsSetUp(processGroups.get(PROCESS_GROUP_ROOT));
    }

    private void givenSearchQueryIsSetUp(final ProcessGroup activeProcessGroup) {
        Mockito.when(searchQuery.getUser()).thenReturn(user);
        Mockito.when(searchQuery.getRootGroup()).thenReturn(processGroups.get(PROCESS_GROUP_ROOT));
        Mockito.when(searchQuery.getActiveGroup()).thenReturn(activeProcessGroup);
    }

    private ProcessGroup givenProcessGroup( //
            final String identifier, //
            final boolean isAuthorized, //
            final Set<ProcessorNode> processors, //
            final Set<ProcessGroup> children) {
        final ProcessGroup result = Mockito.mock(ProcessGroup.class);
        final Funnel funnel = Mockito.mock(Funnel.class); // This is for testing if group content was searched
        Mockito.when(funnel.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(isAuthorized);

        Mockito.when(result.getName()).thenReturn(identifier + "Name");
        Mockito.when(result.getIdentifier()).thenReturn(identifier + "Id");
        Mockito.when(result.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(isAuthorized);

        Mockito.when(result.getProcessGroups()).thenReturn(children);
        Mockito.when(result.getProcessors()).thenReturn(processors);
        Mockito.when(result.getConnections()).thenReturn(Collections.emptySet());
        Mockito.when(result.getRemoteProcessGroups()).thenReturn(Collections.emptySet());
        Mockito.when(result.getInputPorts()).thenReturn(Collections.emptySet());
        Mockito.when(result.getOutputPorts()).thenReturn(Collections.emptySet());
        Mockito.when(result.getFunnels()).thenReturn(Collections.singleton(funnel));
        Mockito.when(result.getLabels()).thenReturn(Collections.emptySet());

        children.forEach(child -> Mockito.when(child.getParent()).thenReturn(result));
        processGroups.put(identifier, result);

        return result;
    }

    private void givenProcessGroupIsNotAutorized(final String processGroupName) {
        Mockito.when(processGroups.get(processGroupName).isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(false);
    }

    private void givenNoFilters() {
        Mockito.when(searchQuery.hasFilter(Mockito.anyString())).thenReturn(false);
    }

    private void givenScopeFilterIsSet() {
        Mockito.when(searchQuery.hasFilter("scope")).thenReturn(true);
        Mockito.when(searchQuery.getFilter("scope")).thenReturn("here");
    }

    private void givenGroupFilterIsSet(final String group) {
        Mockito.when(searchQuery.hasFilter("group")).thenReturn(true);
        Mockito.when(searchQuery.getFilter("group")).thenReturn(group);
    }

    private void givenProcessorIsNotAuthorized() {
        final ProcessorNode processor = processGroups.get(PROCESS_GROUP_ROOT).getProcessors().iterator().next();
        Mockito.when(processor.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(false);
    }

    private void givenParameterSearchIsSetUp(boolean isAuthorized) {
        final ParameterContext parameterContext = Mockito.mock(ParameterContext.class);
        final Parameter parameter = Mockito.mock(Parameter.class);
        final ParameterDescriptor descriptor = Mockito.mock(ParameterDescriptor.class);
        final Map<ParameterDescriptor, Parameter> parameters = new HashMap<>();
        parameters.put(descriptor, parameter);
        Mockito.when(flowController.getFlowManager()).thenReturn(flowManager);
        Mockito.when(flowManager.getParameterContextManager()).thenReturn(parameterContextManager);
        Mockito.when(parameterContextManager.getParameterContexts()).thenReturn(new HashSet<>(Arrays.asList(parameterContext)));
        Mockito.when(parameterContext.getParameters()).thenReturn(parameters);
        Mockito.when(parameterContext.isAuthorized(authorizer, RequestAction.READ, user)).thenReturn(isAuthorized);
    }

    private void thenProcessorMatcherIsNotCalled() {
        final ProcessorNode processor = processGroups.get(PROCESS_GROUP_ROOT).getProcessors().iterator().next();
        Mockito.verify(matcherForProcessor, Mockito.never()).match(processor, searchQuery);
    }

    private void thenAllComponentTypeIsChecked() {
        Mockito.verify(matcherForProcessor, Mockito.times(1)).match(Mockito.any(ProcessorNode.class), Mockito.any(SearchQuery.class));
        Mockito.verify(matcherForConnection, Mockito.times(1)).match(Mockito.any(Connection.class), Mockito.any(SearchQuery.class));
        Mockito.verify(matcherForRemoteProcessGroup, Mockito.times(1)).match(Mockito.any(RemoteProcessGroup.class), Mockito.any(SearchQuery.class));
        // Port needs to be used multiple times as input and output ports are handled separately
        Mockito.verify(matcherForPort, Mockito.times(2)).match(Mockito.any(Port.class), Mockito.any(SearchQuery.class));
        Mockito.verify(matcherForFunnel, Mockito.times(1)).match(Mockito.any(Funnel.class), Mockito.any(SearchQuery.class));
        Mockito.verify(matcherForLabel, Mockito.times(1)).match(Mockito.any(Label.class), Mockito.any(SearchQuery.class));
    }

    private void thenAllComponentResultsAreCollected() {
        Assert.assertEquals(1, results.getProcessorResults().size());
        Assert.assertEquals(1, results.getConnectionResults().size());
        Assert.assertEquals(1, results.getRemoteProcessGroupResults().size());
        Assert.assertEquals(1, results.getInputPortResults().size());
        Assert.assertEquals(1, results.getOutputPortResults().size());
        Assert.assertEquals(1, results.getFunnelResults().size());
        Assert.assertEquals(1, results.getLabelResults().size());
        Assert.assertTrue(results.getParameterContextResults().isEmpty());
        Assert.assertTrue(results.getParameterResults().isEmpty());
    }

    private void thenParameterComponentTypesAreChecked() {
        Mockito.verify(matcherForParameterContext, Mockito.times(1)).match(Mockito.any(ParameterContext.class), Mockito.any(SearchQuery.class));
        Mockito.verify(matcherForParameter, Mockito.times(1)).match(Mockito.any(Parameter.class), Mockito.any(SearchQuery.class));
    }

    private void thenAllParameterComponentResultsAreCollected() {
        Assert.assertTrue(results.getProcessGroupResults().isEmpty());
        Assert.assertTrue(results.getProcessorResults().isEmpty());
        Assert.assertTrue(results.getConnectionResults().isEmpty());
        Assert.assertTrue(results.getRemoteProcessGroupResults().isEmpty());
        Assert.assertTrue(results.getInputPortResults().isEmpty());
        Assert.assertTrue(results.getOutputPortResults().isEmpty());
        Assert.assertTrue(results.getFunnelResults().isEmpty());
        Assert.assertTrue(results.getLabelResults().isEmpty());
        Assert.assertEquals(1, results.getParameterContextResults().size());
        Assert.assertEquals(1, results.getParameterResults().size());
    }

    private void thenParameterSpecificComponentTypesAreNotChecked() {
        Mockito.verify(matcherForParameterContext, Mockito.never()).match(Mockito.any(ParameterContext.class), Mockito.any(SearchQuery.class));
        Mockito.verify(matcherForParameter, Mockito.never()).match(Mockito.any(Parameter.class), Mockito.any(SearchQuery.class));
    }

    private void thenFollowingGroupsAndTheirContentsAreSearched(final Collection<String> searchedProcessGroups) {
        thenFollowingGroupsAreSearched(searchedProcessGroups);
        thenContentOfTheFollowingGroupsAreSearched(searchedProcessGroups);
    }

    private void thenFollowingGroupsAreSearched(final Collection<String> searchedProcessGroups) {
        for (final String processGroup : searchedProcessGroups) {
            Mockito.verify(matcherForProcessGroup, Mockito.times(1)).match(processGroups.get(processGroup), searchQuery);
        }

        Mockito.verifyNoMoreInteractions(matcherForProcessGroup);
    }

    private void thenContentOfTheFollowingGroupsAreSearched(final Collection<String> searchedProcessGroupIds) {
        for (final String processGroupId : searchedProcessGroupIds) {
            // Checking on funnels is arbitrary, any given component we expect to be searched would be a good candidate
            final ProcessGroup processGroup = processGroups.get(processGroupId);
            final Funnel funnel = processGroup.getFunnels().iterator().next();
            Mockito.verify(matcherForFunnel, Mockito.times(1)).match(funnel, searchQuery);
        }

        Mockito.verifyNoMoreInteractions(matcherForFunnel);
    }
}