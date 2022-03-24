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

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.apache.nifi.web.api.dto.search.SearchResultGroupDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.search.query.RegexSearchQueryParser;
import org.apache.nifi.web.search.query.SearchQuery;
import org.apache.nifi.web.search.query.SearchQueryParser;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;

import static org.apache.nifi.web.controller.ComponentMockUtil.getRootProcessGroup;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:nifi-web-api-test-context.xml", "classpath:nifi-web-api-context.xml"})
public abstract class AbstractControllerSearchIntegrationTest {
    protected static final String ROOT_PROCESSOR_GROUP_ID = "3b9a7e60-0172-1000-5f1e-10cbc0c4d5f1";
    protected static final String ROOT_PROCESSOR_GROUP_NAME = "NiFi Flow";

    protected static final boolean AUTHORIZED = true;
    protected static final boolean NOT_AUTHORIZED = false;

    protected static final boolean UNDER_VERSION_CONTROL = true;
    protected static final boolean NOT_UNDER_VERSION_CONTROL = false;

    private NiFiUser user = Mockito.mock(NiFiUser.class);

    private final SearchQueryParser searchQueryParser = new RegexSearchQueryParser();

    protected SearchResultsDTO results;

    private Set<ProcessGroupSetup> processGroups;

    private Set<ParameterContext> parameterContexts;

    @Autowired
    private ControllerSearchService controllerSearchService;

    @Autowired
    private FlowController flowController;

    @Before
    public void setUp() {
        resetResults();
        processGroups = new HashSet<>();
        parameterContexts = new HashSet<>();

        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ParameterContextManager parameterContextManager = Mockito.mock(ParameterContextManager.class);

        Mockito.when(flowController.getFlowManager()).thenReturn(flowManager);
        Mockito.when(flowManager.getParameterContextManager()).thenReturn(parameterContextManager);
        Mockito.when(parameterContextManager.getParameterContexts()).thenReturn(parameterContexts);

        ExtensionManager extensionManager = Mockito.mock(ExtensionManager.class);
        Mockito.when(flowController.getExtensionManager()).thenReturn(extensionManager);
    }

    // given

    protected ProcessGroupSetup givenRootProcessGroup() {
        return givenProcessGroup(getRootProcessGroup(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, "", AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
    }

    protected ProcessGroupSetup givenRootProcessGroup(String comments) {
        return givenProcessGroup(getRootProcessGroup(ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, comments, AUTHORIZED, NOT_UNDER_VERSION_CONTROL));
    }

    protected ProcessGroupSetup givenProcessGroup(final ProcessGroup processGroup) {
        return new ProcessGroupSetup(processGroup);
    }

    protected void givenParameterContext(final ParameterContext parameterContext) {
        parameterContexts.add(parameterContext);
    }

    protected Map<ParameterDescriptor, Parameter> givenParameters(final Parameter... parameters) {
        final Map<ParameterDescriptor, Parameter> result = new HashMap<>();

        for (final Parameter parameter : parameters) {
            result.put(parameter.getDescriptor(), parameter);
        }

        return result;
    }

    // when

    protected void whenExecuteSearch(final String searchString, final String activeGroupId) {
        resetResults();
        final SearchQuery searchQuery = searchQueryParser.parse(searchString, user, getProcessGroup(ROOT_PROCESSOR_GROUP_ID), getProcessGroup(activeGroupId));
        controllerSearchService.search(searchQuery, results);
        controllerSearchService.searchParameters(searchQuery, results);
    }

    protected void whenExecuteSearch(final String searchString) {
        whenExecuteSearch(searchString, ROOT_PROCESSOR_GROUP_ID);
    }

    // then

    protected void thenResultIsEmpty() {
        thenResultConsists().validate(results);
    }

    protected SearchResultMatcher thenResultConsists() {
        return new SearchResultMatcher();
    }

    // Helpers

    void resetResults() {
        results = new SearchResultsDTO();
    }

    protected ProcessGroup getProcessGroup(final String processGroupId) {
        final ProcessGroupSetup result = getProcessGroupSetup(processGroupId);
        return (result == null) ? null : result.getProcessGroup();
    }

    protected ProcessGroupSetup getProcessGroupSetup(final String processGroupId) {
        for (final ProcessGroupSetup helper : processGroups) {
            if (processGroupId.equals(helper.getProcessGroup().getIdentifier())) {
                return helper;
            }
        }

        return null;
    }

    protected ComponentSearchResultDTO getSimpleResultFromRoot(
            final String id,
            final String name,
            final String... matches) {
        return getSimpleResult(id, name, ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_ID, ROOT_PROCESSOR_GROUP_NAME, matches);
    }

    protected ComponentSearchResultDTO getSimpleResult(
            final String id,
            final String name,
            final String groupId,
            final String parentGroupId,
            final String parentGroupName,
            final String... matches) {
        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();
        result.setId(id);
        result.setName(name);
        result.setGroupId(groupId);

        if (parentGroupId != null || parentGroupName != null) {
            final SearchResultGroupDTO parentGroup = new SearchResultGroupDTO();
            parentGroup.setId(parentGroupId);
            parentGroup.setName(parentGroupName);
            result.setParentGroup(parentGroup);
        }

        result.setMatches(Arrays.asList(matches));
        return result;
    }

    protected ComponentSearchResultDTO getVersionedResult(
            final String id,
            final String name,
            final String groupId,
            final String parentGroupId,
            final String parentGroupName,
            final String versionedGroupId,
            final String versionedGroupName,
            final String... matches) {
        final SearchResultGroupDTO versionedGroup = new SearchResultGroupDTO();
        versionedGroup.setId(versionedGroupId);
        versionedGroup.setName(versionedGroupName);

        final ComponentSearchResultDTO result = getSimpleResult(id, name, groupId, parentGroupId, parentGroupName, matches);

        result.setVersionedGroup(versionedGroup);
        return result;
    }

    protected class ProcessGroupSetup {
        private final Set<Port> outputPorts = new HashSet<>();
        private final Set<Port> inputPorts = new HashSet<>();
        private final Set<ProcessorNode> processors = new HashSet<>();
        private final Set<Label> labels = new HashSet<>();
        private final Set<RemoteProcessGroup> remoteProcessGroups = new HashSet<>();
        private final Set<Funnel> funnels = new HashSet<>();
        private final Set<Connection> connections = new HashSet<>();
        private final Set<ControllerServiceNode> controllerServiceNodes = new HashSet<>();
        private final Set<ProcessGroup> children = new HashSet<>();

        private final ProcessGroup processGroup;

        private ProcessGroupSetup(final ProcessGroup processGroup) {
            this.processGroup = processGroup;
            processGroups.add(this);

            if (processGroup.getParent() != null) {
                getProcessGroupSetup(processGroup.getParent().getIdentifier()).withChild(processGroup);
            }

            Mockito.when(processGroup.getInputPorts()).thenReturn(inputPorts);
            Mockito.when(processGroup.getOutputPorts()).thenReturn(outputPorts);
            Mockito.when(processGroup.getProcessors()).thenReturn(processors);
            Mockito.when(processGroup.getLabels()).thenReturn(labels);
            Mockito.when(processGroup.getRemoteProcessGroups()).thenReturn(remoteProcessGroups);
            Mockito.when(processGroup.getFunnels()).thenReturn(funnels);
            Mockito.when(processGroup.getConnections()).thenReturn(connections);
            Mockito.when(processGroup.getControllerServices(Mockito.anyBoolean())).thenReturn(controllerServiceNodes);
            Mockito.when(processGroup.getProcessGroups()).thenReturn(children);
        }

        public ProcessGroup getProcessGroup() {
            return processGroup;
        }

        public ProcessGroupSetup withOutputPort(final Port outputPort) {
            outputPorts.add(outputPort);
            return this;
        }

        public ProcessGroupSetup withInputPort(final Port inputPort) {
            inputPorts.add(inputPort);
            return this;
        }

        public ProcessGroupSetup withProcessor(final ProcessorNode processor) {
            processors.add(processor);
            return this;
        }

        public ProcessGroupSetup withLabel(final Label label) {
            labels.add(label);
            return this;
        }

        public ProcessGroupSetup withRemoteProcessGroup(final RemoteProcessGroup remoteProcessGroup) {
            remoteProcessGroups.add(remoteProcessGroup);
            return this;
        }

        public ProcessGroupSetup withFunnel(final Funnel funnel) {
            funnels.add(funnel);
            return this;
        }

        public ProcessGroupSetup withConnection(final Connection connection) {
            connections.add(connection);
            return this;
        }

        public ProcessGroupSetup withControllerServiceNode(final ControllerServiceNode controllerServiceNode) {
            controllerServiceNodes.add(controllerServiceNode);
            return this;
        }

        public ProcessGroupSetup withChild(final ProcessGroup child) {
            children.add(child);
            return this;
        }
    }

    static class ComponentSearchResultDTOWrapper {
        private final ComponentSearchResultDTO item;

        private final List<Function<ComponentSearchResultDTO, Object>> propertyProvider = Arrays.asList(
            ComponentSearchResultDTO::getId,
            ComponentSearchResultDTO::getName,
            ComponentSearchResultDTO::getGroupId,
            _item -> new HashSet<>(_item.getMatches()),
            _item -> _item.getMatches().size(),
            _item -> Optional.ofNullable(_item.getParentGroup()).map(SearchResultGroupDTO::getId).orElse("NO_PARENT_GROUP_ID"),
            _item -> Optional.ofNullable(_item.getParentGroup()).map(SearchResultGroupDTO::getName).orElse("NO_PARENT_GROUP_NAME"),
            _item -> Optional.ofNullable(_item.getVersionedGroup()).map(SearchResultGroupDTO::getId).orElse("NO_VERSIONED_GROUP_ID"),
            _item -> Optional.ofNullable(_item.getVersionedGroup()).map(SearchResultGroupDTO::getName).orElse("NO_VERSIONED_GROUP_NAME")
        );

        public ComponentSearchResultDTOWrapper(final ComponentSearchResultDTO item) {
            this.item = item;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final ComponentSearchResultDTOWrapper that = (ComponentSearchResultDTOWrapper) o;
            final EqualsBuilder equalsBuilder = new EqualsBuilder();

            propertyProvider.forEach(propertySupplier -> equalsBuilder.append(propertySupplier.apply(item), propertySupplier.apply(that.item)));

            return equalsBuilder.isEquals();
        }

        @Override
        public int hashCode() {
            final HashCodeBuilder hashCodeBuilder = new HashCodeBuilder(17, 37);

            propertyProvider.forEach(propertySupplier -> hashCodeBuilder.append(propertySupplier.apply(item)));

            return hashCodeBuilder.toHashCode();
        }

        @Override
        public String toString() {
            final StringJoiner stringJoiner = new StringJoiner(",\n\t", "{\n\t", "\n}");

            propertyProvider.forEach(propertySupplier -> stringJoiner.add(Optional.ofNullable(propertySupplier.apply(item)).orElse("N/A").toString()));

            return stringJoiner.toString();
        }
    }
}
