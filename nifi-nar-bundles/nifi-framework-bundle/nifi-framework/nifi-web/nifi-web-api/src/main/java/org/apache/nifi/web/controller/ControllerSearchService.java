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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;
import org.apache.nifi.web.search.ComponentMatcher;
import org.apache.nifi.web.search.query.SearchQuery;
import org.apache.nifi.web.search.resultenrichment.ComponentSearchResultEnricher;
import org.apache.nifi.web.search.resultenrichment.ComponentSearchResultEnricherFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * NiFi web controller's helper service that implements component search.
 */
public class ControllerSearchService {
    private final static String FILTER_NAME_GROUP = "group";
    private final static String FILTER_NAME_SCOPE = "scope";
    private final static String FILTER_SCOPE_VALUE_HERE = "here";

    private FlowController flowController;
    private Authorizer authorizer;
    private ComponentSearchResultEnricherFactory resultEnricherFactory;

    private ComponentMatcher<ProcessorNode> matcherForProcessor;
    private ComponentMatcher<ProcessGroup> matcherForProcessGroup;
    private ComponentMatcher<Connection> matcherForConnection;
    private ComponentMatcher<RemoteProcessGroup> matcherForRemoteProcessGroup;
    private ComponentMatcher<Port> matcherForPort;
    private ComponentMatcher<Funnel> matcherForFunnel;
    private ComponentMatcher<ParameterContext> matcherForParameterContext;
    private ComponentMatcher<Parameter> matcherForParameter;
    private ComponentMatcher<Label> matcherForLabel;
    private ComponentMatcher<ControllerServiceNode> matcherForControllerServiceNode;

    /**
     * Searches all parameter contexts and parameters.
     *
     * @param searchQuery Details of the search
     * @param results Search results
     */
    public void search(final SearchQuery searchQuery, final SearchResultsDTO results) {
        if (searchQuery.hasFilter(FILTER_NAME_SCOPE) && FILTER_SCOPE_VALUE_HERE.equals(searchQuery.getFilter(FILTER_NAME_SCOPE))) {
            searchInProcessGroup(results, searchQuery, searchQuery.getActiveGroup());
        } else {
            searchInProcessGroup(results, searchQuery, searchQuery.getRootGroup());
        }
    }

    private void searchInProcessGroup(final SearchResultsDTO results, final SearchQuery searchQuery, final ProcessGroup scope) {
        final NiFiUser user = searchQuery.getUser();
        final ComponentSearchResultEnricher resultEnricher = resultEnricherFactory.getComponentResultEnricher(scope, user);
        final ComponentSearchResultEnricher groupResultEnricher = resultEnricherFactory.getProcessGroupResultEnricher(scope, user);

        if (appliesToGroupFilter(searchQuery, scope)) {

            searchComponentType(Collections.singletonList(scope), user, searchQuery, matcherForProcessGroup, groupResultEnricher, results.getProcessGroupResults());
            searchComponentType(scope.getProcessors(), user, searchQuery, matcherForProcessor, resultEnricher, results.getProcessorResults());
            searchComponentType(scope.getConnections(), user, searchQuery, matcherForConnection, resultEnricher, results.getConnectionResults());
            searchComponentType(scope.getRemoteProcessGroups(), user, searchQuery, matcherForRemoteProcessGroup, resultEnricher, results.getRemoteProcessGroupResults());
            searchComponentType(scope.getInputPorts(), user, searchQuery, matcherForPort, resultEnricher, results.getInputPortResults());
            searchComponentType(scope.getOutputPorts(), user, searchQuery, matcherForPort, resultEnricher, results.getOutputPortResults());
            searchComponentType(scope.getFunnels(), user, searchQuery, matcherForFunnel, resultEnricher, results.getFunnelResults());
            searchComponentType(scope.getLabels(), user, searchQuery, matcherForLabel, resultEnricher, results.getLabelResults());
            searchComponentType(scope.getControllerServices(false), user, searchQuery, matcherForControllerServiceNode, resultEnricher, results.getControllerServiceNodeResults());
        }

        scope.getProcessGroups().forEach(processGroup -> searchInProcessGroup(results, searchQuery, processGroup));
    }

    private boolean appliesToGroupFilter(final SearchQuery searchQuery, final ProcessGroup scope) {
        return !searchQuery.hasFilter(FILTER_NAME_GROUP) || eligibleForGroupFilter(scope, searchQuery.getFilter(FILTER_NAME_GROUP));
    }

    /**
     * Check is the group is eligible for the filter value. It might be eligible based on name or id.
     *
     * @param scope The subject process group.
     * @param filterValue The value to match against.
     *
     * @return True in case the scope process group or any parent is matching. A group is matching when it's name or it's id contains the filter value.
     */
    private boolean eligibleForGroupFilter(final ProcessGroup scope, final String filterValue) {
        final List<ProcessGroup> lineage = getLineage(scope);

        for (final ProcessGroup group : lineage) {
            if (StringUtils.containsIgnoreCase(group.getName(), filterValue) || StringUtils.containsIgnoreCase(group.getIdentifier(), filterValue)) {
                return true;
            }
        }

        return false;
    }

    private List<ProcessGroup> getLineage(final ProcessGroup group) {
        final LinkedList<ProcessGroup> result = new LinkedList<>();
        ProcessGroup current = group;

        while (current != null) {
            result.addLast(current);
            current = current.getParent();
        }

        return result;
    }

    private <T extends Authorizable> void searchComponentType(
               final Collection<T> components,
               final NiFiUser user,
               final SearchQuery searchQuery,
               final ComponentMatcher<T> matcher,
               final ComponentSearchResultEnricher resultEnricher,
               final List<ComponentSearchResultDTO> resultAccumulator) {
        components.stream()
                .filter(component -> component.isAuthorized(authorizer, RequestAction.READ, user))
                .map(component -> matcher.match(component, searchQuery))
                .filter(Optional::isPresent)
                .map(result -> resultEnricher.enrich(result.get()))
                .forEach(result -> resultAccumulator.add(result));
    }

    /**
     * Searches all parameter contexts and parameters.
     *
     * @param searchQuery Details of the search
     * @param results Search results
     */
    public void searchParameters(final SearchQuery searchQuery, final SearchResultsDTO results) {
        flowController.getFlowManager()
                .getParameterContextManager()
                .getParameterContexts()
                .stream()
                .filter(component -> component.isAuthorized(authorizer, RequestAction.READ, searchQuery.getUser()))
                .forEach(parameterContext -> {
                    final ComponentSearchResultEnricher resultEnricher = resultEnricherFactory.getParameterResultEnricher(parameterContext);

                    matcherForParameterContext.match(parameterContext, searchQuery)
                            .ifPresent(match -> results.getParameterContextResults().add(match));

                    parameterContext.getParameters().values().stream()
                            .map(component -> matcherForParameter.match(component, searchQuery))
                            .filter(Optional::isPresent)
                            .map(result -> resultEnricher.enrich(result.get()))
                            .forEach(result -> results.getParameterResults().add(result));
                });
    }

    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setResultEnricherFactory(ComponentSearchResultEnricherFactory resultEnricherFactory) {
        this.resultEnricherFactory = resultEnricherFactory;
    }

    public void setMatcherForProcessor(ComponentMatcher<ProcessorNode> matcherForProcessor) {
        this.matcherForProcessor = matcherForProcessor;
    }

    public void setMatcherForProcessGroup(ComponentMatcher<ProcessGroup> matcherForProcessGroup) {
        this.matcherForProcessGroup = matcherForProcessGroup;
    }

    public void setMatcherForConnection(ComponentMatcher<Connection> matcherForConnection) {
        this.matcherForConnection = matcherForConnection;
    }

    public void setMatcherForRemoteProcessGroup(ComponentMatcher<RemoteProcessGroup> matcherForRemoteProcessGroup) {
        this.matcherForRemoteProcessGroup = matcherForRemoteProcessGroup;
    }

    public void setMatcherForPort(ComponentMatcher<Port> matcherForPort) {
        this.matcherForPort = matcherForPort;
    }

    public void setMatcherForFunnel(ComponentMatcher<Funnel> matcherForFunnel) {
        this.matcherForFunnel = matcherForFunnel;
    }

    public void setMatcherForParameterContext(ComponentMatcher<ParameterContext> matcherForParameterContext) {
        this.matcherForParameterContext = matcherForParameterContext;
    }

    public void setMatcherForParameter(ComponentMatcher<Parameter> matcherForParameter) {
        this.matcherForParameter = matcherForParameter;
    }

    public void setMatcherForLabel(ComponentMatcher<Label> matcherForLabel) {
        this.matcherForLabel = matcherForLabel;
    }

    public void setMatcherForControllerServiceNode(ComponentMatcher<ControllerServiceNode> matcherForControllerServiceNode) {
        this.matcherForControllerServiceNode = matcherForControllerServiceNode;
    }
}
