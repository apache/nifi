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
package org.apache.nifi.web.configuration;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.web.controller.ControllerSearchService;
import org.apache.nifi.web.search.ComponentMatcherFactory;
import org.apache.nifi.web.search.attributematchers.BackPressureMatcher;
import org.apache.nifi.web.search.attributematchers.BasicMatcher;
import org.apache.nifi.web.search.attributematchers.BundleMatcher;
import org.apache.nifi.web.search.attributematchers.ConnectionMatcher;
import org.apache.nifi.web.search.attributematchers.ConnectionRelationshipMatcher;
import org.apache.nifi.web.search.attributematchers.ConnectivityMatcher;
import org.apache.nifi.web.search.attributematchers.ControllerServiceNodeMatcher;
import org.apache.nifi.web.search.attributematchers.ExecutionMatcher;
import org.apache.nifi.web.search.attributematchers.ExpirationMatcher;
import org.apache.nifi.web.search.attributematchers.ExtendedMatcher;
import org.apache.nifi.web.search.attributematchers.LabelMatcher;
import org.apache.nifi.web.search.attributematchers.ParameterContextMatcher;
import org.apache.nifi.web.search.attributematchers.ParameterMatcher;
import org.apache.nifi.web.search.attributematchers.ParameterProviderNodeMatcher;
import org.apache.nifi.web.search.attributematchers.PortScheduledStateMatcher;
import org.apache.nifi.web.search.attributematchers.PrioritiesMatcher;
import org.apache.nifi.web.search.attributematchers.ProcessGroupMatcher;
import org.apache.nifi.web.search.attributematchers.ProcessorMetadataMatcher;
import org.apache.nifi.web.search.attributematchers.PropertyMatcher;
import org.apache.nifi.web.search.attributematchers.RelationshipMatcher;
import org.apache.nifi.web.search.attributematchers.RemoteProcessGroupMatcher;
import org.apache.nifi.web.search.attributematchers.ScheduledStateMatcher;
import org.apache.nifi.web.search.attributematchers.SchedulingMatcher;
import org.apache.nifi.web.search.attributematchers.SearchableMatcher;
import org.apache.nifi.web.search.attributematchers.TargetUriMatcher;
import org.apache.nifi.web.search.attributematchers.TransmissionStatusMatcher;
import org.apache.nifi.web.search.resultenrichment.ComponentSearchResultEnricherFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class WebSearchConfiguration {

    private final Authorizer authorizer;

    private final FlowController flowController;

    public WebSearchConfiguration(
            final Authorizer authorizer,
            final FlowController flowController
    ) {
        this.authorizer = authorizer;
        this.flowController = flowController;
    }

    @Bean
    public ComponentSearchResultEnricherFactory resultEnricherFactory() {
        final ComponentSearchResultEnricherFactory factory = new ComponentSearchResultEnricherFactory();
        factory.setAuthorizer(authorizer);
        return factory;
    }

    @Bean
    public ControllerSearchService controllerSearchService() {
        final ControllerSearchService controllerSearchService = new ControllerSearchService();

        controllerSearchService.setAuthorizer(authorizer);
        controllerSearchService.setFlowController(flowController);
        controllerSearchService.setResultEnricherFactory(resultEnricherFactory());
        final ComponentMatcherFactory factory = new ComponentMatcherFactory();

        controllerSearchService.setMatcherForConnection(factory.getInstanceForConnection(
                List.of(
                        new ConnectionMatcher(),
                        new ConnectionRelationshipMatcher(),
                        new PrioritiesMatcher(),
                        new ExpirationMatcher(),
                        new BackPressureMatcher(),
                        new ConnectivityMatcher()
                )
        ));
        controllerSearchService.setMatcherForControllerServiceNode(factory.getInstanceForControllerServiceNode(
                List.of(
                        new ControllerServiceNodeMatcher(),
                        new BundleMatcher<>(),
                        new PropertyMatcher<>()
                )
        ));
        controllerSearchService.setMatcherForFunnel(factory.getInstanceForConnectable(
                List.of(
                        new BasicMatcher<>()
                )
        ));
        controllerSearchService.setMatcherForProcessGroup(factory.getInstanceForProcessGroup(
                List.of(
                        new ProcessGroupMatcher()
                )
        ));
        controllerSearchService.setMatcherForRemoteProcessGroup(factory.getInstanceForRemoteProcessGroup(
                List.of(
                        new RemoteProcessGroupMatcher(),
                        new TargetUriMatcher(),
                        new TransmissionStatusMatcher()
                )
        ));
        controllerSearchService.setMatcherForLabel(factory.getInstanceForLabel(
                List.of(
                        new LabelMatcher()
                )
        ));
        controllerSearchService.setMatcherForParameter(factory.getInstanceForParameter(
                List.of(
                        new ParameterMatcher())
                )
        );
        controllerSearchService.setMatcherForParameterContext(factory.getInstanceForParameterContext(
                List.of(
                        new ParameterContextMatcher()
                )
        ));
        controllerSearchService.setMatcherForParameterProviderNode(factory.getInstanceForParameterProviderNode(
                List.of(
                        new ParameterProviderNodeMatcher(),
                        new PropertyMatcher<>()
                )
        ));
        controllerSearchService.setMatcherForPort(factory.getInstanceForConnectable(
                List.of(
                        new ExtendedMatcher<>(),
                        new PortScheduledStateMatcher()
                )
        ));

        final SearchableMatcher searchableMatcher = new SearchableMatcher();
        searchableMatcher.setFlowController(flowController);
        controllerSearchService.setMatcherForProcessor(factory.getInstanceForConnectable(
                List.of(
                        new ExtendedMatcher<>(),
                        new SchedulingMatcher(),
                        new ExecutionMatcher(),
                        new ScheduledStateMatcher(),
                        new RelationshipMatcher<>(),
                        new ProcessorMetadataMatcher(),
                        new BundleMatcher<>(),
                        new PropertyMatcher<>(),
                        searchableMatcher
                )
        ));

        return controllerSearchService;
    }
}
