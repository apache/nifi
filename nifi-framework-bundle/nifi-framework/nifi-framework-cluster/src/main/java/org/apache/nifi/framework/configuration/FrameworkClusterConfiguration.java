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
package org.apache.nifi.framework.configuration;

import org.apache.nifi.cluster.ClusterDetailsFactory;
import org.apache.nifi.cluster.StandardClusterDetailsFactory;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.HttpReplicationClient;
import org.apache.nifi.cluster.coordination.http.replication.RequestCompletionCallback;
import org.apache.nifi.cluster.coordination.http.replication.StandardUploadRequestReplicator;
import org.apache.nifi.cluster.coordination.http.replication.ThreadPoolRequestReplicator;
import org.apache.nifi.cluster.coordination.http.replication.UploadRequestReplicator;
import org.apache.nifi.cluster.coordination.http.replication.client.StandardHttpReplicationClient;
import org.apache.nifi.cluster.lifecycle.ClusterDecommissionTask;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;

/**
 * Framework Cluster Configuration with components supporting request replication and cluster details
 */
@Configuration
public class FrameworkClusterConfiguration {
    private NiFiProperties properties;

    private EventReporter eventReporter;

    private FlowController flowController;

    private ClusterCoordinator clusterCoordinator;

    private WebClientService webClientService;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Autowired
    public void setEventReporter(final EventReporter eventReporter) {
        this.eventReporter = eventReporter;
    }

    @Autowired
    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    @Autowired
    public void setWebClientService(final WebClientService webClientService) {
        this.webClientService = webClientService;
    }

    @Bean
    public ThreadPoolRequestReplicator requestReplicator(
            @Autowired(required = false) final SSLContext sslContext,
            @Autowired(required = false) final X509TrustManager trustManager
    ) {
        final ThreadPoolRequestReplicator replicator;

        if (clusterCoordinator == null) {
            replicator = null;
        } else {
            final HttpReplicationClient replicationClient = new StandardHttpReplicationClient(webClientService, StandardHttpUriBuilder::new);

            replicator = new ThreadPoolRequestReplicator(
                    properties.getClusterNodeProtocolMaxPoolSize(),
                    properties.getClusterNodeMaxConcurrentRequests(),
                    replicationClient,
                    clusterCoordinator,
                    (RequestCompletionCallback) clusterCoordinator,
                    eventReporter,
                    properties
            );
        }

        return replicator;
    }

    @Bean
    public ClusterDecommissionTask decommissionTask() {
        return new ClusterDecommissionTask(clusterCoordinator, flowController);
    }

    @Bean
    public ClusterDetailsFactory clusterDetailsFactory() {
        return new StandardClusterDetailsFactory(clusterCoordinator);
    }

    @Bean
    public UploadRequestReplicator uploadRequestReplicator() throws IOException {
        if (clusterCoordinator == null) {
            return null;
        }
        return new StandardUploadRequestReplicator(clusterCoordinator, webClientService, properties);
    }
}
