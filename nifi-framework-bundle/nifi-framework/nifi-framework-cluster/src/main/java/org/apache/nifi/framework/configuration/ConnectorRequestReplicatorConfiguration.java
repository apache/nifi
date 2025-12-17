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

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.ClusteredConnectorRequestReplicator;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.components.connector.ConnectorRequestReplicator;
import org.apache.nifi.components.connector.StandaloneConnectorRequestReplicator;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConnectorRequestReplicatorConfiguration {

    @Bean
    public ConnectorRequestReplicator connectorRequestReplicator(
            final NiFiProperties nifiProperties,
            final ObjectProvider<RequestReplicator> requestReplicatorProvider,
            final ObjectProvider<ClusterCoordinator> clusterCoordinatorProvider) {

        if (nifiProperties.isClustered()) {
            // We have to use an ObjectProvider here and obtain a Supplier because of a circular dependency.
            // The request replicator will not be available when created. However, it will be
            // available before attempting to use the ConnectorRequestReplicator.
            final String httpsHostname = nifiProperties.getProperty(NiFiProperties.WEB_HTTPS_HOST);
            final boolean httpsEnabled = httpsHostname != null;
            return new ClusteredConnectorRequestReplicator(requestReplicatorProvider::getIfAvailable, clusterCoordinatorProvider::getIfAvailable, httpsEnabled);
        }

        return new StandaloneConnectorRequestReplicator();
    }
}
