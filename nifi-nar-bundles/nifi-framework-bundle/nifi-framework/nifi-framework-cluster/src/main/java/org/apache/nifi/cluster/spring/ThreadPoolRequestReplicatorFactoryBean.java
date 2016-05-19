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

package org.apache.nifi.cluster.spring;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestCompletionCallback;
import org.apache.nifi.cluster.coordination.http.replication.ThreadPoolRequestReplicator;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.framework.security.util.SslContextFactory;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.util.WebUtils;
import org.springframework.beans.factory.FactoryBean;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class ThreadPoolRequestReplicatorFactoryBean implements FactoryBean<ThreadPoolRequestReplicator> {
    private NiFiProperties properties;

    private EventReporter eventReporter;
    private ClusterCoordinator clusterCoordinator;
    private RequestCompletionCallback requestCompletionCallback;

    @Override
    public ThreadPoolRequestReplicator getObject() throws Exception {
        final int numThreads = properties.getClusterNodeProtocolThreads();
        final Client jerseyClient = WebUtils.createClient(new DefaultClientConfig(), SslContextFactory.createSslContext(properties));
        final String connectionTimeout = properties.getClusterProtocolConnectionHandshakeTimeout();
        final String readTimeout = properties.getClusterProtocolSocketTimeout();

        final ThreadPoolRequestReplicator replicator = new ThreadPoolRequestReplicator(numThreads, jerseyClient, clusterCoordinator,
            connectionTimeout, readTimeout, requestCompletionCallback, eventReporter);

        return replicator;
    }

    @Override
    public Class<?> getObjectType() {
        return ThreadPoolRequestReplicator.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setEventReporter(final EventReporter eventReporter) {
        this.eventReporter = eventReporter;
    }

    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    public void setRequestCompletionCallback(final RequestCompletionCallback callback) {
        this.requestCompletionCallback = callback;
    }
}
