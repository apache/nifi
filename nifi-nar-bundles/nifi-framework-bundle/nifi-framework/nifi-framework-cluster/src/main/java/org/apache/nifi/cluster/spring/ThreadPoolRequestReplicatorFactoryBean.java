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

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.RequestCompletionCallback;
import org.apache.nifi.cluster.coordination.http.replication.ThreadPoolRequestReplicator;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.framework.security.util.SslContextFactory;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.util.WebUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class ThreadPoolRequestReplicatorFactoryBean implements FactoryBean<ThreadPoolRequestReplicator>, ApplicationContextAware {
    private ApplicationContext applicationContext;
    private NiFiProperties nifiProperties;

    private ThreadPoolRequestReplicator replicator = null;

    @Override
    public ThreadPoolRequestReplicator getObject() throws Exception {
        if (replicator == null && nifiProperties.isNode()) {
            final EventReporter eventReporter = applicationContext.getBean("eventReporter", EventReporter.class);
            final ClusterCoordinator clusterCoordinator = applicationContext.getBean("clusterCoordinator", ClusterCoordinator.class);
            final RequestCompletionCallback requestCompletionCallback = applicationContext.getBean("clusterCoordinator", RequestCompletionCallback.class);

            final int corePoolSize = nifiProperties.getClusterNodeProtocolCorePoolSize();
            final int maxPoolSize = nifiProperties.getClusterNodeProtocolMaxPoolSize();
            final int maxConcurrentRequests = nifiProperties.getClusterNodeMaxConcurrentRequests();
            final Client jerseyClient = WebUtils.createClient(new DefaultClientConfig(), SslContextFactory.createSslContext(nifiProperties));
            final String connectionTimeout = nifiProperties.getClusterNodeConnectionTimeout();
            final String readTimeout = nifiProperties.getClusterNodeReadTimeout();

            replicator = new ThreadPoolRequestReplicator(corePoolSize, maxPoolSize, maxConcurrentRequests, jerseyClient, clusterCoordinator,
                connectionTimeout, readTimeout, requestCompletionCallback, eventReporter, nifiProperties);
        }

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

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setProperties(final NiFiProperties properties) {
        this.nifiProperties = properties;
    }

}
