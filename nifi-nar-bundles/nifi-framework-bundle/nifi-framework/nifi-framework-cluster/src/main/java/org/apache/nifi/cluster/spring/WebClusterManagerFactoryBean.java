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

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.cluster.event.EventManager;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.flow.DataFlowManagementService;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.protocol.impl.ClusterManagerProtocolSenderListener;
import org.apache.nifi.cluster.protocol.impl.ClusterServicesBroadcaster;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory bean for creating a singleton WebClusterManager instance. If the application is not configured to act as the cluster manager, then null is always returned as the created instance.
 */
public class WebClusterManagerFactoryBean implements FactoryBean, ApplicationContextAware {

    private ApplicationContext applicationContext;

    private WebClusterManager clusterManager;

    private NiFiProperties properties;

    private StringEncryptor encryptor;

    @Override
    public Object getObject() throws Exception {
        if (properties.isClusterManager() && properties.isNode()) {
            throw new IllegalStateException("Application may be configured as a cluster manager or a node, but not both.");
        } else if (!properties.isClusterManager()) {
            /*
             * If not configured for the cluster manager, then the cluster manager is never used.
             * null is returned so that we don't instantiate a thread pool or other resources.
             */
            return null;
        } else if (clusterManager == null) {
            final DataFlowManagementService dataFlowService = applicationContext.getBean("dataFlowManagementService", DataFlowManagementService.class);
            final ClusterManagerProtocolSenderListener senderListener = applicationContext.getBean("clusterManagerProtocolSenderListener", ClusterManagerProtocolSenderListener.class);

            // create the manager
            clusterManager = new WebClusterManager(
                    dataFlowService,
                    senderListener,
                    properties,
                encryptor
            );

            // set the service broadcaster
            if (properties.getClusterProtocolUseMulticast()) {

                // create broadcaster
                final ClusterServicesBroadcaster broadcaster = applicationContext.getBean("clusterServicesBroadcaster", ClusterServicesBroadcaster.class);

                // register the cluster manager protocol service
                final String clusterManagerProtocolServiceName = applicationContext.getBean("clusterManagerProtocolServiceName", String.class);
                final DiscoverableService clusterManagerProtocolService = new DiscoverableServiceImpl(clusterManagerProtocolServiceName, properties.getClusterManagerProtocolAddress());
                broadcaster.addService(clusterManagerProtocolService);

                clusterManager.setServicesBroadcaster(broadcaster);
            }

            // set the event manager
            clusterManager.setEventManager(applicationContext.getBean("nodeEventHistoryManager", EventManager.class));

            // set the cluster firewall
            clusterManager.setClusterFirewall(applicationContext.getBean("clusterFirewall", ClusterNodeFirewall.class));

            // set the audit service
            clusterManager.setAuditService(applicationContext.getBean("auditService", AuditService.class));
        }
        return clusterManager;
    }

    @Override
    public Class getObjectType() {
        return WebClusterManager.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setEncryptor(final StringEncryptor encryptor) {
        this.encryptor = encryptor;
    }
}
