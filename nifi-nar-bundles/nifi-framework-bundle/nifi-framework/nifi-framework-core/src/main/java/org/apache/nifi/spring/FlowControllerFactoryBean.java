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
package org.apache.nifi.spring;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.reporting.BulletinRepository;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory bean for creating a singleton FlowController instance. If the application is configured to act as the cluster manager, then null is always returned as the created instance.
 */
@SuppressWarnings("rawtypes")
public class FlowControllerFactoryBean implements FactoryBean, ApplicationContextAware {

    private ApplicationContext applicationContext;
    private FlowController flowController;
    private NiFiProperties properties;
    private Authorizer authorizer;
    private AuditService auditService;
    private StringEncryptor encryptor;
    private BulletinRepository bulletinRepository;
    private ClusterCoordinator clusterCoordinator;
    private VariableRegistry variableRegistry;
    private LeaderElectionManager leaderElectionManager;
    private FlowRegistryClient flowRegistryClient;
    private ExtensionManager extensionManager;

    @Override
    public Object getObject() throws Exception {
        if (flowController == null) {
            final FlowFileEventRepository flowFileEventRepository = applicationContext.getBean("flowFileEventRepository", FlowFileEventRepository.class);

            if (properties.isNode()) {
                final NodeProtocolSender nodeProtocolSender = applicationContext.getBean("nodeProtocolSender", NodeProtocolSender.class);
                final HeartbeatMonitor heartbeatMonitor = applicationContext.getBean("heartbeatMonitor", HeartbeatMonitor.class);
                flowController = FlowController.createClusteredInstance(
                    flowFileEventRepository,
                    properties,
                    authorizer,
                    auditService,
                    encryptor,
                    nodeProtocolSender,
                    bulletinRepository,
                    clusterCoordinator,
                    heartbeatMonitor,
                    leaderElectionManager,
                    variableRegistry,
                    flowRegistryClient,
                    extensionManager);
            } else {
                flowController = FlowController.createStandaloneInstance(
                    flowFileEventRepository,
                    properties,
                    authorizer,
                    auditService,
                    encryptor,
                    bulletinRepository,
                    variableRegistry,
                    flowRegistryClient,
                    extensionManager);
            }

        }

        return flowController;
    }



    @Override
    public Class getObjectType() {
        return FlowController.class;
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

    public void setAuthorizer(final Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setEncryptor(final StringEncryptor encryptor) {
        this.encryptor = encryptor;
    }

    public void setAuditService(final AuditService auditService) {
        this.auditService = auditService;
    }

    public void setBulletinRepository(final BulletinRepository bulletinRepository) {
        this.bulletinRepository = bulletinRepository;
    }

    public void setVariableRegistry(VariableRegistry variableRegistry) {
        this.variableRegistry = variableRegistry;
    }

    public void setClusterCoordinator(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    public void setLeaderElectionManager(final LeaderElectionManager leaderElectionManager) {
        this.leaderElectionManager = leaderElectionManager;
    }

    public void setFlowRegistryClient(final FlowRegistryClient flowRegistryClient) {
        this.flowRegistryClient = flowRegistryClient;
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }
}
