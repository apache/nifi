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

import org.apache.nifi.cluster.coordination.flow.FlowElection;
import org.apache.nifi.cluster.coordination.node.NodeClusterCoordinator;
import org.apache.nifi.cluster.firewall.ClusterNodeFirewall;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

public class NodeClusterCoordinatorFactoryBean implements FactoryBean<NodeClusterCoordinator>, ApplicationContextAware {
    private ApplicationContext applicationContext;
    private NiFiProperties properties;
    private ExtensionManager extensionManager;

    private NodeClusterCoordinator nodeClusterCoordinator = null;

    @Override
    public NodeClusterCoordinator getObject() throws Exception {
        if (nodeClusterCoordinator == null && properties.isNode()) {
            final ClusterCoordinationProtocolSenderListener protocolSenderListener =
                    applicationContext.getBean("clusterCoordinationProtocolSenderListener", ClusterCoordinationProtocolSenderListener.class);
            final EventReporter eventReporter = applicationContext.getBean("eventReporter", EventReporter.class);
            final ClusterNodeFirewall clusterFirewall = applicationContext.getBean("clusterFirewall", ClusterNodeFirewall.class);
            final RevisionManager revisionManager = applicationContext.getBean("revisionManager", RevisionManager.class);
            final LeaderElectionManager electionManager = applicationContext.getBean("leaderElectionManager", LeaderElectionManager.class);
            final FlowElection flowElection = applicationContext.getBean("flowElection", FlowElection.class);
            final NodeProtocolSender nodeProtocolSender = applicationContext.getBean("nodeProtocolSender", NodeProtocolSender.class);
            nodeClusterCoordinator = new NodeClusterCoordinator(protocolSenderListener, eventReporter, electionManager, flowElection, clusterFirewall,
                    revisionManager, properties, extensionManager, nodeProtocolSender);
        }

        return nodeClusterCoordinator;
    }

    @Override
    public Class<?> getObjectType() {
        return NodeClusterCoordinator.class;
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

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

}
