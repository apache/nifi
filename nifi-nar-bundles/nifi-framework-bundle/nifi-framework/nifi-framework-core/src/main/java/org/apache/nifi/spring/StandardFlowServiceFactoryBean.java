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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.StandardFlowService;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory bean for creating a singleton FlowController instance. If the application is configured to act as the cluster manager, then null is always returned as the created instance.
 */
@SuppressWarnings("rawtypes")
public class StandardFlowServiceFactoryBean implements FactoryBean, ApplicationContextAware {

    private ApplicationContext applicationContext;
    private FlowService flowService;
    private NiFiProperties properties;
    private StringEncryptor encryptor;
    private Authorizer authorizer;

    @Override
    public Object getObject() throws Exception {
        if (flowService == null) {
            final FlowController flowController = applicationContext.getBean("flowController", FlowController.class);
            final RevisionManager revisionManager = applicationContext.getBean("revisionManager", RevisionManager.class);

            if (properties.isNode()) {
                final NodeProtocolSenderListener nodeProtocolSenderListener = applicationContext.getBean("nodeProtocolSenderListener", NodeProtocolSenderListener.class);
                final ClusterCoordinator clusterCoordinator = applicationContext.getBean("clusterCoordinator", ClusterCoordinator.class);
                flowService = StandardFlowService.createClusteredInstance(
                    flowController,
                    properties,
                    nodeProtocolSenderListener,
                    clusterCoordinator,
                    encryptor,
                    revisionManager,
                    authorizer);
            } else {
                flowService = StandardFlowService.createStandaloneInstance(
                    flowController,
                    properties,
                    encryptor,
                    revisionManager,
                    authorizer);
            }
        }

        return flowService;
    }

    @Override
    public Class getObjectType() {
        return FlowService.class;
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

    public void setEncryptor(StringEncryptor encryptor) {
        this.encryptor = encryptor;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

}
