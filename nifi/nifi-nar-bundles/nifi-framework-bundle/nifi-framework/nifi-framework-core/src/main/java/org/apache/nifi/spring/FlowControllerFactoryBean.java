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

import org.apache.nifi.admin.service.UserService;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.util.NiFiProperties;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory bean for creating a singleton FlowController instance. If the
 * application is configured to act as the cluster manager, then null is always
 * returned as the created instance.
 */
@SuppressWarnings("rawtypes")
public class FlowControllerFactoryBean implements FactoryBean, ApplicationContextAware {

    private ApplicationContext applicationContext;
    private FlowController flowController;
    private NiFiProperties properties;
    private UserService userService;
    private StringEncryptor encryptor;

    @Override
    public Object getObject() throws Exception {
        /*
         * If configured for the cluster manager, then the flow controller is never used.  
         */
        if (properties.isClusterManager()) {
            return null;
        } else if (flowController == null) {

            final FlowFileEventRepository flowFileEventRepository = applicationContext.getBean("flowFileEventRepository", FlowFileEventRepository.class);

            if (properties.isNode()) {
                final NodeProtocolSender nodeProtocolSender = applicationContext.getBean("nodeProtocolSender", NodeProtocolSender.class);
                flowController = FlowController.createClusteredInstance(
                        flowFileEventRepository,
                        properties,
                        userService,
                        encryptor,
                        nodeProtocolSender);
            } else {
                flowController = FlowController.createStandaloneInstance(
                        flowFileEventRepository,
                        properties,
                        userService,
                        encryptor);
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

    public void setUserService(final UserService userService) {
        this.userService = userService;
    }

    public void setEncryptor(final StringEncryptor encryptor) {
        this.encryptor = encryptor;
    }
}
