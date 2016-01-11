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

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.cluster.protocol.impl.ClusterServiceDiscovery;
import org.apache.nifi.cluster.protocol.impl.ClusterServiceLocator;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Factory bean for creating a singleton ClusterManagerProtocolServiceLocator instance. If the application is configured to act as the cluster manager, then null is always returned as the created
 * instance.
 *
 * The cluster manager protocol service represents the socket endpoint for sending internal socket messages to the cluster manager.
 */
public class ClusterManagerProtocolServiceLocatorFactoryBean implements FactoryBean, ApplicationContextAware, DisposableBean {

    private ApplicationContext applicationContext;

    private ClusterServiceLocator locator;

    private NiFiProperties properties;

    @Override
    public Object getObject() throws Exception {
        /*
         * If configured for the cluster manager, then the service locator is never used.
         */
        if (properties.isClusterManager()) {
            return null;
        } else if (locator == null) {

            if (properties.getClusterProtocolUseMulticast()) {

                // get the service discovery instance
                final ClusterServiceDiscovery serviceDiscovery = applicationContext.getBean("clusterManagerProtocolServiceDiscovery", ClusterServiceDiscovery.class);

                // create service location configuration
                final ClusterServiceLocator.AttemptsConfig config = new ClusterServiceLocator.AttemptsConfig();
                config.setNumAttempts(properties.getClusterProtocolMulticastServiceLocatorAttempts());

                final int delay = (int) FormatUtils.getTimeDuration(properties.getClusterProtocolMulticastServiceLocatorAttemptsDelay(), TimeUnit.SECONDS);
                config.setTimeBetweenAttempts(delay);
                config.setTimeBetweenAttempsUnit(TimeUnit.SECONDS);

                locator = new ClusterServiceLocator(serviceDiscovery);
                locator.setAttemptsConfig(config);

            } else {
                final String serviceName = applicationContext.getBean("clusterManagerProtocolServiceName", String.class);
                final InetSocketAddress serviceAddress = properties.getClusterNodeUnicastManagerProtocolAddress();
                final DiscoverableService service = new DiscoverableServiceImpl(serviceName, serviceAddress);
                locator = new ClusterServiceLocator(service);
            }

            // start the locator
            locator.start();

        }
        return locator;

    }

    @Override
    public Class getObjectType() {
        return ClusterServiceLocator.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws Exception {
        if (locator != null && locator.isRunning()) {
            locator.stop();
        }
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
