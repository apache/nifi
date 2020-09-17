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
package org.apache.nifi.cluster.protocol.spring;

import java.util.concurrent.TimeUnit;
import org.apache.nifi.io.socket.ServerSocketConfiguration;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.FactoryBean;

/**
 * Factory bean for creating a singleton ServerSocketConfiguration instance.
 */
public class ServerSocketConfigurationFactoryBean implements FactoryBean<ServerSocketConfiguration> {

    private ServerSocketConfiguration configuration;
    private NiFiProperties properties;

    @Override
    public ServerSocketConfiguration getObject() throws Exception {
        if (configuration == null) {
            configuration = new ServerSocketConfiguration();
            configuration.setNeedClientAuth(true);

            final int timeout = (int) FormatUtils.getPreciseTimeDuration(properties.getClusterNodeReadTimeout(), TimeUnit.MILLISECONDS);
            configuration.setSocketTimeout(timeout);
            configuration.setReuseAddress(true);

            // If the cluster protocol is marked as secure
            if (Boolean.parseBoolean(properties.getProperty(NiFiProperties.CLUSTER_PROTOCOL_IS_SECURE))) {
                // Parse the TLS configuration from the properties
                configuration.setTlsConfiguration(StandardTlsConfiguration.fromNiFiProperties(properties));
            }
        }
        return configuration;

    }

    @Override
    public Class<ServerSocketConfiguration> getObjectType() {
        return ServerSocketConfiguration.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
