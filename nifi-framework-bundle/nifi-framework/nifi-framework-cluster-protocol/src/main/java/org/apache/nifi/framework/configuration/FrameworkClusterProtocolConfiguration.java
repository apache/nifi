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

import org.apache.nifi.cluster.coordination.node.LeaderElectionNodeProtocolSender;
import org.apache.nifi.cluster.protocol.ClusterCoordinationProtocolSender;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolListener;
import org.apache.nifi.cluster.protocol.impl.ClusterCoordinationProtocolSenderListener;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.cluster.protocol.impl.SocketProtocolListener;
import org.apache.nifi.cluster.protocol.impl.StandardClusterCoordinationProtocolSender;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.cluster.protocol.jaxb.message.JaxbProtocolUtils;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.io.socket.ServerSocketConfiguration;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.net.ssl.SSLContext;
import java.util.concurrent.TimeUnit;

/**
 * Framework Cluster Protocol Configuration with components supporting cluster communication
 */
@Configuration
public class FrameworkClusterProtocolConfiguration {

    private NiFiProperties properties;

    private SSLContext sslContext;

    private LeaderElectionManager leaderElectionManager;

    @Autowired
    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Autowired(required = false)
    public void setSslContext(final SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    @Autowired
    public void setLeaderElectionManager(final LeaderElectionManager leaderElectionManager) {
        this.leaderElectionManager = leaderElectionManager;
    }

    @Bean
    public JaxbProtocolContext<ProtocolMessage> protocolContext() {
        return new JaxbProtocolContext<>(JaxbProtocolUtils.JAXB_CONTEXT);
    }

    @Bean
    public SocketConfiguration protocolSocketConfiguration() {
        final SocketConfiguration configuration = new SocketConfiguration();

        final int timeout = (int) FormatUtils.getPreciseTimeDuration(properties.getClusterNodeReadTimeout(), TimeUnit.MILLISECONDS);
        configuration.setSocketTimeout(timeout);
        configuration.setReuseAddress(true);
        configuration.setSslContext(sslContext);

        return configuration;
    }

    @Bean
    public ServerSocketConfiguration protocolServerSocketConfiguration() {
        final ServerSocketConfiguration configuration = new ServerSocketConfiguration();

        configuration.setNeedClientAuth(true);

        final int timeout = (int) FormatUtils.getPreciseTimeDuration(properties.getClusterNodeReadTimeout(), TimeUnit.MILLISECONDS);
        configuration.setSocketTimeout(timeout);
        configuration.setReuseAddress(true);
        configuration.setSslContext(sslContext);

        return configuration;
    }

    @Bean
    public ClusterCoordinationProtocolSender clusterCoordinationProtocolSender() {
        final StandardClusterCoordinationProtocolSender sender = new StandardClusterCoordinationProtocolSender(
                protocolSocketConfiguration(),
                protocolContext(),
                properties.getClusterNodeProtocolMaxPoolSize()
        );
        sender.setHandshakeTimeout(properties.getClusterNodeConnectionTimeout());

        return sender;
    }

    @Bean
    public ClusterCoordinationProtocolSenderListener clusterCoordinationProtocolSenderListener() {
        return new ClusterCoordinationProtocolSenderListener(
                clusterCoordinationProtocolSender(),
                protocolListener()
        );
    }

    @Bean
    public ProtocolListener protocolListener() {
        final Integer protocolPort = properties.getClusterNodeProtocolPort();
        final int clusterNodeProtocolPort = protocolPort == null ? 0 : protocolPort;

        return new SocketProtocolListener(
                properties.getClusterNodeProtocolMaxPoolSize(),
                clusterNodeProtocolPort,
                protocolServerSocketConfiguration(),
                protocolContext()
        );
    }

    @Bean
    public NodeProtocolSenderListener nodeProtocolSenderListener() {
        return new NodeProtocolSenderListener(
                nodeProtocolSender(),
                protocolListener()
        );
    }

    @Bean
    public NodeProtocolSender nodeProtocolSender() {
        return new LeaderElectionNodeProtocolSender(
                protocolSocketConfiguration(),
                protocolContext(),
                leaderElectionManager
        );
    }
}
