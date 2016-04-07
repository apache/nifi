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
package org.apache.nifi.cluster.authorization;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.AuthorityProviderConfigurationContext;
import org.apache.nifi.authorization.AuthorityProviderInitializationContext;
import org.apache.nifi.authorization.FileAuthorizationProvider;
import org.apache.nifi.authorization.annotation.AuthorityProviderContext;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.ProviderDestructionException;
import org.apache.nifi.cluster.authorization.protocol.message.DoesDnExistMessage;
import org.apache.nifi.cluster.authorization.protocol.message.GetAuthoritiesMessage;
import org.apache.nifi.cluster.authorization.protocol.message.GetGroupForUserMessage;
import org.apache.nifi.cluster.authorization.protocol.message.ProtocolMessage;
import static org.apache.nifi.cluster.authorization.protocol.message.ProtocolMessage.MessageType.DOES_DN_EXIST;
import static org.apache.nifi.cluster.authorization.protocol.message.ProtocolMessage.MessageType.GET_AUTHORITIES;
import static org.apache.nifi.cluster.authorization.protocol.message.ProtocolMessage.MessageType.GET_GROUP_FOR_USER;
import org.apache.nifi.cluster.authorization.protocol.message.jaxb.JaxbProtocolUtils;
import org.apache.nifi.cluster.manager.impl.WebClusterManager;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.ProtocolMessageUnmarshaller;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.io.socket.ServerSocketConfiguration;
import org.apache.nifi.io.socket.SocketListener;
import org.apache.nifi.io.socket.SocketUtils;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.util.NiFiProperties;
import static org.apache.nifi.util.NiFiProperties.CLUSTER_MANAGER_ADDRESS;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Provides authorities for the NCM in clustered environments. Communication
 * occurs over TCP/IP sockets. All method calls are deferred to the
 * FileAuthorizationProvider.
 */
public class ClusterManagerAuthorizationProvider extends FileAuthorizationProvider implements AuthorityProvider, ApplicationContextAware {

    public static final String AUTHORITY_PROVIDER_SERVIVE_NAME = "cluster-authority-provider";

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(ClusterManagerAuthorizationProvider.class));
    private static final String CLUSTER_MANAGER_AUTHORITY_PROVIDER_PORT = "Authority Provider Port";
    private static final String CLUSTER_MANAGER_AUTHORITY_PROVIDER_THREADS = "Authority Provider Threads";
    private static final int DEFAULT_CLUSTER_MANAGER_AUTHORITY_PROVIDER_THREADS = 10;

    private WebClusterManager clusterManager;
    private ProtocolContext<ProtocolMessage> authorityProviderProtocolContext;
    private SocketListener socketListener;
    private NiFiProperties properties;
    private ApplicationContext applicationContext;

    @Override
    public void initialize(final AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException {
        super.initialize(initializationContext);
    }

    @Override
    public void onConfigured(final AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        super.onConfigured(configurationContext);

        // get the socket address of the cluster authority provider
        final InetSocketAddress clusterAuthorityProviderAddress = getClusterManagerAuthorityProviderAddress(configurationContext);

        // get the cluster manager
        clusterManager = applicationContext.getBean("clusterManager", WebClusterManager.class);

        // if using multicast, then the authority provider's service is broadcasted
        if (properties.getClusterProtocolUseMulticast()) {

            // create the authority provider service for discovery
            final DiscoverableService clusterAuthorityProviderService = new DiscoverableServiceImpl(AUTHORITY_PROVIDER_SERVIVE_NAME, clusterAuthorityProviderAddress);

            // register the authority provider service with the cluster manager
            clusterManager.addBroadcastedService(clusterAuthorityProviderService);
        }

        // get the number of protocol listening thread
        final int numThreads = getClusterManagerAuthorityProviderThreads(configurationContext);

        // the server socket configuration
        final ServerSocketConfiguration configuration = applicationContext.getBean("protocolServerSocketConfiguration", ServerSocketConfiguration.class);

        // the authority provider listens for node messages
        socketListener = new SocketListener(numThreads, clusterAuthorityProviderAddress.getPort(), configuration) {
            @Override
            public void dispatchRequest(final Socket socket) {
                ClusterManagerAuthorizationProvider.this.dispatchRequest(socket);
            }
        };

        // start the socket listener
        if (socketListener != null && !socketListener.isRunning()) {
            try {
                socketListener.start();
            } catch (final IOException ioe) {
                throw new ProviderCreationException("Failed to start Cluster Manager Authorization Provider due to: " + ioe, ioe);
            }
        }

        // initialize the protocol context
        authorityProviderProtocolContext = new JaxbProtocolContext<ProtocolMessage>(JaxbProtocolUtils.JAXB_CONTEXT);
    }

    @Override
    public void preDestruction() throws ProviderDestructionException {
        if (socketListener != null && socketListener.isRunning()) {
            try {
                socketListener.stop();
            } catch (final IOException ioe) {
                throw new ProviderDestructionException("Failed to stop Cluster Manager Authorization Provider due to: " + ioe, ioe);
            }
        }
        super.preDestruction();
    }

    private int getClusterManagerAuthorityProviderThreads(final AuthorityProviderConfigurationContext configurationContext) {
        try {
            return Integer.parseInt(configurationContext.getProperty(CLUSTER_MANAGER_AUTHORITY_PROVIDER_THREADS));
        } catch (NumberFormatException nfe) {
            return DEFAULT_CLUSTER_MANAGER_AUTHORITY_PROVIDER_THREADS;
        }
    }

    private InetSocketAddress getClusterManagerAuthorityProviderAddress(final AuthorityProviderConfigurationContext configurationContext) {
        try {
            String socketAddress = properties.getProperty(CLUSTER_MANAGER_ADDRESS);
            if (StringUtils.isBlank(socketAddress)) {
                socketAddress = "localhost";
            }
            return InetSocketAddress.createUnresolved(socketAddress, getClusterManagerAuthorityProviderPort(configurationContext));
        } catch (Exception ex) {
            throw new RuntimeException("Invalid manager authority provider address/port due to: " + ex, ex);
        }
    }

    private Integer getClusterManagerAuthorityProviderPort(final AuthorityProviderConfigurationContext configurationContext) {
        final String authorityProviderPort = configurationContext.getProperty(CLUSTER_MANAGER_AUTHORITY_PROVIDER_PORT);
        if (authorityProviderPort == null || authorityProviderPort.trim().isEmpty()) {
            throw new ProviderCreationException("The authority provider port must be specified.");
        }

        return Integer.parseInt(authorityProviderPort);
    }

    private void dispatchRequest(final Socket socket) {
        try {
            // unmarshall message
            final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller = authorityProviderProtocolContext.createUnmarshaller();
            final ProtocolMessage request = unmarshaller.unmarshal(socket.getInputStream());
            final ProtocolMessage response = request;

            try {
                switch (request.getType()) {
                    case DOES_DN_EXIST: {
                        final DoesDnExistMessage castedMsg = (DoesDnExistMessage) request;
                        castedMsg.setResponse(doesDnExist(castedMsg.getDn()));
                        break;
                    }
                    case GET_AUTHORITIES: {
                        final GetAuthoritiesMessage castedMsg = (GetAuthoritiesMessage) request;
                        castedMsg.setResponse(getAuthorities(castedMsg.getDn()));
                        break;
                    }
                    case GET_GROUP_FOR_USER: {
                        final GetGroupForUserMessage castedMsg = (GetGroupForUserMessage) request;
                        castedMsg.setResponse(getGroupForUser(castedMsg.getDn()));
                        break;
                    }
                    default: {
                        throw new Exception("Unsupported Message Type: " + request.getType());
                    }
                }
            } catch (final Exception ex) {
                response.setExceptionClass(ex.getClass().getName());
                response.setExceptionMessage(ex.getMessage());
            }

            final ProtocolMessageMarshaller<ProtocolMessage> marshaller = authorityProviderProtocolContext.createMarshaller();
            marshaller.marshal(response, socket.getOutputStream());

        } catch (final Exception e) {
            logger.warn("Failed processing Socket Authorization Provider protocol message due to " + e, e);
        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    @Override
    @AuthorityProviderContext
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    @AuthorityProviderContext
    public void setNiFiProperties(NiFiProperties properties) {
        super.setNiFiProperties(properties);
        this.properties = properties;
    }
}
