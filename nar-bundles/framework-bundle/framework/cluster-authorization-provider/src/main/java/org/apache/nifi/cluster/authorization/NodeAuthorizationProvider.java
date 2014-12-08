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

import org.apache.nifi.cluster.authorization.protocol.message.DoesDnExistMessage;
import org.apache.nifi.cluster.authorization.protocol.message.GetAuthoritiesMessage;
import org.apache.nifi.cluster.authorization.protocol.message.ProtocolMessage;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.authorization.Authority;
import org.apache.nifi.authorization.AuthorityProvider;
import org.apache.nifi.authorization.AuthorityProviderConfigurationContext;
import org.apache.nifi.authorization.AuthorityProviderInitializationContext;
import org.apache.nifi.authorization.annotation.AuthorityProviderContext;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.IdentityAlreadyExistsException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.ProviderDestructionException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.cluster.authorization.protocol.message.GetGroupForUserMessage;
import org.apache.nifi.cluster.authorization.protocol.message.jaxb.JaxbProtocolUtils;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.io.socket.SocketUtils;
import org.apache.nifi.io.socket.multicast.DiscoverableService;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolMessageMarshaller;
import org.apache.nifi.cluster.protocol.ProtocolMessageUnmarshaller;
import org.apache.nifi.cluster.protocol.impl.ClusterServiceDiscovery;
import org.apache.nifi.cluster.protocol.impl.ClusterServiceLocator;
import org.apache.nifi.cluster.protocol.jaxb.JaxbProtocolContext;
import org.apache.nifi.io.socket.multicast.DiscoverableServiceImpl;
import org.apache.nifi.io.socket.multicast.MulticastConfiguration;
import org.apache.nifi.logging.NiFiLog;
import org.apache.nifi.util.NiFiProperties;
import static org.apache.nifi.util.NiFiProperties.CLUSTER_NODE_UNICAST_MANAGER_ADDRESS;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * Provides authorities for nodes in clustered environments. Communication
 * occurs over TCP/IP sockets. All method calls are communicated to the cluster
 * manager provider via socket.
 */
public class NodeAuthorizationProvider implements AuthorityProvider, ApplicationContextAware {

    private static final Logger logger = new NiFiLog(LoggerFactory.getLogger(NodeAuthorizationProvider.class));
    private static final String CLUSTER_NODE_MANAGER_AUTHORITY_PROVIDER_PORT = "Cluster Manager Authority Provider Port";

    private ProtocolContext<ProtocolMessage> authorityProviderProtocolContext;
    private SocketConfiguration socketConfiguration;
    private ClusterServiceLocator serviceLocator;
    private ApplicationContext applicationContext;
    private NiFiProperties properties;

    @Override
    public void initialize(AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException {
    }

    @Override
    public void onConfigured(final AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException {
        // TODO clear user cache?

        // if using multicast, then the authority provider's service is broadcasted
        if (properties.getClusterProtocolUseMulticast()) {
            // create the service discovery
            final ClusterServiceDiscovery serviceDiscovery = new ClusterServiceDiscovery(
                    ClusterManagerAuthorizationProvider.AUTHORITY_PROVIDER_SERVIVE_NAME,
                    properties.getClusterProtocolMulticastAddress(),
                    applicationContext.getBean("protocolMulticastConfiguration", MulticastConfiguration.class),
                    applicationContext.getBean("protocolContext", ProtocolContext.class));

            // create service location configuration
            final ClusterServiceLocator.AttemptsConfig config = new ClusterServiceLocator.AttemptsConfig();
            config.setNumAttempts(3);
            config.setTimeBetweenAttempts(1);
            config.setTimeBetweenAttempsUnit(TimeUnit.SECONDS);

            serviceLocator = new ClusterServiceLocator(serviceDiscovery);
            serviceLocator.setAttemptsConfig(config);
        } else {
            final InetSocketAddress serviceAddress = getClusterNodeManagerAuthorityProviderAddress(configurationContext);
            final DiscoverableService service = new DiscoverableServiceImpl(ClusterManagerAuthorizationProvider.AUTHORITY_PROVIDER_SERVIVE_NAME, serviceAddress);
            serviceLocator = new ClusterServiceLocator(service);
        }

        try {
            // start the service locator
            serviceLocator.start();
        } catch (final IOException ioe) {
            throw new ProviderCreationException(ioe);
        }

        // the socket configuration
        socketConfiguration = applicationContext.getBean("protocolSocketConfiguration", SocketConfiguration.class);

        // initialize the protocol context
        authorityProviderProtocolContext = new JaxbProtocolContext<ProtocolMessage>(JaxbProtocolUtils.JAXB_CONTEXT);
    }

    private InetSocketAddress getClusterNodeManagerAuthorityProviderAddress(final AuthorityProviderConfigurationContext configurationContext) {
        try {
            String socketAddress = properties.getProperty(CLUSTER_NODE_UNICAST_MANAGER_ADDRESS);
            if (StringUtils.isBlank(socketAddress)) {
                socketAddress = "localhost";
            }
            return InetSocketAddress.createUnresolved(socketAddress, getClusterNodeManagerAuthorityProviderPort(configurationContext));
        } catch (Exception ex) {
            throw new ProviderCreationException("Invalid cluster manager authority provider address/port due to: " + ex, ex);
        }
    }

    private Integer getClusterNodeManagerAuthorityProviderPort(final AuthorityProviderConfigurationContext configurationContext) {
        final String nodeAuthorityProviderPort = configurationContext.getProperty(CLUSTER_NODE_MANAGER_AUTHORITY_PROVIDER_PORT);
        if (nodeAuthorityProviderPort == null || nodeAuthorityProviderPort.trim().isEmpty()) {
            throw new ProviderCreationException("The cluster manager authority provider port must be specified.");
        }

        return Integer.parseInt(nodeAuthorityProviderPort);
    }

    @Override
    public void setAuthorities(String dn, Set<Authority> authorities) throws AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to set user authorities.");
    }

    @Override
    public void addUser(String dn, String group) throws IdentityAlreadyExistsException, AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to add users.");
    }

    @Override
    public boolean doesDnExist(String dn) throws AuthorityAccessException {
        // create message
        final DoesDnExistMessage msg = new DoesDnExistMessage();
        msg.setDn(dn);

        Socket socket = null;
        try {

            final InetSocketAddress socketAddress = getServiceAddress();
            if (socketAddress == null) {
                throw new AuthorityAccessException("Cluster Authority Provider's address is not known.");
            }

            try {
                // create a socket
                socket = SocketUtils.createSocket(socketAddress, socketConfiguration);
            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed to create socket due to: " + ioe, ioe);
            }

            try {
                // marshal message to output stream
                final ProtocolMessageMarshaller marshaller = authorityProviderProtocolContext.createMarshaller();
                marshaller.marshal(msg, socket.getOutputStream());
            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed marshalling '" + msg.getType() + "' protocol message due to: " + ioe, ioe);
            }

            try {

                // unmarshall response and return
                final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller = authorityProviderProtocolContext.createUnmarshaller();
                final DoesDnExistMessage response = (DoesDnExistMessage) unmarshaller.unmarshal(socket.getInputStream());

                // check if there was an exception
                if (response.wasException()) {
                    throw new AuthorityAccessException(response.getExceptionMessage());
                }

                // return provider's response
                return response.getResponse();

            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed unmarshalling '" + msg.getType() + "' response protocol message due to: " + ioe, ioe);
            }

        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    @Override
    public Set<Authority> getAuthorities(String dn) throws UnknownIdentityException, AuthorityAccessException {
        // create message
        final GetAuthoritiesMessage msg = new GetAuthoritiesMessage();
        msg.setDn(dn);

        Socket socket = null;
        try {

            final InetSocketAddress socketAddress = getServiceAddress();
            if (socketAddress == null) {
                throw new AuthorityAccessException("Cluster Authority Provider's address is not known.");
            }

            try {
                // create a socket
                socket = SocketUtils.createSocket(socketAddress, socketConfiguration);
            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed to create socket due to: " + ioe, ioe);
            }

            try {
                // marshal message to output stream
                final ProtocolMessageMarshaller marshaller = authorityProviderProtocolContext.createMarshaller();
                marshaller.marshal(msg, socket.getOutputStream());
            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed marshalling '" + msg.getType() + "' protocol message due to: " + ioe, ioe);
            }

            try {

                // unmarshall response and return
                final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller = authorityProviderProtocolContext.createUnmarshaller();
                final GetAuthoritiesMessage response = (GetAuthoritiesMessage) unmarshaller.unmarshal(socket.getInputStream());

                // check if there was an exception
                if (response.wasException()) {
                    if (isException(UnknownIdentityException.class, response)) {
                        throw new UnknownIdentityException(response.getExceptionMessage());
                    } else {
                        throw new AuthorityAccessException(response.getExceptionMessage());
                    }
                }

                // return provider's response
                return response.getResponse();

            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed unmarshalling '" + msg.getType() + "' response protocol message due to: " + ioe, ioe);
            }

        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    @Override
    public Set<String> getUsers(Authority authority) throws AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to get users for a given authority.");
    }

    @Override
    public void revokeUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to revoke users.");
    }

    @Override
    public void setUsersGroup(Set<String> dns, String group) throws UnknownIdentityException, AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to set user groups.");
    }

    @Override
    public void ungroupUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to ungroup users.");
    }

    @Override
    public void ungroup(String group) throws AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to ungroup.");
    }

    @Override
    public String getGroupForUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
        // create message
        final GetGroupForUserMessage msg = new GetGroupForUserMessage();
        msg.setDn(dn);

        Socket socket = null;
        try {

            final InetSocketAddress socketAddress = getServiceAddress();
            if (socketAddress == null) {
                throw new AuthorityAccessException("Cluster Authority Provider's address is not known.");
            }

            try {
                // create a socket
                socket = SocketUtils.createSocket(socketAddress, socketConfiguration);
            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed to create socket due to: " + ioe, ioe);
            }

            try {
                // marshal message to output stream
                final ProtocolMessageMarshaller marshaller = authorityProviderProtocolContext.createMarshaller();
                marshaller.marshal(msg, socket.getOutputStream());
            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed marshalling '" + msg.getType() + "' protocol message due to: " + ioe, ioe);
            }

            try {

                // unmarshall response and return
                final ProtocolMessageUnmarshaller<ProtocolMessage> unmarshaller = authorityProviderProtocolContext.createUnmarshaller();
                final GetGroupForUserMessage response = (GetGroupForUserMessage) unmarshaller.unmarshal(socket.getInputStream());

                // check if there was an exception
                if (response.wasException()) {
                    if (isException(UnknownIdentityException.class, response)) {
                        throw new UnknownIdentityException(response.getExceptionMessage());
                    } else {
                        throw new AuthorityAccessException(response.getExceptionMessage());
                    }
                }

                return response.getResponse();
            } catch (final IOException ioe) {
                throw new AuthorityAccessException("Failed unmarshalling '" + msg.getType() + "' response protocol message due to: " + ioe, ioe);
            }

        } finally {
            SocketUtils.closeQuietly(socket);
        }
    }

    @Override
    public void revokeGroup(String group) throws UnknownIdentityException, AuthorityAccessException {
        throw new AuthorityAccessException("Nodes are not allowed to revoke groups.");
    }

    @Override
    public void preDestruction() throws ProviderDestructionException {
        try {
            if (serviceLocator != null && serviceLocator.isRunning()) {
                serviceLocator.stop();
            }
        } catch (final IOException ioe) {
            throw new ProviderDestructionException(ioe);
        }
    }

    @Override
    @AuthorityProviderContext
    public void setApplicationContext(final ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @AuthorityProviderContext
    public void setNiFiProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    private InetSocketAddress getServiceAddress() {
        final DiscoverableService service = serviceLocator.getService();
        if (service != null) {
            return service.getServiceAddress();
        }
        return null;
    }

    private boolean isException(final Class<? extends Exception> exception, final ProtocolMessage protocolMessage) {
        if (protocolMessage.wasException()) {
            return exception.getName().equals(protocolMessage.getExceptionClass());
        } else {
            return false;
        }
    }
}
