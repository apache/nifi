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
package org.apache.nifi.processors.standard.ftp;

import org.apache.ftpserver.ConnectionConfig;
import org.apache.ftpserver.ConnectionConfigFactory;
import org.apache.ftpserver.DataConnectionConfiguration;
import org.apache.ftpserver.DataConnectionConfigurationFactory;
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerConfigurationException;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.Command;
import org.apache.ftpserver.command.CommandFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.FileSystemFactory;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.ClientAuth;
import org.apache.ftpserver.ssl.SslConfiguration;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.ftp.commands.CommandMapFactory;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystemFactory;
import org.apache.nifi.ssl.SSLContextProvider;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

public class NifiFtpServer implements org.apache.nifi.processors.standard.ftp.FtpServer {

    private final FtpServer server;

    private NifiFtpServer(final Map<String, Command> commandMap, final FileSystemFactory fileSystemFactory,
            final ConnectionConfig connectionConfig, final Listener listener, final User user) throws ProcessException {
        try {
            final FtpServerFactory serverFactory = new FtpServerFactory();

            serverFactory.setFileSystem(fileSystemFactory);
            serverFactory.setCommandFactory(createCommandFactory(commandMap));
            serverFactory.setConnectionConfig(connectionConfig);
            serverFactory.addListener("default", listener);
            serverFactory.getUserManager().save(user);

            server = serverFactory.createServer();
        } catch (final Exception exception) {
            throw new ProcessException("FTP server could not be started.", exception);
        }
    }

    private CommandFactory createCommandFactory(final Map<String, Command> commandMap) {
        final CommandFactoryFactory commandFactoryFactory = new CommandFactoryFactory();
        commandFactoryFactory.setUseDefaultCommands(false);
        commandFactoryFactory.setCommandMap(commandMap);
        return commandFactoryFactory.createCommandFactory();
    }

    @Override
    public void start() throws ProcessException {
        try {
            server.start();
        } catch (final Exception exception) {
            throw new ProcessException("FTP server could not be started.", exception);
        }
    }

    @Override
    public void stop() {
        server.stop();
    }

    @Override
    public boolean isStopped() {
        return server.isStopped();
    }

    public static class Builder {
        private static final String HOME_DIRECTORY = "/virtual/ftproot";

        private AtomicReference<ProcessSessionFactory> sessionFactory;
        private CountDownLatch sessionFactorySetSignal;
        private Relationship relationshipSuccess;
        private String bindAddress;
        private int port;
        private String username;
        private String password;
        private SSLContextProvider sslContextProvider;

        public Builder sessionFactory(final AtomicReference<ProcessSessionFactory> sessionFactory) {
            this.sessionFactory = sessionFactory;
            return this;
        }

        public Builder sessionFactorySetSignal(final CountDownLatch sessionFactorySetSignal) {
            Objects.requireNonNull(sessionFactorySetSignal);

            this.sessionFactorySetSignal = sessionFactorySetSignal;
            return this;
        }

        public Builder relationshipSuccess(final Relationship relationship) {
            Objects.requireNonNull(relationship);

            this.relationshipSuccess = relationship;
            return this;
        }

        public Builder bindAddress(final String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        public Builder username(final String username) {
            this.username = username;
            return this;
        }

        public Builder password(final String password) {
            this.password = password;
            return this;
        }

        public Builder sslContextProvider(final SSLContextProvider sslContextProvider) {
            this.sslContextProvider = sslContextProvider;
            return this;
        }

        public NifiFtpServer build() throws ProcessException {
            try {
                final boolean anonymousLoginEnabled = (username == null);

                final FileSystemFactory fileSystemFactory = new VirtualFileSystemFactory();
                final CommandMapFactory commandMapFactory = new CommandMapFactory(sessionFactory, sessionFactorySetSignal, relationshipSuccess);
                final Map<String, Command> commandMap = commandMapFactory.createCommandMap();
                final ConnectionConfig connectionConfig = createConnectionConfig(anonymousLoginEnabled);
                final Listener listener = createListener(bindAddress, port, sslContextProvider);
                final User user = createUser(username, password, HOME_DIRECTORY);

                return new NifiFtpServer(commandMap, fileSystemFactory, connectionConfig, listener, user);
            } catch (final Exception exception) {
                throw new ProcessException("FTP server could not be started.", exception);
            }
        }

        private ConnectionConfig createConnectionConfig(final boolean anonymousLoginEnabled) {
            final ConnectionConfigFactory connectionConfigFactory = new ConnectionConfigFactory();
            connectionConfigFactory.setAnonymousLoginEnabled(anonymousLoginEnabled);
            return connectionConfigFactory.createConnectionConfig();
        }

        private Listener createListener(final String bindAddress, final int port, final SSLContextProvider sslContextProvider) throws FtpServerConfigurationException {
            final ListenerFactory listenerFactory = new ListenerFactory();
            listenerFactory.setServerAddress(bindAddress);
            listenerFactory.setPort(port);
            if (sslContextProvider != null) {
                final SSLContext sslContext = sslContextProvider.createContext();
                final SslConfiguration sslConfiguration = new StandardSslConfiguration(sslContext);

                // Set implicit security for the control socket
                listenerFactory.setSslConfiguration(sslConfiguration);
                listenerFactory.setImplicitSsl(true);

                // Set implicit security for the data connection
                final DataConnectionConfigurationFactory dataConnectionConfigurationFactory = new DataConnectionConfigurationFactory();
                dataConnectionConfigurationFactory.setImplicitSsl(true);
                dataConnectionConfigurationFactory.setSslConfiguration(sslConfiguration);
                final DataConnectionConfiguration dataConnectionConfiguration = dataConnectionConfigurationFactory.createDataConnectionConfiguration();
                listenerFactory.setDataConnectionConfiguration(dataConnectionConfiguration);
            }
            return listenerFactory.createListener();
        }

        private User createUser(final String username, final String password, final String homeDirectory) {
            final boolean anonymousLoginEnabled = (username == null);
            if (anonymousLoginEnabled) {
                return createAnonymousUser(homeDirectory, List.of(new WritePermission()));
            } else {
                return createNamedUser(username, password, homeDirectory, List.of(new WritePermission()));
            }
        }

        private User createAnonymousUser(final String homeDirectory, final List<Authority> authorities) {
            final BaseUser user = new BaseUser();
            user.setName("anonymous");
            user.setHomeDirectory(homeDirectory);
            user.setAuthorities(authorities);
            return user;
        }

        private User createNamedUser(final String username, final String password, final String homeDirectory, final List<Authority> authorities) {
            final BaseUser user = new BaseUser();
            user.setName(username);
            user.setPassword(password);
            user.setHomeDirectory(homeDirectory);
            user.setAuthorities(authorities);
            return user;
        }
    }

    private static class StandardSslConfiguration implements SslConfiguration {
        private final SSLContext sslContext;
        private final SSLParameters sslParameters;

        private StandardSslConfiguration(final SSLContext sslContext) {
            this.sslContext = sslContext;
            this.sslParameters = sslContext.getDefaultSSLParameters();
        }

        @Override
        public SSLSocketFactory getSocketFactory() {
            return sslContext.getSocketFactory();
        }

        @Override
        public SSLContext getSSLContext() {
            return sslContext;
        }

        @Override
        public SSLContext getSSLContext(final String enabledProtocol) {
            return sslContext;
        }

        @Override
        public String[] getEnabledCipherSuites() {
            return sslParameters.getCipherSuites();
        }

        @Override
        public String[] getEnabledProtocols() {
            return sslParameters.getProtocols();
        }

        @Override
        public ClientAuth getClientAuth() {
            return ClientAuth.WANT;
        }

        @Override
        public String getEnabledProtocol() {
            return sslContext.getProtocol();
        }
    }
}
