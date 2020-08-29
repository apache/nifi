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
import org.apache.ftpserver.FtpServer;
import org.apache.ftpserver.FtpServerConfigurationException;
import org.apache.ftpserver.FtpServerFactory;
import org.apache.ftpserver.command.Command;
import org.apache.ftpserver.command.CommandFactory;
import org.apache.ftpserver.command.CommandFactoryFactory;
import org.apache.ftpserver.command.impl.ABOR;
import org.apache.ftpserver.command.impl.AUTH;
import org.apache.ftpserver.command.impl.CDUP;
import org.apache.ftpserver.command.impl.CWD;
import org.apache.ftpserver.command.impl.EPRT;
import org.apache.ftpserver.command.impl.EPSV;
import org.apache.ftpserver.command.impl.FEAT;
import org.apache.ftpserver.command.impl.LIST;
import org.apache.ftpserver.command.impl.MDTM;
import org.apache.ftpserver.command.impl.MKD;
import org.apache.ftpserver.command.impl.MLSD;
import org.apache.ftpserver.command.impl.MLST;
import org.apache.ftpserver.command.impl.MODE;
import org.apache.ftpserver.command.impl.NLST;
import org.apache.ftpserver.command.impl.NOOP;
import org.apache.ftpserver.command.impl.OPTS;
import org.apache.ftpserver.command.impl.PASS;
import org.apache.ftpserver.command.impl.PASV;
import org.apache.ftpserver.command.impl.PBSZ;
import org.apache.ftpserver.command.impl.PORT;
import org.apache.ftpserver.command.impl.PROT;
import org.apache.ftpserver.command.impl.PWD;
import org.apache.ftpserver.command.impl.QUIT;
import org.apache.ftpserver.command.impl.REIN;
import org.apache.ftpserver.command.impl.RMD;
import org.apache.ftpserver.command.impl.SITE;
import org.apache.ftpserver.command.impl.SITE_DESCUSER;
import org.apache.ftpserver.command.impl.SITE_HELP;
import org.apache.ftpserver.command.impl.SITE_STAT;
import org.apache.ftpserver.command.impl.SITE_WHO;
import org.apache.ftpserver.command.impl.SITE_ZONE;
import org.apache.ftpserver.command.impl.SIZE;
import org.apache.ftpserver.command.impl.STAT;
import org.apache.ftpserver.command.impl.STRU;
import org.apache.ftpserver.command.impl.SYST;
import org.apache.ftpserver.command.impl.TYPE;
import org.apache.ftpserver.command.impl.USER;
import org.apache.ftpserver.ftplet.Authority;
import org.apache.ftpserver.ftplet.User;
import org.apache.ftpserver.listener.Listener;
import org.apache.ftpserver.listener.ListenerFactory;
import org.apache.ftpserver.ssl.SslConfigurationFactory;
import org.apache.ftpserver.usermanager.impl.BaseUser;
import org.apache.ftpserver.usermanager.impl.WritePermission;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.ftp.commands.FtpCommandHELP;
import org.apache.nifi.processors.standard.ftp.commands.FtpCommandSTOR;
import org.apache.nifi.processors.standard.ftp.commands.NotSupportedCommand;
import org.apache.nifi.processors.standard.ftp.filesystem.DefaultVirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystem;
import org.apache.nifi.processors.standard.ftp.filesystem.VirtualFileSystemFactory;
import org.apache.nifi.ssl.SSLContextService;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public class NifiFtpServer {

    private final Map<String, Command> commandMap = new HashMap<>();
    private final FtpCommandHELP customHelpCommand = new FtpCommandHELP();

    private final FtpServer server;
    private static final String HOME_DIRECTORY = "/virtual/ftproot";

    private NifiFtpServer(Builder builder) throws ProcessException {
        try {
            initializeCommandMap(builder.sessionFactory, builder.sessionFactorySetSignal);

            VirtualFileSystem fileSystem = new DefaultVirtualFileSystem();
            boolean anonymousLoginEnabled = (builder.username == null);

            FtpServerFactory serverFactory = new FtpServerFactory();
            serverFactory.setFileSystem(new VirtualFileSystemFactory(fileSystem));
            serverFactory.setCommandFactory(createCommandFactory(commandMap));

            serverFactory.setConnectionConfig(createConnectionConfig(anonymousLoginEnabled));
            serverFactory.addListener("default", createListener(builder.bindAddress, builder.port, builder.sslContextService));
            if (anonymousLoginEnabled) {
                serverFactory.getUserManager().save(createAnonymousUser(HOME_DIRECTORY, Collections.singletonList(new WritePermission())));
            } else {
                serverFactory.getUserManager().save(createUser(builder.username, builder.password, HOME_DIRECTORY, Collections.singletonList(new WritePermission())));
            }
            server = serverFactory.createServer();
        } catch (Exception exception) {
            throw new ProcessException("FTP server could not be started.", exception);
        }
    }

    public void start() throws ProcessException {
        try {
            server.start();
        } catch (Exception exception) {
            throw new ProcessException("FTP server could not be started.", exception);
        }
    }

    public void stop() {
        server.stop();
    }

    public boolean isStopped() {
        return server.isStopped();
    }

    private CommandFactory createCommandFactory(Map<String, Command> commandMap) {
        CommandFactoryFactory commandFactoryFactory = new CommandFactoryFactory();
        commandFactoryFactory.setUseDefaultCommands(false);
        commandFactoryFactory.setCommandMap(commandMap);
        return commandFactoryFactory.createCommandFactory();
    }

    private ConnectionConfig createConnectionConfig(boolean anonymousLoginEnabled) {
        ConnectionConfigFactory connectionConfigFactory = new ConnectionConfigFactory();
        connectionConfigFactory.setAnonymousLoginEnabled(anonymousLoginEnabled);
        return connectionConfigFactory.createConnectionConfig();
    }

    private Listener createListener(String bindAddress, int port, SSLContextService sslContextService) throws FtpServerConfigurationException {
        ListenerFactory listenerFactory = new ListenerFactory();
        listenerFactory.setServerAddress(bindAddress);
        listenerFactory.setPort(port);
        if (sslContextService != null) {
            SslConfigurationFactory ssl = new SslConfigurationFactory();
            ssl.setKeystoreFile(new File(sslContextService.getKeyStoreFile()));
            ssl.setKeystorePassword(sslContextService.getKeyStorePassword());
            ssl.setKeyPassword(sslContextService.getKeyPassword());
            ssl.setKeystoreType(sslContextService.getKeyStoreType());
            ssl.setSslProtocol(sslContextService.getSslAlgorithm());

            if (sslContextService.getTrustStoreFile() != null){
                ssl.setTruststoreFile(new File(sslContextService.getTrustStoreFile()));
                ssl.setTruststorePassword(sslContextService.getTrustStorePassword());
                ssl.setTruststoreType(sslContextService.getTrustStoreType());
            }

            listenerFactory.setSslConfiguration(ssl.createSslConfiguration());
            listenerFactory.setImplicitSsl(true);
        }
        return listenerFactory.createListener();
    }

    private User createUser(String username, String password, String homeDirectory, List<Authority> authorities) {
        BaseUser user = new BaseUser();
        user.setName(username);
        user.setPassword(password);
        user.setHomeDirectory(homeDirectory);
        user.setAuthorities(authorities);
        return user;
    }

    private User createAnonymousUser(String homeDirectory, List<Authority> authorities) {
        BaseUser user = new BaseUser();
        user.setName("anonymous");
        user.setHomeDirectory(homeDirectory);
        user.setAuthorities(authorities);
        return user;
    }

    private void initializeCommandMap(AtomicReference<ProcessSessionFactory> sessionFactory, CountDownLatch sessionFactorySetSignal) {
        addToCommandMap("ABOR", new ABOR());
        addToCommandMap("ACCT", new NotSupportedCommand("Operation (ACCT) not supported."));
        addToCommandMap("APPE", new NotSupportedCommand("Operation (APPE) not supported."));
        addToCommandMap("AUTH", new AUTH());
        addToCommandMap("CDUP", new CDUP());
        addToCommandMap("CWD", new CWD());
        addToCommandMap("DELE", new NotSupportedCommand("Operation (DELE) not supported."));
        addToCommandMap("EPRT", new EPRT());
        addToCommandMap("EPSV", new EPSV());
        addToCommandMap("FEAT", new FEAT());
        addToCommandMap("HELP", customHelpCommand);
        addToCommandMap("LIST", new LIST());
        addToCommandMap("MFMT", new NotSupportedCommand("Operation (MFMT) not supported."));
        addToCommandMap("MDTM", new MDTM());
        addToCommandMap("MLST", new MLST());
        addToCommandMap("MKD", new MKD());
        addToCommandMap("MLSD", new MLSD());
        addToCommandMap("MODE", new MODE());
        addToCommandMap("NLST", new NLST());
        addToCommandMap("NOOP", new NOOP());
        addToCommandMap("OPTS", new OPTS());
        addToCommandMap("PASS", new PASS());
        addToCommandMap("PASV", new PASV());
        addToCommandMap("PBSZ", new PBSZ());
        addToCommandMap("PORT", new PORT());
        addToCommandMap("PROT", new PROT());
        addToCommandMap("PWD", new PWD());
        addToCommandMap("QUIT", new QUIT());
        addToCommandMap("REIN", new REIN());
        addToCommandMap("REST", new NotSupportedCommand("Operation (REST) not supported."));
        addToCommandMap("RETR", new NotSupportedCommand("Operation (RETR) not supported."));
        addToCommandMap("RMD", new RMD());
        //addToCommandMap("RNFR", new RNFR());
        //addToCommandMap("RNTO", new RNTO());
        addToCommandMap("SITE", new SITE());
        addToCommandMap("SIZE", new SIZE());
        addToCommandMap("SITE_DESCUSER", new SITE_DESCUSER());
        addToCommandMap("SITE_HELP", new SITE_HELP());
        addToCommandMap("SITE_STAT", new SITE_STAT());
        addToCommandMap("SITE_WHO", new SITE_WHO());
        addToCommandMap("SITE_ZONE", new SITE_ZONE());

        addToCommandMap("STAT", new STAT());
        addToCommandMap("STOR", new FtpCommandSTOR(sessionFactory, sessionFactorySetSignal));
        addToCommandMap("STOU", new FtpCommandSTOR(sessionFactory, sessionFactorySetSignal));
        addToCommandMap("STRU", new STRU());
        addToCommandMap("SYST", new SYST());
        addToCommandMap("TYPE", new TYPE());
        addToCommandMap("USER", new USER());
    }

    private void addToCommandMap(String command, Command instance) {
        commandMap.put(command, instance);
        if (!(instance instanceof NotSupportedCommand)) {
            customHelpCommand.addCommand(command);
        }
    }

    public static class Builder {
        private AtomicReference<ProcessSessionFactory> sessionFactory;
        CountDownLatch sessionFactorySetSignal;
        private String bindAddress;
        private int port;
        private String username;
        private String password;
        private SSLContextService sslContextService;

        public Builder sessionFactory(AtomicReference<ProcessSessionFactory> sessionFactory) {
            this.sessionFactory = sessionFactory;
            return this;
        }

        public Builder sessionFactorySetSignal(CountDownLatch sessionFactorySetSignal) {
            Objects.requireNonNull(sessionFactorySetSignal);

            this.sessionFactorySetSignal = sessionFactorySetSignal;
            return this;
        }

        public Builder bindAddress(String bindAddress) {
            this.bindAddress = bindAddress;
            return this;
        }

        public Builder port(int port) {
            this.port = port;
            return this;
        }

        public Builder username(String username) {
            this.username = username;
            return this;
        }

        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public Builder sslContextService(SSLContextService sslContextService) {
            this.sslContextService = sslContextService;
            return this;
        }

        public NifiFtpServer build() throws ProcessException {
            return new NifiFtpServer(this);
        }
    }

}
