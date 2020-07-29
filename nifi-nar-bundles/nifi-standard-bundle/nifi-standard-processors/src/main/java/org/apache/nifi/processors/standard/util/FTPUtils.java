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
package org.apache.nifi.processors.standard.util;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.regex.Matcher;

import org.apache.nifi.processor.Processor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.net.ProtocolCommandEvent;
import org.apache.commons.net.ProtocolCommandListener;
import org.apache.commons.net.ftp.FTPClient;

/**
 *
 */
public class FTPUtils {

    private static final Log logger = LogFactory.getLog(FTPUtils.class);

    public static final String CONNECTION_MODE_KEY = "connection.mode";//optional(active local)
    public static final String ACTIVE_LOCAL_CONNECTION_MODE = "active_local";
    public static final String PASSIVE_LOCAL_CONNECTION_MODE = "passive_local";

    public static final String TRANSFER_MODE_KEY = "transfer.mode";//optional(binary)
    public static final String BINARY_TRANSFER_MODE = "binary";
    public static final String ASCII_TRANSFER_MODE = "ascii";

    public static final String REMOTE_HOST_KEY = "remote.host";//required
    public static final String REMOTE_USER_KEY = "remote.user";//required
    public static final String REMOTE_PASSWORD_KEY = "remote.password";//required
    public static final String REMOTE_PORT_KEY = "remote.port"; //optional (default port)

    public static final String NETWORK_DATA_TIMEOUT_KEY = "network.data.timeout";
    public static final String NETWORK_SOCKET_TIMEOUT_KEY = "network.socket.timeout";

    /**
     * Creates a new FTPClient connected to an FTP server. The following properties must exist:
     * <ul>Required Properties:
     * <li>remote.host - The hostname or IP address of the FTP server to connect to</li>
     * <li>remote.user - The username of the account to authenticate with</li>
     * <li>remote.password = The password for the username to authenticate with</li>
     * </ul>
     * <ul>Optional Properties:
     * <li>remote.port - The port on the FTP server to connect to. Defaults to FTP default.</li>
     * <li>transfer.mode - The type of transfer for this connection ('ascii', 'binary'). Defaults to 'binary'</li>
     * <li>connection.mode - The type of FTP connection to make ('active_local', 'passive_local'). Defaults to 'active_local'. In active_local the server initiates 'data connections' to the client
     * where in passive_local the client initiates 'data connections' to the server.</li>
     * <li>network.data.timeout - Default is 0. Sets the timeout in milliseconds for waiting to establish a new 'data connection' (not a control connection) when in ACTIVE_LOCAL mode. Also, this
     * establishes the amount of time to wait on read calls on the data connection in either mode. A value of zero means do not timeout. Users should probably set a value here unless using very
     * reliable communications links or else risk indefinite hangs that require a restart.</li>
     * <li>network.socket.timeout - Default is 0. Sets the timeout in milliseconds to use when creating a new control channel socket and also a timeout to set when reading from a control socket. A
     * value of zero means do not timeout. Users should probably set a value here unless using very reliable communications links or else risk indefinite hangs that require a restart.</li>
     * </ul>
     *
     * @param conf conf
     * @param monitor if provided will be used to monitor FTP commands processed but may be null
     * @return FTPClient connected to FTP server as configured
     * @throws NullPointerException if either argument is null
     * @throws IllegalArgumentException if a required property is missing
     * @throws NumberFormatException if any argument that must be an int cannot be converted to int
     * @throws IOException if some problem occurs connecting to FTP server
     */
    public static FTPClient connect(final FTPConfiguration conf, final ProtocolCommandListener monitor) throws IOException {
        if (null == conf) {
            throw new NullPointerException();
        }

        final String portVal = conf.port;
        final int portNum = (null == portVal) ? -1 : Integer.parseInt(portVal);
        final String connectionModeVal = conf.connectionMode;
        final String connectionMode = (null == connectionModeVal) ? ACTIVE_LOCAL_CONNECTION_MODE : connectionModeVal;
        final String transferModeVal = conf.transferMode;
        final String transferMode = (null == transferModeVal) ? BINARY_TRANSFER_MODE : transferModeVal;
        final String networkDataTimeoutVal = conf.dataTimeout;
        final int networkDataTimeout = (null == networkDataTimeoutVal) ? 0 : Integer.parseInt(networkDataTimeoutVal);
        final String networkSocketTimeoutVal = conf.connectionTimeout;
        final int networkSocketTimeout = (null == networkSocketTimeoutVal) ? 0 : Integer.parseInt(networkSocketTimeoutVal);

        final FTPClient client = new FTPClient();
        if (networkDataTimeout > 0) {
            client.setDataTimeout(networkDataTimeout);
        }
        if (networkSocketTimeout > 0) {
            client.setDefaultTimeout(networkSocketTimeout);
        }
        client.setRemoteVerificationEnabled(false);
        if (null != monitor) {
            client.addProtocolCommandListener(monitor);
        }
        InetAddress inetAddress = null;
        try {
            inetAddress = InetAddress.getByAddress(conf.remoteHostname, null);
        } catch (final UnknownHostException uhe) {
        }
        if (inetAddress == null) {
            inetAddress = InetAddress.getByName(conf.remoteHostname);
        }

        if (portNum < 0) {
            client.connect(inetAddress);
        } else {
            client.connect(inetAddress, portNum);
        }
        if (networkDataTimeout > 0) {
            client.setDataTimeout(networkDataTimeout);
        }
        if (networkSocketTimeout > 0) {
            client.setSoTimeout(networkSocketTimeout);
        }
        final boolean loggedIn = client.login(conf.username, conf.password);
        if (!loggedIn) {
            throw new IOException("Could not login for user '" + conf.username + "'");
        }
        if (connectionMode.equals(ACTIVE_LOCAL_CONNECTION_MODE)) {
            client.enterLocalActiveMode();
        } else if (connectionMode.equals(PASSIVE_LOCAL_CONNECTION_MODE)) {
            client.enterLocalPassiveMode();
        }
        boolean transferModeSet = false;
        if (transferMode.equals(ASCII_TRANSFER_MODE)) {
            transferModeSet = client.setFileType(FTPClient.ASCII_FILE_TYPE);
        } else {
            transferModeSet = client.setFileType(FTPClient.BINARY_FILE_TYPE);
        }
        if (!transferModeSet) {
            throw new IOException("Unable to set transfer mode to type " + transferMode);
        }

        return client;
    }

    public static class FTPCommandMonitor implements ProtocolCommandListener {

        final Processor processor;

        public FTPCommandMonitor(final Processor _processor) {
            this.processor = _processor;
        }

        @Override
        public void protocolCommandSent(final ProtocolCommandEvent event) {
            if (logger.isDebugEnabled()) {
                logger.debug(processor + " : " + event.getMessage().trim());
            }
        }

        @Override
        public void protocolReplyReceived(final ProtocolCommandEvent event) {
            if (logger.isDebugEnabled()) {
                logger.debug(processor + " : " + event.getMessage().trim());
            }
        }

    }

    /**
     * Handles the logic required to change to the given directory RELATIVE TO THE CURRENT DIRECTORY which can include creating new directories needed.
     *
     * This will first attempt to change to the full path of the given directory outright. If that fails, then it will attempt to change from the top of the tree of the given directory all the way
     * down to the final leaf node of the given directory.
     *
     * @param client - the ftp client with an already active connection
     * @param dirPath - the path to change or create directories to
     * @param createDirs - if true will attempt to create any missing directories
     * @param processor - used solely for targeting logging output.
     * @throws IOException if any access problem occurs
     */
    public static void changeWorkingDirectory(final FTPClient client, final String dirPath, final boolean createDirs, final Processor processor) throws IOException {
        final String currentWorkingDirectory = client.printWorkingDirectory();
        final File dir = new File(dirPath);
        logger.debug(processor + " attempting to change directory from " + currentWorkingDirectory + " to " + dir.getPath());
        boolean dirExists = false;
        final String forwardPaths = dir.getPath().replaceAll(Matcher.quoteReplacement("\\"), Matcher.quoteReplacement("/"));
        //always use forward paths for long string attempt
        try {
            dirExists = client.changeWorkingDirectory(forwardPaths);
            if (dirExists) {
                logger.debug(processor + " changed working directory to '" + forwardPaths + "' from '" + currentWorkingDirectory + "'");
            } else {
                logger.debug(processor + " could not change directory to '" + forwardPaths + "' from '" + currentWorkingDirectory + "' so trying the hard way.");
            }
        } catch (final IOException ioe) {
            logger.debug(processor + " could not change directory to '" + forwardPaths + "' from '" + currentWorkingDirectory + "' so trying the hard way.");
        }
        if (!dirExists) {  //couldn't navigate directly...begin hard work
            final Deque<String> stack = new LinkedList<>();
            File fakeFile = new File(dir.getPath());
            do {
                stack.push(fakeFile.getName());
            } while ((fakeFile = fakeFile.getParentFile()) != null);

            String dirName = null;
            while ((dirName = stack.peek()) != null) {
                stack.pop();
                //find out if exists, if not make it if configured to do so or throw exception
                dirName = ("".equals(dirName.trim())) ? "/" : dirName;
                boolean exists = false;
                try {
                    exists = client.changeWorkingDirectory(dirName);
                } catch (final IOException ioe) {
                    exists = false;
                }
                if (!exists && createDirs) {
                    logger.debug(processor + " creating new directory and changing to it " + dirName);
                    client.makeDirectory(dirName);
                    if (!(client.makeDirectory(dirName) || client.changeWorkingDirectory(dirName))) {
                        throw new IOException(processor + " could not create and change to newly created directory " + dirName);
                    } else {
                        logger.debug(processor + " successfully changed working directory to " + dirName);
                    }
                } else if (!exists) {
                    throw new IOException(processor + " could not change directory to '" + dirName + "' from '" + currentWorkingDirectory + "'");
                }
            }
        }
    }

    public static class FTPConfiguration {

        private String remoteHostname;
        private String username;
        private String port;
        private String connectionTimeout;
        private String dataTimeout;
        private String password;
        private String connectionMode;
        private String transferMode;

        public FTPConfiguration() {

        }

        public void setRemoteHostname(final String val) {
            this.remoteHostname = val;
        }

        public String getRemoteHostname() {
            return remoteHostname;
        }

        public void setUsername(final String val) {
            this.username = val;
        }

        public String getUsername() {
            return username;
        }

        public void setPort(final String val) {
            this.port = val;
        }

        public String getPort() {
            return port;
        }

        public void setConnectionTimeout(final String val) {
            this.connectionTimeout = val;
        }

        public void setDataTimeout(final String val) {
            this.dataTimeout = val;
        }

        public void setPassword(final String val) {
            this.password = val;
        }

        public void setConnectionMode(final String val) {
            this.connectionMode = val;
        }

        public void setTransferMode(final String val) {
            this.transferMode = val;
        }

    }

}
