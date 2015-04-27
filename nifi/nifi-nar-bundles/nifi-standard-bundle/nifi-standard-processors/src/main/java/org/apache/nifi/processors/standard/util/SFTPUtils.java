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
import java.util.ArrayList;
import java.util.Deque;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;

public class SFTPUtils {

    public static final PropertyDescriptor SFTP_PRIVATEKEY_PATH = new PropertyDescriptor.Builder()
            .required(false)
            .description("sftp.privatekey.path")
            .defaultValue(null)
            .name("sftp.privatekey.path")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor REMOTE_PASSWORD = new PropertyDescriptor.Builder()
            .required(false)
            .description("remote.password")
            .defaultValue(null)
            .name("remote.password")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor SFTP_PRIVATEKEY_PASSPHRASE = new PropertyDescriptor.Builder()
            .required(false)
            .description("sftp.privatekey.passphrase")
            .defaultValue(null)
            .name("sftp.privatekey.passphrase")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();
    public static final PropertyDescriptor SFTP_PORT = new PropertyDescriptor.Builder()
            .required(false)
            .description("sftp.port")
            .defaultValue(null)
            .name("sftp.port")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor NETWORK_DATA_TIMEOUT = new PropertyDescriptor.Builder()
            .required(false)
            .description("network.data.timeout")
            .defaultValue(null)
            .name("network.data.timeout")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor SFTP_HOSTKEY_FILENAME = new PropertyDescriptor.Builder()
            .required(false)
            .description("sftp.hostkey.filename")
            .defaultValue(null)
            .name("sftp.hostkey.filename")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor NETWORK_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .required(false)
            .description("network.connection.timeout")
            .defaultValue(null)
            .name("network.connection.timeout")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    // required properties
    public static final PropertyDescriptor REMOTE_HOSTNAME = new PropertyDescriptor.Builder()
            .required(true)
            .description("remote.hostname")
            .defaultValue(null)
            .name("remote.hostname")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();
    public static final PropertyDescriptor REMOTE_USERNAME = new PropertyDescriptor.Builder()
            .required(true)
            .description("remote.username")
            .defaultValue(null)
            .name("remote.username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> propertyDescriptors = new ArrayList<>();

    static {
        JSch.setLogger(SFTPUtils.createLogger());
        propertyDescriptors.add(SFTP_PRIVATEKEY_PATH);
        propertyDescriptors.add(REMOTE_PASSWORD);
        propertyDescriptors.add(SFTP_PRIVATEKEY_PASSPHRASE);
        propertyDescriptors.add(SFTP_PORT);
        propertyDescriptors.add(NETWORK_DATA_TIMEOUT);
        propertyDescriptors.add(SFTP_HOSTKEY_FILENAME);
        propertyDescriptors.add(NETWORK_CONNECTION_TIMEOUT);
        propertyDescriptors.add(REMOTE_USERNAME);
        propertyDescriptors.add(REMOTE_HOSTNAME);
    }

    private static final Log logger = LogFactory.getLog(SFTPUtils.class);

    public static List<PropertyDescriptor> getPropertyDescriptors() {
        return propertyDescriptors;
    }

    public static SFTPConnection connectSftp(final SFTPConfiguration conf) throws JSchException, SftpException, IOException {
        final JSch jsch = new JSch();
        final Session session = SFTPUtils.createSession(conf, jsch);
        final ChannelSftp sftp = (ChannelSftp) session.openChannel("sftp");
        sftp.connect();
        return new SFTPConnection(session, sftp);
    }

    public static void changeWorkingDirectory(final ChannelSftp sftp, final String dirPath, final boolean createDirs, final Processor proc) throws IOException {
        final Deque<String> stack = new LinkedList<>();
        File dir = new File(dirPath);
        String currentWorkingDirectory = null;
        boolean dirExists = false;
        final String forwardPaths = dir.getPath().replaceAll(Matcher.quoteReplacement("\\"), Matcher.quoteReplacement("/"));
        try {
            currentWorkingDirectory = sftp.pwd();
            logger.debug(proc + " attempting to change directory from " + currentWorkingDirectory + " to " + dir.getPath());
            //always use forward paths for long string attempt
            sftp.cd(forwardPaths);
            dirExists = true;
            logger.debug(proc + " changed working directory to '" + forwardPaths + "' from '" + currentWorkingDirectory + "'");
        } catch (final SftpException sftpe) {
            logger.debug(proc + " could not change directory to '" + forwardPaths + "' from '" + currentWorkingDirectory + "' so trying the hard way.");
        }
        if (dirExists) {
            return;
        }
        if (!createDirs) {
            throw new IOException("Unable to change to requested working directory \'" + forwardPaths + "\' but not configured to create dirs.");
        }

        do {
            stack.push(dir.getName());
        } while ((dir = dir.getParentFile()) != null);

        String dirName = null;
        while ((dirName = stack.peek()) != null) {
            stack.pop();
            //find out if exists, if not make it if configured to do so or throw exception
            dirName = ("".equals(dirName.trim())) ? "/" : dirName;
            try {
                sftp.cd(dirName);
            } catch (final SftpException sftpe) {
                logger.debug(proc + " creating new directory and changing to it " + dirName);
                try {
                    sftp.mkdir(dirName);
                    sftp.cd(dirName);
                } catch (final SftpException e) {
                    throw new IOException(proc + " could not make/change directory to [" + dirName + "] [" + e.getLocalizedMessage() + "]", e);
                }
            }
        }
    }

    public static Session createSession(final SFTPConfiguration conf, final JSch jsch) throws JSchException, IOException {
        if (conf == null || null == jsch) {
            throw new NullPointerException();
        }

        final Hashtable<String, String> newOptions = new Hashtable<>();

        Session session = jsch.getSession(conf.username, conf.hostname, conf.port);

        final String hostKeyVal = conf.hostkeyFile;

        if (null != hostKeyVal) {
            try {
                jsch.setKnownHosts(hostKeyVal);
            } catch (final IndexOutOfBoundsException iob) {
                throw new IOException("Unable to establish connection due to bad known hosts key file " + hostKeyVal, iob);
            }
        } else {
            newOptions.put("StrictHostKeyChecking", "no");
            session.setConfig(newOptions);
        }

        final String privateKeyVal = conf.privatekeyFile;
        if (null != privateKeyVal) {
            jsch.addIdentity(privateKeyVal, conf.privateKeypassphrase);
        }

        if (null != conf.password) {
            session.setPassword(conf.password);
        }

        session.setTimeout(conf.connectionTimeout); //set timeout for connection
        session.connect();
        session.setTimeout(conf.dataTimeout); //set timeout for data transfer

        return session;
    }

    public static com.jcraft.jsch.Logger createLogger() {

        return new com.jcraft.jsch.Logger() {

            @Override
            public boolean isEnabled(int level) {
                return true;
            }

            @Override
            public void log(int level, String message) {
                logger.debug("SFTP Log: " + message);
            }
        };
    }

    public static class SFTPConfiguration {

        private String hostname;
        private String username;
        private int port = 22;
        private int connectionTimeout = 0;
        private int dataTimeout = 0;
        private String hostkeyFile;
        private String privatekeyFile;
        private String password;
        private String privateKeypassphrase;

        public SFTPConfiguration() {
        }

        public void setHostname(final String val) {
            this.hostname = val;
        }

        public String getHostname() {
            return hostname;
        }

        public void setUsername(final String val) {
            this.username = val;
        }

        public String getUsername() {
            return username;
        }

        public void setPort(final String val) {
            if (val != null) {
                port = Integer.parseInt(val);
            }
        }

        public void setConnectionTimeout(final String val) {
            if (val != null) {
                connectionTimeout = Integer.parseInt(val);
            }
        }

        public void setDataTimeout(final String val) {
            if (val != null) {
                dataTimeout = Integer.parseInt(val);
            }
        }

        public void setHostkeyFile(final String val) {
            this.hostkeyFile = val;
        }

        public void setPrivateKeyFile(final String val) {
            this.privatekeyFile = val;
        }

        public void setPassword(final String val) {
            this.password = val;
        }

        public void setPrivateKeyPassphrase(final String val) {
            this.privateKeypassphrase = val;
        }
    }
}
