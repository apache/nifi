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
package org.apache.nifi.nar.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.nar.NarAutoLoaderContext;
import org.apache.nifi.nar.NarAutoLoaderExternalSource;
import org.apache.nifi.nar.hadoop.util.ExtensionFilter;
import org.apache.nifi.processors.hadoop.ExtendedConfiguration;
import org.apache.nifi.processors.hadoop.HdfsResources;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class HDFSNarAutoLoaderExternalSource implements NarAutoLoaderExternalSource {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSNarAutoLoaderExternalSource.class);
    private static final String RESOURCES_PROPERTY = "nifi.library.nar.autoload.hdfs.resources";
    private static final String SOURCE_DIRECTORY_PROPERTY = "nifi.library.nar.autoload.hdfs.source.directory";

    private static final String KERBEROS_PRINCIPAL_PROPERTY = "nifi.library.nar.autoload.hdfs.kerberos.principal";
    private static final String KERBEROS_KEYTAB_PROPERTY = "nifi.library.nar.autoload.hdfs.kerberos.keytab";
    private static final String KERBEROS_PASSWORD_PROPERTY = "nifi.library.nar.autoload.hdfs.kerberos.password";

    private static final String NAR_EXTENSION = "nar";
    private static final int BUFFER_SIZE_DEFAULT = 4096;
    private static final Object RESOURCES_LOCK = new Object();

    private volatile boolean started = false;
    private volatile List<String> resources = null;
    private volatile Path sourceDirectory = null;

    private volatile NarAutoLoaderContext context;

    public void start(final NarAutoLoaderContext context) {
        resources = Arrays.stream(Objects.requireNonNull(context.getParameter(RESOURCES_PROPERTY)).split(",")).map(s -> s.trim()).collect(Collectors.toList());

        if (resources.isEmpty()) {
            throw new IllegalArgumentException("At least one HDFS configuration resource is necessary");
        }

        sourceDirectory = new Path(Objects.requireNonNull(context.getParameter(SOURCE_DIRECTORY_PROPERTY)));
        this.context = context;
        started = true;
    }

    @Override
    public void stop() {
        started = false;
    }

    @Override
    public void acquire() {
        if (!started) {
            LOGGER.error("External source is not started");
        }

        try {
            final HdfsResources hdfsResources = getHdfsResources();
            final FileStatus[] fileStatuses = hdfsResources.getFileSystem().listStatus(sourceDirectory, new ExtensionFilter(NAR_EXTENSION));
            final Set<String> loadedNars = getLoadedNars();

            Arrays.stream(fileStatuses)
                .filter(fileStatus -> fileStatus.isFile())
                .filter(fileStatus -> !loadedNars.contains(fileStatus.getPath().getName()))
                .forEach(fileStatus -> acquireFile(fileStatus, hdfsResources));
        } catch (final Throwable e) {
            LOGGER.error("Issue happened during acquiring NAR files from external source", e);
        }
    }

    private Set<String> getLoadedNars() {
        return Arrays.stream(context.getAutoLoadDirectory().listFiles(file -> file.isFile() && file.getName().toLowerCase().endsWith("." + NAR_EXTENSION)))
            .map(file -> file.getName())
            .collect(Collectors.toSet());
    }

    private void acquireFile(final FileStatus fileStatus, final HdfsResources hdfsResources) {
        final FSDataInputStream inputStream;
        try {
            inputStream = hdfsResources.getUserGroupInformation()
                    .doAs((PrivilegedExceptionAction<FSDataInputStream>) () -> hdfsResources.getFileSystem().open(fileStatus.getPath(), BUFFER_SIZE_DEFAULT));
            final String targetFile = getTargetFile(fileStatus);
            Files.copy(inputStream, new File(targetFile).toPath(), StandardCopyOption.REPLACE_EXISTING);
            LOGGER.info("File " + fileStatus.getPath().getName() + " acquired successfully");
        } catch (Throwable e) {
            LOGGER.error("Error during acquiring file", e);
            e.printStackTrace();
        }
    }

    private String getTargetFile(final FileStatus fileStatus) {
        String targetDirectoryPath = context.getAutoLoadDirectory().getAbsolutePath();

        if (!targetDirectoryPath.endsWith(FileSystems.getDefault().getSeparator())) {
            targetDirectoryPath += FileSystems.getDefault().getSeparator();
        }

        return targetDirectoryPath + fileStatus.getPath().getName();
    }

    private HdfsResources getHdfsResources() throws IOException {
        final Configuration config = new ExtendedConfiguration(LOGGER);
        config.setClassLoader(this.getClass().getClassLoader());

        for (final String resource : resources) {
            config.addResource(new Path(resource));
        }

        // first check for timeout on HDFS connection, because FileSystem has a hard coded 15 minute timeout
        checkHdfsUriForTimeout(config);

        // disable caching of Configuration and FileSystem objects, else we cannot reconfigure the processor without a complete restart
        final String disableCacheName = String.format("fs.%s.impl.disable.cache", FileSystem.getDefaultUri(config).getScheme());
        config.set(disableCacheName, "true");

        // If kerberos is enabled, create the file system as the kerberos principal
        // -- use RESOURCE_LOCK to guarantee UserGroupInformation is accessed by only a single thread at at time
        FileSystem fs;
        UserGroupInformation ugi;
        KerberosUser kerberosUser;

        synchronized (RESOURCES_LOCK) {
            if (SecurityUtil.isSecurityEnabled(config)) {
                final String principal = context.getParameter(KERBEROS_PRINCIPAL_PROPERTY);
                final String keyTab = context.getParameter(KERBEROS_KEYTAB_PROPERTY);
                final String password = context.getParameter(KERBEROS_PASSWORD_PROPERTY); // TODO

                if (keyTab != null) {
                    kerberosUser = new KerberosKeytabUser(principal, keyTab);
                } else if (password != null) {
                    kerberosUser = new KerberosPasswordUser(principal, password);
                } else {
                    throw new IOException("Unable to authenticate with Kerberos, no keytab or password was provided");
                }

                ugi = SecurityUtil.getUgiForKerberosUser(config, kerberosUser);
            } else {
                config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
                config.set("hadoop.security.authentication", "simple");
                ugi = SecurityUtil.loginSimple(config);
                kerberosUser = null;
            }

            fs = getFileSystemAsUser(config, ugi);
        }
        LOGGER.debug("resetHDFSResources UGI [{}], KerberosUser [{}]", new Object[]{ugi, kerberosUser});

        final Path workingDir = fs.getWorkingDirectory();
        LOGGER.debug("Initialized a new HDFS File System with working dir: {} default block size: {} default replication: {} config: {}",
                new Object[]{workingDir, fs.getDefaultBlockSize(workingDir), fs.getDefaultReplication(workingDir), config.toString()});

        if (!fs.exists(sourceDirectory)) {
            throw new IllegalArgumentException("Source directory is not existing");
        }

        return new HdfsResources(config, fs, ugi, kerberosUser);
    }

    private void checkHdfsUriForTimeout(final Configuration config) throws IOException {
        final URI hdfsUri = FileSystem.getDefaultUri(config);
        final String address = hdfsUri.getAuthority();
        final int port = hdfsUri.getPort();

        if (address == null || address.isEmpty() || port < 0) {
            return;
        }

        final InetSocketAddress namenode = NetUtils.createSocketAddr(address, port);
        final SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(config);
        Socket socket = null;

        try {
            socket = socketFactory.createSocket();
            NetUtils.connect(socket, namenode, 1000); // 1 second timeout
        } finally {
            IOUtils.closeQuietly(socket);
        }
    }

    private FileSystem getFileSystemAsUser(final Configuration config, final UserGroupInformation ugi) throws IOException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws Exception {
                    return FileSystem.get(config);
                }
            });
        } catch (final InterruptedException e) {
            throw new IOException("Unable to create file system: " + e.getMessage(), e);
        }
    }
}
