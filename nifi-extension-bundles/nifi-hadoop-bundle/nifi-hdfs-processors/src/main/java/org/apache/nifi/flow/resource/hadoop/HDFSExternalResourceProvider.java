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
package org.apache.nifi.flow.resource.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.flow.resource.ExternalResourceDescriptor;
import org.apache.nifi.flow.resource.ExternalResourceProvider;
import org.apache.nifi.flow.resource.ExternalResourceProviderInitializationContext;
import org.apache.nifi.flow.resource.ImmutableExternalResourceDescriptor;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processors.hadoop.ExtendedConfiguration;
import org.apache.nifi.processors.hadoop.HDFSResourceHelper;
import org.apache.nifi.processors.hadoop.HdfsResources;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// Implementation considerations: The public methods are considered as steps orchestrated by clients. As of this, there is no direct dependency
// or connection between {@code listResources} and {@code fetchExternalResource}: both are self-sufficing actions. As of this they do not share
// a {@code FileSystem} instance but every method is responsible for collecting and maintaining one. This comes with a minimal overhead but due to
// the nature of the service the method calls are relatively rare. Alternatively a provider could have a FileService instance maintained during its
// lifecycle but that is considered a more error-prone approach as it comes with logic regularly checking for the state of the maintained instance.
@RequiresInstanceClassLoading(cloneAncestorResources = true)
public class HDFSExternalResourceProvider implements ExternalResourceProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(HDFSExternalResourceProvider.class);

    private static final String RESOURCES_PARAMETER = "resources";
    private static final String SOURCE_DIRECTORY_PARAMETER = "source.directory";
    private static final String STORAGE_LOCATION = "storage.location";
    private static final String KERBEROS_PRINCIPAL_PARAMETER = "kerberos.principal";
    private static final String KERBEROS_KEYTAB_PARAMETER = "kerberos.keytab";
    private static final String KERBEROS_PASSWORD_PARAMETER = "kerberos.password";
    private static final Object RESOURCES_LOCK = new Object();
    private static final String STORAGE_LOCATION_PROPERTY = "fs.defaultFS";
    private static final int BUFFER_SIZE_DEFAULT = 4096;
    private static final String DELIMITER = "/";

    private volatile List<String> resources = null;
    private volatile Path sourceDirectory = null;
    private volatile String storageLocation = null;

    private volatile ExternalResourceProviderInitializationContext context;
    private volatile boolean initialized = false;

    @Override
    public void initialize(final ExternalResourceProviderInitializationContext context) {
        resources = Arrays.stream(Objects.requireNonNull(
                context.getProperties().get(RESOURCES_PARAMETER)).split(",")).map(s -> s.trim()).filter(s -> !s.isEmpty()).collect(Collectors.toList());

        if (resources.isEmpty()) {
            throw new IllegalArgumentException("At least one HDFS configuration resource is necessary");
        }

        final String sourceDirectory = context.getProperties().get(SOURCE_DIRECTORY_PARAMETER);

        if (sourceDirectory == null || sourceDirectory.isEmpty()) {
            throw new IllegalArgumentException("Provider needs the source directory to be set");
        }

        this.sourceDirectory = new Path(sourceDirectory);
        this.storageLocation = context.getProperties().get(STORAGE_LOCATION);

        this.context = context;
        this.initialized = true;
    }

    @Override
    public Collection<ExternalResourceDescriptor> listResources() throws IOException {
        if (!initialized) {
            throw new IllegalStateException("Provider is not initialized");
        }

        final HdfsResources hdfsResources = getHdfsResources();

        try {
            final FileStatus[] fileStatuses = hdfsResources.getUserGroupInformation()
                    .doAs((PrivilegedExceptionAction<FileStatus[]>) () -> hdfsResources.getFileSystem().listStatus(sourceDirectory));

            final List<ExternalResourceDescriptor> result = Arrays.stream(fileStatuses)
                    .filter(fileStatus -> fileStatus.isFile())
                    .map(HDFSExternalResourceProvider::convertStatusToDescriptor)
                    .filter(context.getFilter())
                    .collect(Collectors.toList());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("The following NARs were found: {}", String.join(", ", result.stream().map(d -> d.getLocation()).collect(Collectors.toList())));
            }

            return result;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Provider cannot list resources", e);
        } finally {
            HDFSResourceHelper.closeFileSystem(hdfsResources.getFileSystem());
        }
    }

    private static ExternalResourceDescriptor convertStatusToDescriptor(final FileStatus fileStatus) {
        return new ImmutableExternalResourceDescriptor(fileStatus.getPath().getName(), fileStatus.getModificationTime());
    }

    @Override
    public InputStream fetchExternalResource(final ExternalResourceDescriptor descriptor) throws IOException {
        if (!initialized) {
            throw new IllegalStateException("Provider is not initialized");
        }

        final String location = descriptor.getLocation();
        final Path path = getLocation(location);
        final HdfsResources hdfsResources = getHdfsResources();

        try {
            final FSDataInputStream fsDataInputStream =
                    hdfsResources.getUserGroupInformation().doAs((PrivilegedExceptionAction<FSDataInputStream>) () -> {
                        if (!hdfsResources.getFileSystem().exists(path)) {
                            throw new IOException("Cannot find file in HDFS at location " + location);
                        }

                        return hdfsResources.getFileSystem().open(path, BUFFER_SIZE_DEFAULT);
                    });
            // The acquired InputStream is used by the client and for this reason the FileSystem cannot be closed here.
            // The closing of the file system is delegated to the decorator (HDFSResourceInputStream) which will close
            // it when the decorated input stream is closed.
            return new HDFSResourceInputStream(hdfsResources.getFileSystem(), fsDataInputStream);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Error during acquiring file", e);
        }
    }

    private Path getLocation(final String location) {
        String result = sourceDirectory.toString();

        if (!result.endsWith(DELIMITER)) {
            result += DELIMITER;
        }

        return new Path(result + location);
    }

    private HdfsResources getHdfsResources() throws IOException {
        final Configuration config = new ExtendedConfiguration(LOGGER);
        config.setClassLoader(Thread.currentThread().getContextClassLoader());

        for (final String resource : resources) {
            config.addResource(new Path(resource));
        }

        // If storage location property is set, the original location will be overwritten
        if (storageLocation != null) {
            config.set(STORAGE_LOCATION_PROPERTY, storageLocation);
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
                final String principal = context.getProperties().get(KERBEROS_PRINCIPAL_PARAMETER);
                final String keyTab = context.getProperties().get(KERBEROS_KEYTAB_PARAMETER);
                final String password = context.getProperties().get(KERBEROS_PASSWORD_PARAMETER);

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

        LOGGER.debug("resetHDFSResources UGI [{}], KerberosUser [{}]", ugi, kerberosUser);

        final Path workingDir = fs.getWorkingDirectory();
        LOGGER.debug("Initialized a new HDFS File System with working dir: {} default block size: {} default replication: {} config: {}",
                workingDir, fs.getDefaultBlockSize(workingDir), fs.getDefaultReplication(workingDir), config);

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
            return ugi.doAs((PrivilegedExceptionAction<FileSystem>) () -> FileSystem.get(config));
        } catch (final InterruptedException e) {
            throw new IOException("Unable to create file system: " + e.getMessage(), e);
        }
    }
}
