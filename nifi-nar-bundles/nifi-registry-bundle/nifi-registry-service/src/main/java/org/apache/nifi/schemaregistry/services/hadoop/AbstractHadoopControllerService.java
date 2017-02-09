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
package org.apache.nifi.schemaregistry.services.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;

import javax.net.SocketFactory;
import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
/**
 * This is a base class that is helpful when building processors interacting with HDFS.
 */
public abstract class AbstractHadoopControllerService extends AbstractControllerService {

    /**
     * Compression Type Enum
     */
    public enum CompressionType {
        NONE,
        DEFAULT,
        BZIP,
        GZIP,
        LZ4,
        SNAPPY,
        AUTOMATIC;

        @Override
        public String toString() {
            switch (this) {
                case NONE: return "NONE";
                case DEFAULT: return DefaultCodec.class.getName();
                case BZIP: return BZip2Codec.class.getName();
                case GZIP: return GzipCodec.class.getName();
                case LZ4: return Lz4Codec.class.getName();
                case SNAPPY: return SnappyCodec.class.getName();
                case AUTOMATIC: return "Automatically Detected";
            }
            return null;
        }
    }

    public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Resources")
            .description("A file or comma separated list of files which contains the Hadoop file system configuration. Without this, Hadoop "
                    + "will search the classpath for a 'core-site.xml' and 'hdfs-site.xml' file or will revert to a default configuration.")
            .required(false)
            .addValidator(createMultipleFilesExistValidator())
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The HDFS directory from which files should be read")
            .required(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("Compression codec")
            .required(true)
            .allowableValues(CompressionType.values())
            .defaultValue(CompressionType.NONE.toString())
            .build();

    public static final PropertyDescriptor KERBEROS_RELOGIN_PERIOD = new PropertyDescriptor.Builder()
            .name("Kerberos Relogin Period").required(false)
            .description("Period of time which should pass before attempting a kerberos relogin")
            .defaultValue("4 hours")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor ADDITIONAL_CLASSPATH_RESOURCES = new PropertyDescriptor.Builder()
            .name("Additional Classpath Resources")
            .description("A comma-separated list of paths to files and/or directories that will be added to the classpath. When specifying a " +
                    "directory, all files with in the directory will be added to the classpath, but further sub-directories will not be included.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamicallyModifiesClasspath(true)
            .build();

    private static KerberosProperties kerberosProperties;

    private static File kerberosConfigFile;

    private volatile long kerberosReloginThreshold;

    private volatile long lastKerberosReloginTime;

    protected static final List<PropertyDescriptor> DESCRIPTORS;

    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HADOOP_CONFIGURATION_RESOURCES);
        props.add(COMPRESSION_CODEC);
        props.add(DIRECTORY);
        if (kerberosProperties != null) {
            props.add(kerberosProperties.getKerberosPrincipal());
            props.add(kerberosProperties.getKerberosKeytab());
        }
        props.add(KERBEROS_RELOGIN_PERIOD);
        props.add(ADDITIONAL_CLASSPATH_RESOURCES);
        DESCRIPTORS = Collections.unmodifiableList(props);
    }

    private boolean kerberosEnabled;

    protected volatile String schemaDirectory;

    // Hadoop Configuration, Filesystem, and UserGroupInformation (optional)
    private final AtomicReference<HdfsResources> hdfsResources = new AtomicReference<>();

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    private static KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return new KerberosProperties(kerberosConfigFile);
    }

    @OnEnabled
    public void enable(ConfigurationContext context) throws InitializationException {
        kerberosReloginThreshold = context.getProperty(KERBEROS_RELOGIN_PERIOD).asTimePeriod(TimeUnit.SECONDS);

        schemaDirectory = context.getProperty(DIRECTORY).getValue();
        if (!schemaDirectory.endsWith("/")) {
            schemaDirectory += "/";
        }
        try {
            String configResources = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).getValue();
            hdfsResources.set(setupHDFSResources(configResources, context));

        } catch (IOException ex) {
            getLogger().error("HDFS Configuration error - {}", new Object[] { ex });
            hdfsResources.set(new HdfsResources(null, null, null));
            throw new InitializationException(ex);
        }
    }

    @OnDisabled
    public void close() throws Exception {
        hdfsResources.set(new HdfsResources(null, null, null));
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    protected void init(ControllerServiceInitializationContext context) {
        hdfsResources.set(new HdfsResources(null, null, null));
        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosProperties = getKerberosProperties(kerberosConfigFile);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        String configResources = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).getValue();
        String principal = validationContext.getProperty(kerberosProperties.getKerberosPrincipal()).getValue();
        String keytab = validationContext.getProperty(kerberosProperties.getKerberosKeytab()).getValue();

        List<ValidationResult> results = new ArrayList<>();

        if (!StringUtils.isBlank(configResources)) {
            try {
                ValidationResources resources = validationResourceHolder.get();
                // if no resources in the holder, or if the holder has different resources loaded,
                // then load the Configuration and set the new resources in the holder
                if (resources == null || !configResources.equals(resources.getConfigResources())) {
                    getLogger().debug("Reloading validation resources");
                    resources = new ValidationResources(configResources, getConfigurationFromResources(configResources));
                    validationResourceHolder.set(resources);
                }

                Configuration conf = resources.getConfiguration();
                results.addAll(KerberosProperties.validatePrincipalAndKeytab(this.getClass().getSimpleName(), conf, principal, keytab, getLogger()));
            } catch (IOException e) {
                results.add(new ValidationResult.Builder()
                        .valid(false)
                        .subject(this.getClass().getSimpleName())
                        .explanation("Could not load Hadoop Configuration resources")
                        .build());
            }
        }

        return results;
    }

    /**
     * This exists in order to allow unit tests to override it so that they
     * don't take several minutes waiting for UDP packets to be received
     *
     * @param config
     *            the configuration to use
     * @return the FileSystem that is created for the given Configuration
     * @throws IOException
     *             if unable to create the FileSystem
     */
    protected FileSystem getFileSystem(Configuration config) throws IOException {
        return FileSystem.get(config);
    }

    /**
     *
     */
    FileSystem getFileSystem() {
        // trigger Relogin if necessary
        getUserGroupInformation();
        return hdfsResources.get().getFileSystem();
    }

    /**
     * Setup Hadoop Configuration and FileSystem based on the supplied
     * configuration resources.
     */
    HdfsResources setupHDFSResources(String configResources, ConfigurationContext context) throws IOException {
        Configuration config = getConfigurationFromResources(configResources);
        config.setClassLoader(Thread.currentThread().getContextClassLoader()); // set the InstanceClassLoader

        // first check for timeout on HDFS connection, because FileSystem has a
        // hard coded 15 minute timeout
        checkHdfsUriForTimeout(config);

        // If kerberos is enabled, create the file system as the kerberos principal
        FileSystem fs;
        UserGroupInformation ugi;
        if (SecurityUtil.isSecurityEnabled(config)) {
            String principal = context.getProperty(kerberosProperties.getKerberosPrincipal()).getValue();
            String keyTab = context.getProperty(kerberosProperties.getKerberosKeytab()).getValue();
            ugi = SecurityUtil.loginKerberos(config, principal, keyTab);
            fs = getFileSystemAsUser(config, ugi);
            lastKerberosReloginTime = System.currentTimeMillis() / 1000;
            kerberosEnabled = true;
        } else {
            config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
            config.set("hadoop.security.authentication", "simple");
            ugi = SecurityUtil.loginSimple(config);
            fs = getFileSystemAsUser(config, ugi);
        }

        Path workingDir = fs.getWorkingDirectory();
        getLogger().info("Initialized HDFS File System with working dir: {} default block size: {} default replication: {} config: {}",
                new Object[] { workingDir, fs.getDefaultBlockSize(workingDir), fs.getDefaultReplication(workingDir), config.toString() });

        return new HdfsResources(config, fs, ugi);
    }

    /**
     *
     */
    private static Configuration getConfigurationFromResources(String configResources) throws IOException {
        boolean foundResources = false;
        Configuration config = new ExtendedConfiguration();
        if (null != configResources) {
            String[] resources = configResources.split(",");
            for (String resource : resources) {
                config.addResource(new Path(resource.trim()));
                foundResources = true;
            }
        }

        if (!foundResources) {
            // check that at least 1 non-default resource is available on the classpath
            String configStr = config.toString();
            for (String resource : configStr.substring(configStr.indexOf(":") + 1).split(",")) {
                if (!resource.contains("default") && config.getResource(resource.trim()) != null) {
                    foundResources = true;
                    break;
                }
            }
        }

        if (!foundResources) {
            throw new IOException("Could not find any of the " + HADOOP_CONFIGURATION_RESOURCES.getName() + " on the classpath");
        }
        return config;
    }

    /**
     *
     */
    private FileSystem getFileSystemAsUser(final Configuration config, UserGroupInformation ugi) throws IOException {
        try {
            return ugi.doAs(new PrivilegedExceptionAction<FileSystem>() {
                @Override
                public FileSystem run() throws Exception {
                    return FileSystem.get(config);
                }
            });
        } catch (InterruptedException e) {
            throw new IOException("Unable to create file system: " + e.getMessage());
        }
    }

    /*
     * Drastically reduce the timeout of a socket connection from the default in FileSystem.get()
     */
    private void checkHdfsUriForTimeout(Configuration config) throws IOException {
        URI hdfsUri = FileSystem.getDefaultUri(config);
        String address = hdfsUri.getAuthority();
        int port = hdfsUri.getPort();
        if (address == null || address.isEmpty() || port < 0) {
            return;
        }
        InetSocketAddress namenode = NetUtils.createSocketAddr(address, port);
        SocketFactory socketFactory = NetUtils.getDefaultSocketFactory(config);
        Socket socket = null;
        try {
            socket = socketFactory.createSocket();
            NetUtils.connect(socket, namenode, 1000); // 1 second timeout
        } finally {
            IOUtils.closeQuietly(socket);
        }
    }

    /*
     * Validates that one or more files exist, as specified in a single property.
     */
    private static Validator createMultipleFilesExistValidator() {
        return new Validator() {

            @Override
            public ValidationResult validate(String subject, String input, ValidationContext context) {
                final String[] files = input.split(",");
                for (String filename : files) {
                    try {
                        final File file = new File(filename.trim());
                        final boolean valid = file.exists() && file.isFile();
                        if (!valid) {
                            final String message = "File " + file + " does not exist or is not a file";
                            return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                        }
                    } catch (SecurityException e) {
                        final String message = "Unable to access " + filename + " due to " + e.getMessage();
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                    }
                }
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }

        };
    }

    /**
     *
     */
    private UserGroupInformation getUserGroupInformation() {
        // if kerberos is enabled, check if the ticket should be renewed before returning
        UserGroupInformation userGroupInformation = hdfsResources.get().getUserGroupInformation();
        if (userGroupInformation != null && (kerberosEnabled && isTicketOld())) {
            tryKerberosRelogin(userGroupInformation);
        }
        return userGroupInformation;
    }

    /**
     *
     */
    private void tryKerberosRelogin(UserGroupInformation ugi) {
        try {
            getLogger().info("Kerberos ticket age exceeds threshold [{} seconds] " +
                    "attempting to renew ticket for user {}", new Object[]{
                    kerberosReloginThreshold, ugi.getUserName()});
            ugi.checkTGTAndReloginFromKeytab();
            lastKerberosReloginTime = System.currentTimeMillis() / 1000;
            getLogger().info("Kerberos relogin successful or ticket still valid");
        } catch (IOException e) {
            // Most likely case of this happening is ticket is expired and error getting a new one,
            // meaning dfs operations would fail
            getLogger().error("Kerberos relogin failed", e);
            throw new ProcessException("Unable to renew kerberos ticket", e);
        }
    }

    /**
     *
     */
    private boolean isTicketOld() {
        return (System.currentTimeMillis() / 1000 - lastKerberosReloginTime) > kerberosReloginThreshold;
    }

    /**
     *
     */
    static protected class HdfsResources {
        private final Configuration configuration;
        private final FileSystem fileSystem;
        private final UserGroupInformation userGroupInformation;

        HdfsResources(Configuration configuration, FileSystem fileSystem, UserGroupInformation userGroupInformation) {
            this.configuration = configuration;
            this.fileSystem = fileSystem;
            this.userGroupInformation = userGroupInformation;
        }

        Configuration getConfiguration() {
            return configuration;
        }

        FileSystem getFileSystem() {
            return fileSystem;
        }

        UserGroupInformation getUserGroupInformation() {
            return userGroupInformation;
        }
    }

    /**
     *
     */
    static protected class ValidationResources {
        private final String configResources;
        private final Configuration configuration;

        ValidationResources(String configResources, Configuration configuration) {
            this.configResources = configResources;
            this.configuration = configuration;
        }

        String getConfigResources() {
            return configResources;
        }

        Configuration getConfiguration() {
            return configuration;
        }
    }

    /**
     * Extending Hadoop Configuration to prevent it from caching classes that can't be found. Since users may be
     * adding additional JARs to the classpath we don't want them to have to restart the JVM to be able to load
     * something that was previously not found, but might now be available.
     * <p>
     * Reference the original getClassByNameOrNull from Configuration.
     */
    static class ExtendedConfiguration extends Configuration {

        private final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<>();

        @Override
        public Class<?> getClassByNameOrNull(String name) {
            Map<String, WeakReference<Class<?>>> map;

            synchronized (CACHE_CLASSES) {
                map = CACHE_CLASSES.computeIfAbsent(getClassLoader(), k -> Collections.synchronizedMap(new WeakHashMap<>()));
            }

            Class<?> clazz = null;
            WeakReference<Class<?>> ref = map.get(name);
            if (ref != null) {
                clazz = ref.get();
            }

            if (clazz == null) {
                try {
                    clazz = Class.forName(name, true, getClassLoader());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                    return null;
                }
                // two putters can race here, but they'll put the same class
                map.put(name, new WeakReference<>(clazz));
                return clazz;
            } else {
                // cache hit
                return clazz;
            }
        }
    }
}
