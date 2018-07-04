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
package org.apache.nifi.processors.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SaslPlainServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;

import javax.net.SocketFactory;
import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.security.Security;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This is a base class that is helpful when building processors interacting with HDFS.
 * <p/>
 * As of Apache NiFi 1.5.0, the Relogin Period property is no longer used in the configuration of a Hadoop processor.
 * Due to changes made to {@link SecurityUtil#loginKerberos(Configuration, String, String)}, which is used by this
 * class to authenticate a principal with Kerberos, Hadoop components no longer
 * attempt relogins explicitly.  For more information, please read the documentation for
 * {@link SecurityUtil#loginKerberos(Configuration, String, String)}.
 *
 * @see SecurityUtil#loginKerberos(Configuration, String, String)
 */
@RequiresInstanceClassLoading(cloneAncestorResources = true)
public abstract class AbstractHadoopProcessor extends AbstractProcessor {
    private static final String ALLOW_EXPLICIT_KEYTAB = "NIFI_ALLOW_EXPLICIT_KEYTAB";

    // properties
    public static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("Hadoop Configuration Resources")
            .description("A file or comma separated list of files which contains the Hadoop file system configuration. Without this, Hadoop "
                    + "will search the classpath for a 'core-site.xml' and 'hdfs-site.xml' file or will revert to a default configuration. "
                    + "To use swebhdfs, see 'Additional Details' section of PutHDFS's documentation.")
            .required(false)
            .addValidator(HadoopValidators.ONE_OR_MORE_FILE_EXISTS_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("The HDFS directory from which files should be read")
            .required(true)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor COMPRESSION_CODEC = new PropertyDescriptor.Builder()
            .name("Compression codec")
            .required(true)
            .allowableValues(CompressionType.allowableValues())
            .defaultValue(CompressionType.NONE.toString())
            .build();

    public static final PropertyDescriptor KERBEROS_RELOGIN_PERIOD = new PropertyDescriptor.Builder()
            .name("Kerberos Relogin Period").required(false)
            .description("Period of time which should pass before attempting a kerberos relogin.\n\nThis property has been deprecated, and has no effect on processing. " +
                    "Relogins now occur automatically.")
            .defaultValue("4 hours")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor ADDITIONAL_CLASSPATH_RESOURCES = new PropertyDescriptor.Builder()
            .name("Additional Classpath Resources")
            .description("A comma-separated list of paths to files and/or directories that will be added to the classpath. When specifying a " +
                    "directory, all files with in the directory will be added to the classpath, but further sub-directories will not be included.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dynamicallyModifiesClasspath(true)
            .build();

    static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();

    public static final String ABSOLUTE_HDFS_PATH_ATTRIBUTE = "absolute.hdfs.path";

    private static final Object RESOURCES_LOCK = new Object();

    protected KerberosProperties kerberosProperties;
    protected List<PropertyDescriptor> properties;
    private volatile File kerberosConfigFile = null;

    // variables shared by all threads of this processor
    // Hadoop Configuration, Filesystem, and UserGroupInformation (optional)
    private final AtomicReference<HdfsResources> hdfsResources = new AtomicReference<>();

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    @Override
    protected void init(ProcessorInitializationContext context) {
        hdfsResources.set(new HdfsResources(null, null, null));

        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosProperties = getKerberosProperties(kerberosConfigFile);

        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HADOOP_CONFIGURATION_RESOURCES);
        props.add(KERBEROS_CREDENTIALS_SERVICE);
        props.add(kerberosProperties.getKerberosPrincipal());
        props.add(kerberosProperties.getKerberosKeytab());
        props.add(KERBEROS_RELOGIN_PERIOD);
        props.add(ADDITIONAL_CLASSPATH_RESOURCES);
        properties = Collections.unmodifiableList(props);
    }

    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return new KerberosProperties(kerberosConfigFile);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final String configResources = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        final String explicitPrincipal = validationContext.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = validationContext.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();
        final KerberosCredentialsService credentialsService = validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

        final String resolvedPrincipal;
        final String resolvedKeytab;
        if (credentialsService == null) {
            resolvedPrincipal = explicitPrincipal;
            resolvedKeytab = explicitKeytab;
        } else {
            resolvedPrincipal = credentialsService.getPrincipal();
            resolvedKeytab = credentialsService.getKeytab();
        }

        final List<ValidationResult> results = new ArrayList<>();

        if (StringUtils.isBlank(configResources)) {
            return results;
        }

        try {
            ValidationResources resources = validationResourceHolder.get();

            // if no resources in the holder, or if the holder has different resources loaded,
            // then load the Configuration and set the new resources in the holder
            if (resources == null || !configResources.equals(resources.getConfigResources())) {
                getLogger().debug("Reloading validation resources");
                final Configuration config = new ExtendedConfiguration(getLogger());
                config.setClassLoader(Thread.currentThread().getContextClassLoader());
                resources = new ValidationResources(configResources, getConfigurationFromResources(config, configResources));
                validationResourceHolder.set(resources);
            }

            final Configuration conf = resources.getConfiguration();
            results.addAll(KerberosProperties.validatePrincipalAndKeytab(
                this.getClass().getSimpleName(), conf, resolvedPrincipal, resolvedKeytab, getLogger()));

        } catch (final IOException e) {
            results.add(new ValidationResult.Builder()
                    .valid(false)
                    .subject("Hadoop Configuration Resources")
                    .explanation("Could not load Hadoop Configuration resources due to: " + e)
                    .build());
        }

        if (credentialsService != null && (explicitPrincipal != null || explicitKeytab != null)) {
            results.add(new ValidationResult.Builder()
                .subject("Kerberos Credentials")
                .valid(false)
                .explanation("Cannot specify both a Kerberos Credentials Service and a principal/keytab")
                .build());
        }

        final String allowExplicitKeytabVariable = System.getenv(ALLOW_EXPLICIT_KEYTAB);
        if ("false".equalsIgnoreCase(allowExplicitKeytabVariable) && (explicitPrincipal != null || explicitKeytab != null)) {
            results.add(new ValidationResult.Builder()
                .subject("Kerberos Credentials")
                .valid(false)
                .explanation("The '" + ALLOW_EXPLICIT_KEYTAB + "' system environment variable is configured to forbid explicitly configuring principal/keytab in processors. "
                    + "The Kerberos Credentials Service should be used instead of setting the Kerberos Keytab or Kerberos Principal property.")
                .build());
        }

        return results;
    }

    /**
     * If your subclass also has an @OnScheduled annotated method and you need hdfsResources in that method, then be sure to call super.abstractOnScheduled(context)
     */
    @OnScheduled
    public final void abstractOnScheduled(ProcessContext context) throws IOException {
        try {
            // This value will be null when called from ListHDFS, because it overrides all of the default
            // properties this processor sets. TODO: re-work ListHDFS to utilize Kerberos
            HdfsResources resources = hdfsResources.get();
            if (resources.getConfiguration() == null) {
                final String configResources = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
                resources = resetHDFSResources(configResources, context);
                hdfsResources.set(resources);
            }
        } catch (IOException ex) {
            getLogger().error("HDFS Configuration error - {}", new Object[] { ex });
            hdfsResources.set(new HdfsResources(null, null, null));
            throw ex;
        }
    }

    @OnStopped
    public final void abstractOnStopped() {
        final HdfsResources resources = hdfsResources.get();
        if (resources != null) {
            // Attempt to close the FileSystem
            final FileSystem fileSystem = resources.getFileSystem();
            try {
                interruptStatisticsThread(fileSystem);
            } catch (Exception e) {
                getLogger().warn("Error stopping FileSystem statistics thread: " + e.getMessage(), e);
            } finally {
                if (fileSystem != null) {
                    try {
                        fileSystem.close();
                    } catch (IOException e) {
                        getLogger().warn("Error close FileSystem: " + e.getMessage(), e);
                    }
                }
            }

            // Clean-up the static reference to the Configuration instance
            UserGroupInformation.setConfiguration(new Configuration());

            // Clean-up the reference to the InstanceClassLoader that was put into Configuration
            final Configuration configuration = resources.getConfiguration();
            configuration.setClassLoader(null);

            // Need to remove the Provider instance from the JVM's Providers class so that InstanceClassLoader can be GC'd eventually
            final SaslPlainServer.SecurityProvider saslProvider = new SaslPlainServer.SecurityProvider();
            Security.removeProvider(saslProvider.getName());
        }

        // Clear out the reference to the resources
        hdfsResources.set(new HdfsResources(null, null, null));
    }

    private void interruptStatisticsThread(final FileSystem fileSystem) throws NoSuchFieldException, IllegalAccessException {
        final Field statsField = FileSystem.class.getDeclaredField("statistics");
        statsField.setAccessible(true);

        final Object statsObj = statsField.get(fileSystem);
        if (statsObj != null && statsObj instanceof FileSystem.Statistics) {
            final FileSystem.Statistics statistics = (FileSystem.Statistics) statsObj;

            final Field statsThreadField = statistics.getClass().getDeclaredField("STATS_DATA_CLEANER");
            statsThreadField.setAccessible(true);

            final Object statsThreadObj = statsThreadField.get(statistics);
            if (statsThreadObj != null && statsThreadObj instanceof Thread) {
                final Thread statsThread = (Thread) statsThreadObj;
                try {
                    statsThread.interrupt();
                } catch (Exception e) {
                    getLogger().warn("Error interrupting thread: " + e.getMessage(), e);
                }
            }
        }
    }

    private static Configuration getConfigurationFromResources(final Configuration config, String configResources) throws IOException {
        boolean foundResources = false;
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

    /*
     * Reset Hadoop Configuration and FileSystem based on the supplied configuration resources.
     */
    HdfsResources resetHDFSResources(String configResources, ProcessContext context) throws IOException {
        Configuration config = new ExtendedConfiguration(getLogger());
        config.setClassLoader(Thread.currentThread().getContextClassLoader());

        getConfigurationFromResources(config, configResources);

        // give sub-classes a chance to process configuration
        preProcessConfiguration(config, context);

        // first check for timeout on HDFS connection, because FileSystem has a hard coded 15 minute timeout
        checkHdfsUriForTimeout(config);

        // disable caching of Configuration and FileSystem objects, else we cannot reconfigure the processor without a complete
        // restart
        String disableCacheName = String.format("fs.%s.impl.disable.cache", FileSystem.getDefaultUri(config).getScheme());
        config.set(disableCacheName, "true");

        // If kerberos is enabled, create the file system as the kerberos principal
        // -- use RESOURCE_LOCK to guarantee UserGroupInformation is accessed by only a single thread at at time
        FileSystem fs;
        UserGroupInformation ugi;
        synchronized (RESOURCES_LOCK) {
            if (SecurityUtil.isSecurityEnabled(config)) {
                String principal = context.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
                String keyTab = context.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();

                // If the Kerberos Credentials Service is specified, we need to use its configuration, not the explicit properties for principal/keytab.
                // The customValidate method ensures that only one can be set, so we know that the principal & keytab above are null.
                final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);
                if (credentialsService != null) {
                    principal = credentialsService.getPrincipal();
                    keyTab = credentialsService.getKeytab();
                }

                ugi = SecurityUtil.loginKerberos(config, principal, keyTab);
                fs = getFileSystemAsUser(config, ugi);
            } else {
                config.set("ipc.client.fallback-to-simple-auth-allowed", "true");
                config.set("hadoop.security.authentication", "simple");
                ugi = SecurityUtil.loginSimple(config);
                fs = getFileSystemAsUser(config, ugi);
            }
        }
        getLogger().debug("resetHDFSResources UGI {}", new Object[]{ugi});

        final Path workingDir = fs.getWorkingDirectory();
        getLogger().info("Initialized a new HDFS File System with working dir: {} default block size: {} default replication: {} config: {}",
                new Object[]{workingDir, fs.getDefaultBlockSize(workingDir), fs.getDefaultReplication(workingDir), config.toString()});

        return new HdfsResources(config, fs, ugi);
    }

    /**
     * This method will be called after the Configuration has been created, but before the FileSystem is created,
     * allowing sub-classes to take further action on the Configuration before creating the FileSystem.
     *
     * @param config the Configuration that will be used to create the FileSystem
     * @param context the context that can be used to retrieve additional values
     */
    protected void preProcessConfiguration(final Configuration config, final ProcessContext context) {

    }

    /**
     * This exists in order to allow unit tests to override it so that they don't take several minutes waiting for UDP packets to be received
     *
     * @param config
     *            the configuration to use
     * @return the FileSystem that is created for the given Configuration
     * @throws IOException
     *             if unable to create the FileSystem
     */
    protected FileSystem getFileSystem(final Configuration config) throws IOException {
        return FileSystem.get(config);
    }

    protected FileSystem getFileSystemAsUser(final Configuration config, UserGroupInformation ugi) throws IOException {
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
    protected void checkHdfsUriForTimeout(Configuration config) throws IOException {
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

    /**
     * Returns the configured CompressionCodec, or null if none is configured.
     *
     * @param context
     *            the ProcessContext
     * @param configuration
     *            the Hadoop Configuration
     * @return CompressionCodec or null
     */
    protected org.apache.hadoop.io.compress.CompressionCodec getCompressionCodec(ProcessContext context, Configuration configuration) {
        org.apache.hadoop.io.compress.CompressionCodec codec = null;
        if (context.getProperty(COMPRESSION_CODEC).isSet()) {
            String compressionClassname = CompressionType.valueOf(context.getProperty(COMPRESSION_CODEC).getValue()).toString();
            CompressionCodecFactory ccf = new CompressionCodecFactory(configuration);
            codec = ccf.getCodecByClassName(compressionClassname);
        }

        return codec;
    }

    /**
     * Returns the relative path of the child that does not include the filename or the root path.
     *
     * @param root
     *            the path to relativize from
     * @param child
     *            the path to relativize
     * @return the relative path
     */
    public static String getPathDifference(final Path root, final Path child) {
        final int depthDiff = child.depth() - root.depth();
        if (depthDiff <= 1) {
            return "".intern();
        }
        String lastRoot = root.getName();
        Path childsParent = child.getParent();
        final StringBuilder builder = new StringBuilder();
        builder.append(childsParent.getName());
        for (int i = (depthDiff - 3); i >= 0; i--) {
            childsParent = childsParent.getParent();
            String name = childsParent.getName();
            if (name.equals(lastRoot) && childsParent.toString().endsWith(root.toString())) {
                break;
            }
            builder.insert(0, Path.SEPARATOR).insert(0, name);
        }
        return builder.toString();
    }

    protected Configuration getConfiguration() {
        return hdfsResources.get().getConfiguration();
    }

    protected FileSystem getFileSystem() {
        return hdfsResources.get().getFileSystem();
    }

    protected UserGroupInformation getUserGroupInformation() {
        return hdfsResources.get().getUserGroupInformation();
    }

    static protected class HdfsResources {
        private final Configuration configuration;
        private final FileSystem fileSystem;
        private final UserGroupInformation userGroupInformation;

        public HdfsResources(Configuration configuration, FileSystem fileSystem, UserGroupInformation userGroupInformation) {
            this.configuration = configuration;
            this.fileSystem = fileSystem;
            this.userGroupInformation = userGroupInformation;
        }

        public Configuration getConfiguration() {
            return configuration;
        }

        public FileSystem getFileSystem() {
            return fileSystem;
        }

        public UserGroupInformation getUserGroupInformation() {
            return userGroupInformation;
        }
    }

    static protected class ValidationResources {
        private final String configResources;
        private final Configuration configuration;

        public ValidationResources(String configResources, Configuration configuration) {
            this.configResources = configResources;
            this.configuration = configuration;
        }

        public String getConfigResources() {
            return configResources;
        }

        public Configuration getConfiguration() {
            return configuration;
        }
    }

    /**
     * Extending Hadoop Configuration to prevent it from caching classes that can't be found. Since users may be
     * adding additional JARs to the classpath we don't want them to have to restart the JVM to be able to load
     * something that was previously not found, but might now be available.
     *
     * Reference the original getClassByNameOrNull from Configuration.
     */
    static class ExtendedConfiguration extends Configuration {

        private final ComponentLog logger;
        private final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<>();

        public ExtendedConfiguration(final ComponentLog logger) {
            this.logger = logger;
        }

        @Override
        public Class<?> getClassByNameOrNull(String name) {
            final ClassLoader classLoader = getClassLoader();

            Map<String, WeakReference<Class<?>>> map;
            synchronized (CACHE_CLASSES) {
                map = CACHE_CLASSES.get(classLoader);
                if (map == null) {
                    map = Collections.synchronizedMap(new WeakHashMap<>());
                    CACHE_CLASSES.put(classLoader, map);
                }
            }

            Class<?> clazz = null;
            WeakReference<Class<?>> ref = map.get(name);
            if (ref != null) {
                clazz = ref.get();
            }

            if (clazz == null) {
                try {
                    clazz = Class.forName(name, true, classLoader);
                } catch (ClassNotFoundException e) {
                    logger.error(e.getMessage(), e);
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
