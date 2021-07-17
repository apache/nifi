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
package org.apache.nifi.dbcp;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.RequiresInstanceClassLoading;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of Database Connection Pooling Service for Hadoop related JDBC Service.
 * Apache DBCP is used for connection pooling functionality.
 *
 */
@RequiresInstanceClassLoading
@Tags({ "dbcp", "jdbc", "database", "connection", "pooling", "store", "hadoop" })
@CapabilityDescription("Provides a Database Connection Pooling Service for Hadoop related JDBC services. This service requires that " +
        "the Database Driver Location(s) contains some version of a hadoop-common JAR, or a shaded JAR that shades hadoop-common.")
@DynamicProperty(name = "The name of a Hadoop configuration property.", value = "The value of the given Hadoop configuration property.",
        description = "These properties will be set on the Hadoop configuration after loading any provided configuration files.",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
public class HadoopDBCPConnectionPool extends AbstractControllerService implements DBCPService {

    private static final String ALLOW_EXPLICIT_KEYTAB = "NIFI_ALLOW_EXPLICIT_KEYTAB";

    private static final String HADOOP_CONFIGURATION_CLASS = "org.apache.hadoop.conf.Configuration";
    private static final String HADOOP_UGI_CLASS = "org.apache.hadoop.security.UserGroupInformation";

    private static final String DEFAULT_MIN_IDLE = "0";
    private static final String DEFAULT_MAX_IDLE = "8";
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";
    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);
    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";
    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
                    + " The exact syntax of a database connection URL is specified by your DBMS.")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
            .name("Database Driver Class Name")
            .description("Database driver class name")
            .defaultValue(null)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("database-driver-locations")
            .displayName("Database Driver Location(s)")
            .description("Comma-separated list of files/folders and/or URLs containing the driver JAR and its dependencies (if any). " +
                    "For example '/var/tmp/phoenix-client.jar'. NOTE: It is required that the resources specified by this property provide " +
                    "the classes from hadoop-common, such as Configuration and UserGroupInformation.")
            .defaultValue(null)
            .required(true)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE, ResourceType.DIRECTORY, ResourceType.URL)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    static final PropertyDescriptor HADOOP_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hadoop-config-resources")
            .displayName("Hadoop Configuration Resources")
            .description("A file, or comma separated list of files, which contain the Hadoop configuration (core-site.xml, etc.). Without this, Hadoop "
                    + "will search the classpath, or will revert to a default configuration. Note that to enable authentication with Kerberos, "
                    + "the appropriate properties must be set in the configuration files.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamicallyModifiesClasspath(true)
            .build();

    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("Database User")
            .description("The user for the database")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("Password")
            .description("The password for the database user")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections) "
                    + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
            .defaultValue("500 millis")
            .required(true)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("Max Total Connections")
            .description("The maximum number of active connections that can be allocated from this pool at the same time, "
                    + " or negative for no limit.")
            .defaultValue("8")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
            .name("Validation-query")
            .displayName("Validation query")
            .description("Validation query used to validate connections before returning them. "
                    + "When connection is invalid, it get's dropped and new valid connection will be returned. "
                    + "Note!! Using validation might have some performance penalty.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_IDLE = new PropertyDescriptor.Builder()
            .displayName("Minimum Idle Connections")
            .name("dbcp-min-idle-conns")
            .description("The minimum number of connections that can remain idle in the pool, without extra ones being " +
                    "created, or zero to create none.")
            .defaultValue(DEFAULT_MIN_IDLE)
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_IDLE = new PropertyDescriptor.Builder()
            .displayName("Max Idle Connections")
            .name("dbcp-max-idle-conns")
            .description("The maximum number of connections that can remain idle in the pool, without extra ones being " +
                    "released, or negative for no limit.")
            .defaultValue(DEFAULT_MAX_IDLE)
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MAX_CONN_LIFETIME = new PropertyDescriptor.Builder()
            .displayName("Max Connection Lifetime")
            .name("dbcp-max-conn-lifetime")
            .description("The maximum lifetime in milliseconds of a connection. After this time is exceeded the " +
                    "connection will fail the next activation, passivation or validation test. A value of zero or less " +
                    "means the connection has an infinite lifetime.")
            .defaultValue(DEFAULT_MAX_CONN_LIFETIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor EVICTION_RUN_PERIOD = new PropertyDescriptor.Builder()
            .displayName("Time Between Eviction Runs")
            .name("dbcp-time-between-eviction-runs")
            .description("The number of milliseconds to sleep between runs of the idle connection evictor thread. When " +
                    "non-positive, no idle connection evictor thread will be run.")
            .defaultValue(DEFAULT_EVICTION_RUN_PERIOD)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Minimum Evictable Idle Time")
            .name("dbcp-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for eviction.")
            .defaultValue(DEFAULT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SOFT_MIN_EVICTABLE_IDLE_TIME = new PropertyDescriptor.Builder()
            .displayName("Soft Minimum Evictable Idle Time")
            .name("dbcp-soft-min-evictable-idle-time")
            .description("The minimum amount of time a connection may sit idle in the pool before it is eligible for " +
                    "eviction by the idle connection evictor, with the extra condition that at least a minimum number of" +
                    " idle connections remain in the pool. When the not-soft version of this option is set to a positive" +
                    " value, it is examined first by the idle connection evictor: when idle connections are visited by " +
                    "the evictor, idle time is first compared against it (without considering the number of idle " +
                    "connections in the pool) and then against this soft option, including the minimum idle connections " +
                    "constraint.")
            .defaultValue(DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME)
            .required(false)
            .addValidator(DBCPValidator.CUSTOM_TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();


    private File kerberosConfigFile = null;
    private KerberosProperties kerberosProperties;
    private List<PropertyDescriptor> properties;

    private volatile BasicDataSource dataSource;
    private volatile UserGroupInformation ugi;
    private volatile KerberosUser kerberosUser;
    private volatile Boolean foundHadoopDependencies;

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>(null);

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosProperties = getKerberosProperties(kerberosConfigFile);

        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(DB_DRIVERNAME);
        props.add(DB_DRIVER_LOCATION);
        props.add(HADOOP_CONFIGURATION_RESOURCES);
        props.add(KERBEROS_CREDENTIALS_SERVICE);
        props.add(kerberosProperties.getKerberosPrincipal());
        props.add(kerberosProperties.getKerberosKeytab());
        props.add(kerberosProperties.getKerberosPassword());
        props.add(DB_USER);
        props.add(DB_PASSWORD);
        props.add(MAX_WAIT_TIME);
        props.add(MAX_TOTAL_CONNECTIONS);
        props.add(VALIDATION_QUERY);
        props.add(MIN_IDLE);
        props.add(MAX_IDLE);
        props.add(MAX_CONN_LIFETIME);
        props.add(EVICTION_RUN_PERIOD);
        props.add(MIN_EVICTABLE_IDLE_TIME);
        props.add(SOFT_MIN_EVICTABLE_IDLE_TIME);

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
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                .dynamic(true)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>();

        // Determine if we need to validate the presence of the required hadoop dependencies...
        if (foundHadoopDependencies == null) {
            final ClassLoader classLoader = this.getClass().getClassLoader();
            try {
                Class.forName(HADOOP_CONFIGURATION_CLASS, true, classLoader);
                Class.forName(HADOOP_UGI_CLASS, true, classLoader);
                foundHadoopDependencies = true;
            } catch (ClassNotFoundException cnf) {
                getLogger().debug(cnf.getMessage(), cnf);
                foundHadoopDependencies = false;
            }
        }

        // If hadoop classes are missing then we can't perform the rest of the validation, so short circuit and return...
        if (!foundHadoopDependencies) {
            problems.add(new ValidationResult.Builder()
                    .subject(DB_DRIVER_LOCATION.getDisplayName())
                    .valid(false)
                    .explanation("required Hadoop classes were not found in any of the specified resources, please ensure that hadoop-common is available")
                    .build());
            return problems;
        }

        // Hadoop classes were found, so proceed with the rest of validation...

        final String explicitPrincipal = validationContext.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
        final String explicitKeytab = validationContext.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();
        final String explicitPassword = validationContext.getProperty(kerberosProperties.getKerberosPassword()).getValue();
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

        final boolean confFileProvided = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).isSet();
        if (confFileProvided) {
            final String configFiles = validationContext.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            ValidationResources resources = validationResourceHolder.get();

            // if no resources in the holder, or if the holder has different resources loaded,
            // then load the Configuration and set the new resources in the holder
            if (resources == null || !configFiles.equals(resources.getConfigResources())) {
                getLogger().debug("Reloading validation resources");
                resources = new ValidationResources(configFiles, getConfigurationFromFiles(configFiles));
                validationResourceHolder.set(resources);
            }

            final Configuration hadoopConfig = resources.getConfiguration();

            problems.addAll(KerberosProperties.validatePrincipalWithKeytabOrPassword(getClass().getSimpleName(), hadoopConfig,
                    resolvedPrincipal, resolvedKeytab, explicitPassword, getLogger()));
        }

        if (credentialsService != null && (explicitPrincipal != null || explicitKeytab != null || explicitPassword != null)) {
            problems.add(new ValidationResult.Builder()
                    .subject("Kerberos Credentials")
                    .valid(false)
                    .explanation("Cannot specify a Kerberos Credentials Service while also specifying a Kerberos Principal, Kerberos Keytab, or Kerberos Password")
                    .build());
        }

        if (!isAllowExplicitKeytab() && explicitKeytab != null) {
            problems.add(new ValidationResult.Builder()
                    .subject("Kerberos Credentials")
                    .valid(false)
                    .explanation("The '" + ALLOW_EXPLICIT_KEYTAB + "' system environment variable is configured to forbid explicitly configuring Kerberos Keytab in processors. "
                            + "The Kerberos Credentials Service should be used instead of setting the Kerberos Keytab or Kerberos Principal property.")
                    .build());
        }

        return problems;
    }

    protected Configuration getConfigurationFromFiles(final String configFiles) {
        final Configuration conf = new Configuration();
        if (StringUtils.isNotBlank(configFiles)) {
            for (final String configFile : configFiles.split(",")) {
                conf.addResource(new Path(configFile.trim()));
            }
        }
        return conf;
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link BasicDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     *
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws IOException {
        // Get Configuration instance from specified resources
        final String configFiles = context.getProperty(HADOOP_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        final Configuration hadoopConfig = getConfigurationFromFiles(configFiles);

        // Add any dynamic properties to the HBase Configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hadoopConfig.set(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        }

        // If security is enabled then determine how to authenticate based on the various principal/keytab/password options
        if (SecurityUtil.isSecurityEnabled(hadoopConfig)) {
            final String explicitPrincipal = context.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
            final String explicitKeytab = context.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();
            final String explicitPassword = context.getProperty(kerberosProperties.getKerberosPassword()).getValue();
            final KerberosCredentialsService credentialsService = context.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

            final String resolvedPrincipal;
            final String resolvedKeytab;
            if (credentialsService != null) {
                resolvedPrincipal = credentialsService.getPrincipal();
                resolvedKeytab = credentialsService.getKeytab();
            } else {
                resolvedPrincipal = explicitPrincipal;
                resolvedKeytab = explicitKeytab;
            }

            if (resolvedKeytab != null) {
                kerberosUser = new KerberosKeytabUser(resolvedPrincipal, resolvedKeytab);
                getLogger().info("Security Enabled, logging in as principal {} with keytab {}", new Object[] {resolvedPrincipal, resolvedKeytab});
            } else if (explicitPassword != null) {
                kerberosUser = new KerberosPasswordUser(resolvedPrincipal, explicitPassword);
                getLogger().info("Security Enabled, logging in as principal {} with password", new Object[] {resolvedPrincipal});
            } else {
                throw new IOException("Unable to authenticate with Kerberos, no keytab or password was provided");
            }

            ugi = SecurityUtil.getUgiForKerberosUser(hadoopConfig, kerberosUser);
            getLogger().info("Successfully logged in as principal " + resolvedPrincipal);
        } else {
            getLogger().info("Simple Authentication");
        }

        // Initialize the DataSource...
        final String dbUrl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();
        final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = extractMillisWithInfinite(context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions());
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());

        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(driverName);
        dataSource.setDriverClassLoader(this.getClass().getClassLoader());
        dataSource.setUrl(dbUrl);
        dataSource.setUsername(user);
        dataSource.setPassword(passw);
        dataSource.setMaxWaitMillis(maxWaitMillis);
        dataSource.setMaxTotal(maxTotal);
        dataSource.setMinIdle(minIdle);
        dataSource.setMaxIdle(maxIdle);
        dataSource.setMaxConnLifetimeMillis(maxConnLifetimeMillis);
        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        dataSource.setSoftMinEvictableIdleTimeMillis(softMinEvictableIdleTimeMillis);

        if (StringUtils.isEmpty(validationQuery)) {
            dataSource.setValidationQuery(validationQuery);
            dataSource.setTestOnBorrow(true);
        }
    }

    private Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     *
     * If a @{@link LoginException} occurs while attempting to log out the @{@link org.apache.nifi.security.krb.KerberosUser},
     * an attempt will still be made to shut down the pool and close open connections.
     *
     * @throws SQLException if there is an error while closing open connections
     * @throws LoginException if there is an error during the principal log out, and will only be thrown if there was
     * no exception while closing open connections
     */
    @OnDisabled
    public void shutdown() throws SQLException, LoginException {
        try {
            if (kerberosUser != null) {
                kerberosUser.logout();
            }
        } finally {
            validationResourceHolder.set(null);
            foundHadoopDependencies = null;
            kerberosUser = null;
            ugi = null;
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            } finally {
                dataSource = null;
            }
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            if (ugi != null) {
                // Explicitly check the TGT and relogin if necessary with the KerberosUser instance.  No synchronization
                // is necessary in the client code, since AbstractKerberosUser's checkTGTAndRelogin method is synchronized.
                getLogger().trace("getting UGI instance");
                if (kerberosUser != null) {
                    // if there's a KerberosUser associated with this UGI, check the TGT and relogin if it is close to expiring
                    getLogger().debug("kerberosUser is " + kerberosUser);
                    try {
                        getLogger().debug("checking TGT on kerberosUser " + kerberosUser);
                        kerberosUser.checkTGTAndRelogin();
                    } catch (LoginException e) {
                        throw new ProcessException("Unable to relogin with kerberos credentials for " + kerberosUser.getPrincipal(), e);
                    }
                } else {
                    getLogger().debug("kerberosUser was null, will not refresh TGT with KerberosUser");
                    // no synchronization is needed for UserGroupInformation.checkTGTAndReloginFromKeytab; UGI handles the synchronization internally
                    ugi.checkTGTAndReloginFromKeytab();
                }

                try {
                    return ugi.doAs((PrivilegedExceptionAction<Connection>) () -> dataSource.getConnection());
                } catch (UndeclaredThrowableException e) {
                    Throwable cause = e.getCause();
                    if (cause instanceof SQLException) {
                        throw (SQLException) cause;
                    } else {
                        throw e;
                    }
                }
            } else {
                getLogger().info("Simple Authentication");
                return dataSource.getConnection();
            }
        } catch (SQLException | IOException | InterruptedException e) {
            getLogger().error("Error getting Connection: " + e.getMessage(), e);
            throw new ProcessException(e);
        }
    }

    @Override
    public String toString() {
        return "HadoopDBCPConnectionPool[id=" + getIdentifier() + "]";
    }

    /*
     * Overridable by subclasses in the same package, mainly intended for testing purposes to allow verification without having to set environment variables.
     */
    boolean isAllowExplicitKeytab() {
        return Boolean.parseBoolean(System.getenv(ALLOW_EXPLICIT_KEYTAB));
    }

    BasicDataSource getDataSource() {
        return dataSource;
    }
}
