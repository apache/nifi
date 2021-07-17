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
package org.apache.nifi.dbcp.hive;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hive.jdbc.HiveDriver;
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
import org.apache.nifi.dbcp.DBCPValidator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.hadoop.SecurityUtil;
import org.apache.nifi.kerberos.KerberosCredentialsService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosKeytabUser;
import org.apache.nifi.security.krb.KerberosPasswordUser;
import org.apache.nifi.security.krb.KerberosUser;
import org.apache.nifi.util.hive.AuthenticationFailedException;
import org.apache.nifi.util.hive.HiveConfigurator;
import org.apache.nifi.util.hive.ValidationResources;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation for Database Connection Pooling Service used for Apache Hive
 * connections. Apache DBCP is used for connection pooling functionality.
 */
@RequiresInstanceClassLoading
@Tags({"hive", "dbcp", "jdbc", "database", "connection", "pooling", "store"})
@CapabilityDescription("Provides Database Connection Pooling Service for Apache Hive 3.x. Connections can be asked from pool and returned after usage.")
public class Hive3ConnectionPool extends AbstractControllerService implements Hive3DBCPService {
    private static final String ALLOW_EXPLICIT_KEYTAB = "NIFI_ALLOW_EXPLICIT_KEYTAB";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_IDLE} in Commons-DBCP 2.6.0
     */
    private static final String DEFAULT_MIN_IDLE = "0";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MAX_IDLE} in Commons-DBCP 2.6.0
     */
    private static final String DEFAULT_MAX_IDLE = "8";
    /**
     * Copied from private variable {@link BasicDataSource.maxConnLifetimeMillis} in Commons-DBCP 2.6.0
     */
    private static final String DEFAULT_MAX_CONN_LIFETIME = "-1";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_TIME_BETWEEN_EVICTION_RUNS_MILLIS} in Commons-DBCP 2.6.0
     */
    private static final String DEFAULT_EVICTION_RUN_PERIOD = String.valueOf(-1L);
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.6.0
     * and converted from 1800000L to "1800000 millis" to "30 mins"
     */
    private static final String DEFAULT_MIN_EVICTABLE_IDLE_TIME = "30 mins";
    /**
     * Copied from {@link GenericObjectPoolConfig.DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS} in Commons-DBCP 2.6.0
     */
    private static final String DEFAULT_SOFT_MIN_EVICTABLE_IDLE_TIME = String.valueOf(-1L);

    static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
            .name("hive-db-connect-url")
            .displayName("Database Connection URL")
            .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
                    + " The exact syntax of a database connection URL is specified by the Hive documentation. For example, the server principal is often included "
                    + "as a connection parameter when connecting to a secure Hive server.")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor HIVE_CONFIGURATION_RESOURCES = new PropertyDescriptor.Builder()
            .name("hive-config-resources")
            .displayName("Hive Configuration Resources")
            .description("A file or comma separated list of files which contains the Hive configuration (hive-site.xml, e.g.). Without this, Hadoop "
                    + "will search the classpath for a 'hive-site.xml' file or will revert to a default configuration. Note that to enable authentication "
                    + "with Kerberos e.g., the appropriate properties must be set in the configuration files. Please see the Hive documentation for more details.")
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
            .name("hive-db-user")
            .displayName("Database User")
            .description("Database user name")
            .defaultValue(null)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
            .name("hive-db-password")
            .displayName("Password")
            .description("The password for the database user")
            .defaultValue(null)
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor MAX_WAIT_TIME = new PropertyDescriptor.Builder()
            .name("hive-max-wait-time")
            .displayName("Max Wait Time")
            .description("The maximum amount of time that the pool will wait (when there are no available connections) "
                    + " for a connection to be returned before failing, or -1 to wait indefinitely. ")
            .defaultValue("500 millis")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor MAX_TOTAL_CONNECTIONS = new PropertyDescriptor.Builder()
            .name("hive-max-total-connections")
            .displayName("Max Total Connections")
            .description("The maximum number of active connections that can be allocated from this pool at the same time, "
                    + "or negative for no limit.")
            .defaultValue("8")
            .required(true)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    static final PropertyDescriptor VALIDATION_QUERY = new PropertyDescriptor.Builder()
            .name("Validation-query")
            .displayName("Validation query")
            .description("Validation query used to validate connections before returning them. "
                    + "When a borrowed connection is invalid, it gets dropped and a new valid connection will be returned. "
                    + "NOTE: Using validation may have a performance penalty.")
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

    private static final PropertyDescriptor KERBEROS_CREDENTIALS_SERVICE = new PropertyDescriptor.Builder()
            .name("kerberos-credentials-service")
            .displayName("Kerberos Credentials Service")
            .description("Specifies the Kerberos Credentials Controller Service that should be used for authenticating with Kerberos")
            .identifiesControllerService(KerberosCredentialsService.class)
            .required(false)
            .build();


    private List<PropertyDescriptor> properties;

    private String connectionUrl = "unknown";

    // Holder of cached Configuration information so validation does not reload the same config over and over
    private final AtomicReference<ValidationResources> validationResourceHolder = new AtomicReference<>();

    private volatile BasicDataSource dataSource;

    private volatile HiveConfigurator hiveConfigurator = new HiveConfigurator();
    private volatile UserGroupInformation ugi;
    private final AtomicReference<KerberosUser> kerberosUserReference = new AtomicReference<>();
    private volatile File kerberosConfigFile = null;
    private volatile KerberosProperties kerberosProperties;

    @Override
    protected void init(final ControllerServiceInitializationContext context) {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(DATABASE_URL);
        props.add(HIVE_CONFIGURATION_RESOURCES);
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
        props.add(KERBEROS_CREDENTIALS_SERVICE);

        kerberosConfigFile = context.getKerberosConfigurationFile();
        kerberosProperties = new KerberosProperties(kerberosConfigFile);
        props.add(kerberosProperties.getKerberosPrincipal());
        props.add(kerberosProperties.getKerberosKeytab());
        props.add(kerberosProperties.getKerberosPassword());
        properties = props;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        boolean confFileProvided = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).isSet();

        final List<ValidationResult> problems = new ArrayList<>();

        if (confFileProvided) {
            final String explicitPrincipal = validationContext.getProperty(kerberosProperties.getKerberosPrincipal()).evaluateAttributeExpressions().getValue();
            final String explicitKeytab = validationContext.getProperty(kerberosProperties.getKerberosKeytab()).evaluateAttributeExpressions().getValue();
            final String explicitPassword = validationContext.getProperty(kerberosProperties.getKerberosPassword()).getValue();
            final KerberosCredentialsService credentialsService = validationContext.getProperty(KERBEROS_CREDENTIALS_SERVICE).asControllerService(KerberosCredentialsService.class);

            final String resolvedPrincipal;
            final String resolvedKeytab;
            if (credentialsService != null) {
                resolvedPrincipal = credentialsService.getPrincipal();
                resolvedKeytab = credentialsService.getKeytab();
            } else {
                resolvedPrincipal = explicitPrincipal;
                resolvedKeytab = explicitKeytab;
            }


            final String configFiles = validationContext.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
            problems.addAll(hiveConfigurator.validate(configFiles, resolvedPrincipal, resolvedKeytab, explicitPassword, validationResourceHolder, getLogger()));

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
        }

        return problems;
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link BasicDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     * <p/>
     * As of Apache NiFi 1.5.0, due to changes made to
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}, which is used by this class invoking
     * {@link HiveConfigurator#authenticate(Configuration, String, String)}
     * to authenticate a principal with Kerberos, Hive controller services no longer use a separate thread to
     * relogin, and instead call {@link UserGroupInformation#checkTGTAndReloginFromKeytab()} from
     * {@link Hive3ConnectionPool#getConnection()}.  The relogin request is performed in a synchronized block to prevent
     * threads from requesting concurrent relogins.  For more information, please read the documentation for
     * {@link SecurityUtil#loginKerberos(Configuration, String, String)}.
     * <p/>
     * In previous versions of NiFi, a {@link org.apache.nifi.hadoop.KerberosTicketRenewer} was started by
     * {@link HiveConfigurator#authenticate(Configuration, String, String, long)} when the Hive
     * controller service was enabled.  The use of a separate thread to explicitly relogin could cause race conditions
     * with the implicit relogin attempts made by hadoop/Hive code on a thread that references the same
     * {@link UserGroupInformation} instance.  One of these threads could leave the
     * {@link javax.security.auth.Subject} in {@link UserGroupInformation} to be cleared or in an unexpected state
     * while the other thread is attempting to use the {@link javax.security.auth.Subject}, resulting in failed
     * authentication attempts that would leave the Hive controller service in an unrecoverable state.
     *
     * @see SecurityUtil#loginKerberos(Configuration, String, String)
     * @see HiveConfigurator#authenticate(Configuration, String, String)
     * @see HiveConfigurator#authenticate(Configuration, String, String, long)
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        ComponentLog log = getLogger();

        final String configFiles = context.getProperty(HIVE_CONFIGURATION_RESOURCES).evaluateAttributeExpressions().getValue();
        final Configuration hiveConfig = hiveConfigurator.getConfigurationFromFiles(configFiles);
        final String validationQuery = context.getProperty(VALIDATION_QUERY).evaluateAttributeExpressions().getValue();

        // add any dynamic properties to the Hive configuration
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.isDynamic()) {
                hiveConfig.set(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        }

        final String drv = HiveDriver.class.getName();
        if (SecurityUtil.isSecurityEnabled(hiveConfig)) {
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
                kerberosUserReference.set(new KerberosKeytabUser(resolvedPrincipal, resolvedKeytab));
                log.info("Hive Security Enabled, logging in as principal {} with keytab {}", new Object[] {resolvedPrincipal, resolvedKeytab});
            } else if (explicitPassword != null) {
                kerberosUserReference.set(new KerberosPasswordUser(resolvedPrincipal, explicitPassword));
                log.info("Hive Security Enabled, logging in as principal {} with password", new Object[] {resolvedPrincipal});
            } else {
                throw new InitializationException("Unable to authenticate with Kerberos, no keytab or password was provided");
            }

            try {
                ugi = hiveConfigurator.authenticate(hiveConfig, kerberosUserReference.get());
            } catch (AuthenticationFailedException ae) {
                log.error(ae.getMessage(), ae);
                throw new InitializationException(ae);
            }

            getLogger().info("Successfully logged in as principal " + resolvedPrincipal);
        }

        final String user = context.getProperty(DB_USER).evaluateAttributeExpressions().getValue();
        final String passw = context.getProperty(DB_PASSWORD).evaluateAttributeExpressions().getValue();
        final Long maxWaitMillis = context.getProperty(MAX_WAIT_TIME).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);
        final Integer maxTotal = context.getProperty(MAX_TOTAL_CONNECTIONS).evaluateAttributeExpressions().asInteger();
        final Integer minIdle = context.getProperty(MIN_IDLE).evaluateAttributeExpressions().asInteger();
        final Integer maxIdle = context.getProperty(MAX_IDLE).evaluateAttributeExpressions().asInteger();
        final Long maxConnLifetimeMillis = extractMillisWithInfinite(context.getProperty(MAX_CONN_LIFETIME).evaluateAttributeExpressions());
        final Long timeBetweenEvictionRunsMillis = extractMillisWithInfinite(context.getProperty(EVICTION_RUN_PERIOD).evaluateAttributeExpressions());
        final Long minEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());
        final Long softMinEvictableIdleTimeMillis = extractMillisWithInfinite(context.getProperty(SOFT_MIN_EVICTABLE_IDLE_TIME).evaluateAttributeExpressions());

        dataSource = new BasicDataSource();
        dataSource.setDriverClassName(drv);

        connectionUrl = context.getProperty(DATABASE_URL).evaluateAttributeExpressions().getValue();

        if (validationQuery != null && !validationQuery.isEmpty()) {
            dataSource.setValidationQuery(validationQuery);
            dataSource.setTestOnBorrow(true);
        }

        dataSource.setUrl(connectionUrl);
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
    }

    private Long extractMillisWithInfinite(PropertyValue prop) {
        return "-1".equals(prop.getValue()) ? -1 : prop.asTimePeriod(TimeUnit.MILLISECONDS);
    }

    /**
     * Shutdown pool, close all open connections.
     */
    @OnDisabled
    public void shutdown() {
        try {
            dataSource.close();
        } catch (final SQLException e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public Connection getConnection() throws ProcessException {
        try {
            if (ugi != null) {
                /*
                 * Explicitly check the TGT and relogin if necessary with the KerberosUser instance.  No synchronization
                 * is necessary in the client code, since AbstractKerberosUser's checkTGTAndRelogin method is synchronized.
                 */
                getLogger().trace("getting UGI instance");
                if (kerberosUserReference.get() != null) {
                    // if there's a KerberosUser associated with this UGI, check the TGT and relogin if it is close to expiring
                    KerberosUser kerberosUser = kerberosUserReference.get();
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
            getLogger().error("Error getting Hive connection", e);
            throw new ProcessException(e);
        }
    }

    @Override
    public String toString() {
        return "Hive3ConnectionPool[id=" + getIdentifier() + "]";
    }

    @Override
    public String getConnectionURL() {
        return connectionUrl;
    }

    /*
     * Overridable by subclasses in the same package, mainly intended for testing purposes to allow verification without having to set environment variables.
     */
    boolean isAllowExplicitKeytab() {
        return Boolean.parseBoolean(System.getenv(ALLOW_EXPLICIT_KEYTAB));
    }
}
