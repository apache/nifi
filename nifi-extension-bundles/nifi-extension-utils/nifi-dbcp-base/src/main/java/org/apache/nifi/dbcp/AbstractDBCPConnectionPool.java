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
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.dbcp.utils.DataSourceConfiguration;
import org.apache.nifi.kerberos.KerberosUserService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.krb.KerberosAction;
import org.apache.nifi.security.krb.KerberosLoginException;
import org.apache.nifi.security.krb.KerberosUser;

import java.io.File;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.apache.nifi.components.ConfigVerificationResult.Outcome.FAILED;
import static org.apache.nifi.components.ConfigVerificationResult.Outcome.SUCCESSFUL;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVERNAME;
import static org.apache.nifi.dbcp.utils.DBCPProperties.DB_DRIVER_LOCATION;
import static org.apache.nifi.dbcp.utils.DBCPProperties.KERBEROS_USER_SERVICE;

public abstract class AbstractDBCPConnectionPool extends AbstractControllerService implements DBCPService, VerifiableControllerService {

    protected volatile BasicDataSource dataSource;
    protected volatile KerberosUser kerberosUser;

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger, final Map<String, String> variables) {
        List<ConfigVerificationResult> results = new ArrayList<>();

        KerberosUser kerberosUser = null;

        try {
            kerberosUser = getKerberosUser(context);
            if (kerberosUser != null) {
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Configure Kerberos User")
                        .outcome(SUCCESSFUL)
                        .explanation("Successfully configured Kerberos user")
                        .build());
            }
        } catch (final Exception e) {
            verificationLogger.error("Failed to configure Kerberos user", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Kerberos User")
                    .outcome(FAILED)
                    .explanation("Failed to configure Kerberos user: " + e.getMessage())
                    .build());
        }

        final BasicDataSource basicDataSource = new BasicDataSource();
        try {
            final DataSourceConfiguration configuration = getDataSourceConfiguration(context);
            configureDataSource(context, basicDataSource, configuration);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Data Source")
                    .outcome(SUCCESSFUL)
                    .explanation("Successfully configured data source")
                    .build());

            try (final Connection ignored = getConnection(basicDataSource, kerberosUser)) {
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Establish Connection")
                        .outcome(SUCCESSFUL)
                        .explanation("Successfully established Database Connection")
                        .build());
            } catch (final Exception e) {
                verificationLogger.error("Failed to establish Database Connection", e);
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("Establish Connection")
                        .outcome(FAILED)
                        .explanation("Failed to establish Database Connection: " + e.getMessage())
                        .build());
            }
        } catch (final Exception e) {
            String message = "Failed to configure Data Source.";
            if (e.getCause() instanceof ClassNotFoundException) {
                message += String.format(" Ensure changes to the '%s' property are applied before verifying.",
                        DB_DRIVER_LOCATION.getDisplayName());
            }
            verificationLogger.error(message, e);

            final String driverName = context.getProperty(DB_DRIVERNAME).evaluateAttributeExpressions().getValue();
            final ResourceReferences driverResources = context.getProperty(DB_DRIVER_LOCATION).evaluateAttributeExpressions().asResources();

            if (StringUtils.isNotBlank(driverName) && driverResources.getCount() != 0) {
                List<String> availableDrivers = discoverDriverClassesStatic(driverResources);
                if (!availableDrivers.isEmpty() && !availableDrivers.contains(driverName)) {
                    message += String.format(" Driver class '%s' not found in provided resources. Available driver classes found: %s.",
                            driverName, String.join(", ", availableDrivers));
                }
            }

            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Configure Data Source")
                    .outcome(FAILED)
                    .explanation(message + " Exception: " + e.getMessage())
                    .build());
        } finally {
            try {
                shutdown(basicDataSource, kerberosUser);
            } catch (final SQLException e) {
                verificationLogger.error("Failed to shut down data source", e);
            }
        }
        return results;
    }

    /**
     * Configures connection pool by creating an instance of the
     * {@link BasicDataSource} based on configuration provided with
     * {@link ConfigurationContext}.
     * <p>
     * This operation makes no guarantees that the actual connection could be
     * made since the underlying system may still go off-line during normal
     * operation of the connection pool.
     *
     * @param context the configuration context
     * @throws InitializationException if unable to create a database connection
     */
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        dataSource = new BasicDataSource();
        kerberosUser = getKerberosUser(context);
        loginKerberos(kerberosUser);
        final DataSourceConfiguration configuration = getDataSourceConfiguration(context);
        configureDataSource(context, dataSource, configuration);
    }

    private void loginKerberos(KerberosUser kerberosUser) throws InitializationException {
        if (kerberosUser != null) {
            try {
                kerberosUser.login();
            } catch (KerberosLoginException e) {
                throw new InitializationException("Unable to authenticate Kerberos principal", e);
            }
        }
    }

    protected abstract Driver getDriver(final String driverName, final String url);

    protected abstract DataSourceConfiguration getDataSourceConfiguration(final ConfigurationContext context);

    protected void configureDataSource(final ConfigurationContext context, final BasicDataSource basicDataSource, final DataSourceConfiguration configuration) {
        final Driver driver = getDriver(configuration.getDriverName(), configuration.getUrl());

        basicDataSource.setDriver(driver);
        basicDataSource.setMaxWait(Duration.ofMillis(configuration.getMaxWaitMillis()));
        basicDataSource.setMaxTotal(configuration.getMaxTotal());
        basicDataSource.setMinIdle(configuration.getMinIdle());
        basicDataSource.setMaxIdle(configuration.getMaxIdle());
        basicDataSource.setMaxConn(Duration.ofMillis(configuration.getMaxConnLifetimeMillis()));
        basicDataSource.setDurationBetweenEvictionRuns(Duration.ofMillis(configuration.getTimeBetweenEvictionRunsMillis()));
        basicDataSource.setMinEvictableIdle(Duration.ofMillis(configuration.getMinEvictableIdleTimeMillis()));
        basicDataSource.setSoftMinEvictableIdle(Duration.ofMillis(configuration.getSoftMinEvictableIdleTimeMillis()));

        final String validationQuery = configuration.getValidationQuery();
        if (StringUtils.isNotBlank(validationQuery)) {
            basicDataSource.setValidationQuery(validationQuery);
            basicDataSource.setTestOnBorrow(true);
        }

        basicDataSource.setUrl(configuration.getUrl());
        basicDataSource.setUsername(configuration.getUserName());
        basicDataSource.setPassword(configuration.getPassword());

        getConnectionProperties(context).forEach(basicDataSource::addConnectionProperty);
    }

    protected Map<String, String> getConnectionProperties(final ConfigurationContext context) {
        return getDynamicProperties(context)
                .stream()
                .collect(Collectors.toMap(PropertyDescriptor::getName, s -> {
                    final PropertyValue propertyValue = context.getProperty(s);
                    return propertyValue.evaluateAttributeExpressions().getValue();
                }));
    }

    protected List<PropertyDescriptor> getDynamicProperties(final ConfigurationContext context) {
        return context.getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .collect(Collectors.toList());
    }

    protected KerberosUser getKerberosUser(final ConfigurationContext context) {
        final KerberosUser kerberosUser;
        final KerberosUserService kerberosUserService = context.getProperty(KERBEROS_USER_SERVICE).asControllerService(KerberosUserService.class);

        if (kerberosUserService == null) {
            kerberosUser = null;
        } else {
            kerberosUser = kerberosUserService.createKerberosUser();
        }

        return kerberosUser;
    }


    @Override
    public Connection getConnection() throws ProcessException {
        return getConnection(dataSource, kerberosUser);
    }

    private Connection getConnection(final BasicDataSource dataSource, final KerberosUser kerberosUser) {
        try {
            final Connection con;
            if (kerberosUser != null) {
                KerberosAction<Connection> kerberosAction = new KerberosAction<>(kerberosUser, dataSource::getConnection, getLogger());
                con = kerberosAction.execute();
            } else {
                con = dataSource.getConnection();
            }
            return con;
        } catch (final SQLException e) {
            // If using Kerberos,  attempt to re-login
            if (kerberosUser != null) {
                try {
                    getLogger().info("Error getting connection, performing Kerberos re-login");
                    kerberosUser.login();
                } catch (KerberosLoginException le) {
                    throw new ProcessException("Unable to authenticate Kerberos principal", le);
                }
            }
            throw new ProcessException(e);
        }
    }

    /**
     * Shutdown pool, close all open connections.
     * If a principal is authenticated with a KDC, that principal is logged out.
     *
     * @throws SQLException if there is an error while closing open connections
     */
    @OnDisabled
    public void shutdown() throws SQLException {
        try {
            shutdown(dataSource, kerberosUser);
        } finally {
            kerberosUser = null;
            dataSource = null;
        }
    }

    private void shutdown(final BasicDataSource dataSource, final KerberosUser kerberosUser) throws SQLException {
        try {
            if (kerberosUser != null) {
                kerberosUser.logout();
            }
        } finally {
            if (dataSource != null) {
                dataSource.close();
            }
        }
    }

    /**
     * Discovers JDBC driver classes using static JAR scanning only (no class
     * loading) This is used during validation when resources aren't loaded into
     * classpath yet
     */
    protected List<String> discoverDriverClassesStatic(final ResourceReferences driverResources) {
        final Set<String> driverClasses = new TreeSet<>();

        if (driverResources == null || driverResources.getCount() == 0) {
            getLogger().debug("No driver resources provided for static discovery");
            return new ArrayList<>();
        }
        getLogger().debug("Starting static driver discovery for {} resources", driverResources.getCount());

        for (final ResourceReference resource : driverResources.flattenRecursively().asList()) {
            try {
                getLogger().debug("Processing resource: {} (type: {})", resource.getLocation(), resource.getResourceType());
                final List<File> jarFiles = getJarFilesFromResource(resource);
                getLogger().debug("Found {} JAR files in resource {}", jarFiles.size(), resource.getLocation());

                for (final File jarFile : jarFiles) {
                    getLogger().debug("Scanning JAR file: {}", jarFile.getAbsolutePath());
                    final Set<String> foundDrivers = scanJarForDriverClassesStatic(jarFile);
                    getLogger().debug("Found {} potential driver classes in {}", foundDrivers.size(), jarFile.getName());
                    driverClasses.addAll(foundDrivers);
                }
            } catch (final Exception e) {
                getLogger().warn("Error processing resource {} for static driver discovery", resource.getLocation(), e);
            }
        }

        getLogger().debug("Static driver discovery completed. Found {} potential driver classes", driverClasses.size());
        return new ArrayList<>(driverClasses);
    }

    /**
     * Scans a JAR file for potential JDBC driver class names using static analysis
     * only This uses improved heuristics including checking META-INF/services and
     * common patterns
     */
    private Set<String> scanJarForDriverClassesStatic(final File jarFile) {
        final Set<String> driverClasses = new TreeSet<>();

        try (final JarFile jar = new JarFile(jarFile)) {
            // First, check META-INF/services/java.sql.Driver for registered drivers
            // This is the most reliable method
            final JarEntry servicesEntry = jar.getJarEntry("META-INF/services/java.sql.Driver");
            if (servicesEntry != null) {
                try (Scanner scanner = new Scanner(jar.getInputStream(servicesEntry))) {
                    while (scanner.hasNextLine()) {
                        final String line = scanner.nextLine().trim();
                        if (!line.isEmpty() && !line.startsWith("#")) {
                            driverClasses.add(line);
                            getLogger().debug("Found driver in META-INF/services: {}", line);
                        }
                    }
                }
            }

            // If we found drivers via META-INF/services, prefer those (most reliable)
            if (!driverClasses.isEmpty()) {
                getLogger().debug("Found {} drivers via META-INF/services, using those", driverClasses.size());
                return driverClasses;
            }

            // Fallback: scan for classes with very specific driver patterns
            getLogger().debug("No META-INF/services found, falling back to pattern matching");
            final Enumeration<JarEntry> entries = jar.entries();
            while (entries.hasMoreElements()) {
                final JarEntry entry = entries.nextElement();
                final String entryName = entry.getName();

                // Look for .class files (exclude inner classes)
                if (entryName.endsWith(".class") && !entryName.contains("$")) {
                    final String className = entryName.substring(0, entryName.length() - 6).replace('/', '.');

                    // Use very specific patterns for actual JDBC drivers
                    if (isVeryLikelyDriverClass(className)) {
                        driverClasses.add(className);
                        getLogger().debug("Found potential driver by strict pattern: {}", className);
                    }
                }
            }
        } catch (final Exception e) {
            getLogger().warn("Error scanning JAR file {} for driver classes", jarFile.getAbsolutePath(), e);
        }

        return driverClasses;
    }

    /**
     * Very strict heuristics to identify only actual JDBC driver classes
     */
    private boolean isVeryLikelyDriverClass(final String className) {
        // Must end with "Driver" and be in a reasonable package structure
        if (!className.endsWith("Driver")) {
            return false;
        }

        // Must be in a package (not default package)
        if (!className.contains(".")) {
            return false;
        }

        // Known driver class names (exact matches)
        final Set<String> knownDrivers = Set.of(
                "com.mysql.cj.jdbc.Driver",
                "com.mysql.jdbc.Driver",
                "org.postgresql.Driver",
                "oracle.jdbc.driver.OracleDriver",
                "oracle.jdbc.OracleDriver",
                "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "org.apache.derby.jdbc.EmbeddedDriver",
                "org.apache.derby.jdbc.ClientDriver",
                "org.h2.Driver",
                "org.hsqldb.jdbc.JDBCDriver",
                "org.sqlite.JDBC",
                "org.mariadb.jdbc.Driver",
                "net.sourceforge.jtds.jdbc.Driver");

        if (knownDrivers.contains(className)) {
            return true;
        }

        // Pattern-based matching for common driver structures
        final String lowerClassName = className.toLowerCase();

        // Must be in jdbc/driver related package AND end with Driver
        final boolean hasDriverPackage = lowerClassName.matches(".*\\.(jdbc|driver)\\.[^.]*driver$");

        // Known vendor patterns
        final boolean hasVendorPattern = className.matches(".*\\.(mysql|postgresql|oracle|microsoft|apache|mariadb|h2|hsqldb|sqlite|jtds)\\..*Driver$");

        return hasDriverPackage || hasVendorPattern;
    }

    /**
     * Gets JAR files from a ResourceReference for scanning
     */
    protected List<File> getJarFilesFromResource(final ResourceReference resource) {
        final List<File> jarFiles = new ArrayList<>();

        if (resource.getResourceType() == ResourceType.FILE) {
            final File file = new File(resource.getLocation());
            if (file.exists() && file.canRead() && file.getName().toLowerCase().endsWith(".jar")) {
                jarFiles.add(file);
            }
        }
        // we don't need to scan for directory since we already did a recursive flatten
        // and we don't want to deal with URLs in this case

        return jarFiles;
    }
}
