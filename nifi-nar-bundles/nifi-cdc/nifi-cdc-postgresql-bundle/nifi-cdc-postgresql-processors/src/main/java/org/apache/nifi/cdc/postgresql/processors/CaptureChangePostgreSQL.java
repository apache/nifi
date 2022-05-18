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

package org.apache.nifi.cdc.postgresql.processors;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.cdc.CDCException;
import org.apache.nifi.cdc.postgresql.event.Reader;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

/**
 * A processor to Capture Data Change (CDC) events from a PostgreSQL database
 * and return them as flow files.
 */
@TriggerSerially
@PrimaryNodeOnly
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)

@Tags({ "sql", "jdbc", "cdc", "postgresql" })

@CapabilityDescription("Retrieves Change Data Capture (CDC) events from a PostgreSQL database. Works for PostgreSQL version 10+. "
        + "Events include INSERT, UPDATE, and DELETE operations and are output as individual flow files ordered by the time at which the operation occurred. "
        + "This processor uses Replication Connection to stream data. By default, an existing Logical Replication Slot with the specified name will be used or, "
        + "if none exists, a new one will be created. In the case of an existing slot, make sure that pgoutput is the output plugin. "
        + "This processor also uses SQL Connection to query system views. In addition, a Publication in PostgreSQL database should already exist.")

@Stateful(scopes = Scope.CLUSTER, description = "The last received Log Sequence Number (LSN) from Replication Slot is stored by this processor, "
        + "such that it can continue from the same location if restarted.")

@WritesAttributes({
        @WritesAttribute(attribute = "cdc.type", description = "The CDC event type, as begin, commit, insert, update, delete, etc."),
        @WritesAttribute(attribute = "cdc.lsn", description = "The Log Sequence Number (i.e. strictly increasing integer value) specifying the order "
                + "of the CDC event flow file relative to the other event flow files."),
        @WritesAttribute(attribute = "mime.type", description = "The processor outputs flow file content in JSON format, and sets the mime.type attribute to application/json.") })

public class CaptureChangePostgreSQL extends AbstractProcessor {
    // Constants
    private static final String MIME_TYPE_DEFAULT = "application/json";
    private static final String LAST_LSN_STATE_MAP_KEY = "last.received.lsn";
    private static final String POSTGRESQL_MIN_VERSION = "10";
    private static final String POSTGRESQL_PORT_DEFAULT = "5432";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
            .description("Successfully created FlowFile from CDC event.").build();

    private Set<Relationship> relationships;

    // Properties
    public static final PropertyDescriptor DRIVER_NAME = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-driver-class")
            .displayName("PostgreSQL Driver Class Name")
            .description("The class name of the PostgreSQL database driver class")
            .defaultValue("org.postgresql.Driver")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-driver-locations")
            .displayName("PostgreSQL Driver Location(s)")
            .description(
                    "Comma-separated list of files/folders containing the PostgreSQL driver JAR and its dependencies (if any). For example '/usr/share/java/postgresql-42.3.1.jar'")
            .defaultValue(null)
            .required(false)
            .identifiesExternalResource(ResourceCardinality.MULTIPLE, ResourceType.FILE)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-host")
            .displayName("PostgreSQL Hostname")
            .description("The hostname for PostgreSQL connections.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-port")
            .displayName("PostgreSQL Port")
            .description("The default port for PostgreSQL connections.")
            .required(true)
            .defaultValue(POSTGRESQL_PORT_DEFAULT)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DATABASE = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-database")
            .displayName("Database")
            .description("Specifies the name of the database to connect to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-user")
            .displayName("Username")
            .description("Username to access the PostgreSQL database. Must be a superuser.")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-password")
            .displayName("Password")
            .description("Password to access the PostgreSQL database.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-max-wait-time").displayName("Max Wait Time")
            .description(
                    "The maximum amount of time allowed for a connection to be established, zero means there is effectively no limit.")
            .defaultValue("30 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PUBLICATION = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-publication")
            .displayName("Publication Name")
            .description(
                    "A group of tables whose data changes are intended to be replicated through Logical Replication. "
                            + "It should be created in the database before the processor starts.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor REPLICATION_SLOT = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-slot-name")
            .displayName("Replication Slot Name")
            .description(
                    "A unique, cluster-wide identifier for the PostgreSQL Replication Slot. "
                            + "If it already exists, make sure that pgoutput is the output plugin.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DROP_SLOT_IF_EXISTS = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-drop-slot-if-exists")
            .displayName("Drop if exists replication slot?")
            .description(
                    "Drop the Replication Slot in PostgreSQL database if it already exists every processor starts.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_BEGIN_COMMIT = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-include-begin-commit")
            .displayName("Include BEGIN and COMMIT statements?")
            .description(
                    "Specifies whether to emit events including BEGIN and COMMIT statements. Set to true if the BEGIN and COMMIT statements are necessary in the downstream flow, "
                            + "otherwise set to false, which suppresses these statements and can increase flow performance.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_ALL_METADATA = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-include-all-metadata")
            .displayName("Include all metadata?")
            .description(
                    "Specifies whether to emit events including all message types (BEGIN, COMMIT, RELATION, TYPE, etc) and all metadata (relation id, tuple type, tuple data before update, etc). "
                            + "Set to true if all metadata are necessary in the downstream flow, otherwise set to false, which suppresses these metadata and can increase flow performance.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor START_LSN = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-start-lsn")
            .displayName("Start Log Sequence Number (LSN)")
            .description(
                    "Specifies a start Log Sequence Number (LSN) to use if this processor's state does not have a current "
                            + "sequence identifier. If a LSN is present in the processor's state, this property is ignored.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    // Key Attributes
    private List<PropertyDescriptor> descriptors;
    private JDBCConnectionHolder queryConnHolder = null;
    private JDBCConnectionHolder replicationConnHolder = null;
    private volatile Long lastLSN = null;
    protected Reader replicationReader = null;
    protected Long maxFlowFileListSize = 100L;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(DRIVER_NAME);
        descriptors.add(DRIVER_LOCATION);
        descriptors.add(HOST);
        descriptors.add(PORT);
        descriptors.add(DATABASE);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(CONNECTION_TIMEOUT);
        descriptors.add(PUBLICATION);
        descriptors.add(REPLICATION_SLOT);
        descriptors.add(DROP_SLOT_IF_EXISTS);
        descriptors.add(INCLUDE_BEGIN_COMMIT);
        descriptors.add(INCLUDE_ALL_METADATA);
        descriptors.add(START_LSN);

        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);

        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    // Runs the initial configuration of processor.
    public void setup(ProcessContext context) {
        final ComponentLog logger = getLogger();

        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            logger.error(
                    "Failed to retrieve observed maximum values from the State Manager. Will not attempt connection until this is accomplished.",
                    ioe);
            context.yield();
            return;
        }

        final String driverName = context.getProperty(DRIVER_NAME).evaluateAttributeExpressions().getValue();
        final String driverLocation = context.getProperty(DRIVER_LOCATION).evaluateAttributeExpressions().getValue();

        final String host = context.getProperty(HOST).evaluateAttributeExpressions().getValue();
        final String port = context.getProperty(PORT).evaluateAttributeExpressions().getValue();
        final String database = context.getProperty(DATABASE).evaluateAttributeExpressions().getValue();
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();
        final Long connectionTimeout = context.getProperty(CONNECTION_TIMEOUT).evaluateAttributeExpressions()
                .asTimePeriod(TimeUnit.MILLISECONDS);

        final String publication = context.getProperty(PUBLICATION).evaluateAttributeExpressions().getValue();
        final String slot = context.getProperty(REPLICATION_SLOT).evaluateAttributeExpressions().getValue();
        final Boolean dropSlotIfExists = context.getProperty(DROP_SLOT_IF_EXISTS).asBoolean();
        final Boolean includeBeginCommit = context.getProperty(INCLUDE_BEGIN_COMMIT).asBoolean();
        final Boolean includeAllMetadata = context.getProperty(INCLUDE_ALL_METADATA).asBoolean();
        final Long startLSN = context.getProperty(START_LSN).asLong();

        if (stateMap.get(LAST_LSN_STATE_MAP_KEY) != null && Long.parseLong(stateMap.get(LAST_LSN_STATE_MAP_KEY)) > 0L) {
            this.lastLSN = Long.parseLong(stateMap.get(LAST_LSN_STATE_MAP_KEY));
        } else {
            if (startLSN != null) {
                this.lastLSN = startLSN;
            }
        }

        try {
            this.connect(host, port, database, username, password, driverLocation, driverName, connectionTimeout);
            this.createReplicationReader(slot, dropSlotIfExists, publication, lastLSN, includeBeginCommit,
                    includeAllMetadata, this.getReplicationConnection(), this.getQueryConnection());

        } catch (final IOException | TimeoutException | SQLException | RuntimeException e) {
            context.yield();
            this.closeQueryConnection();
            this.closeReplicationConnection();
            throw new ProcessException(e.getMessage(), e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        if (this.replicationReader == null || this.replicationReader.getReplicationStream() == null) {
            setup(context);
        }

        try {
            List<FlowFile> listFlowFiles = new ArrayList<>();

            while (listFlowFiles.size() < this.getMaxFlowFileListSize()) {
                HashMap<String, Object> message = this.replicationReader.readMessage();

                if (message == null) { // No more messages.
                    break;
                }

                if (message.isEmpty()) { // Skip empty messages.
                    continue;
                }

                String data = this.replicationReader.convertMessageToJSON(message);

                FlowFile flowFile = null;
                flowFile = session.create();

                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(data.getBytes(StandardCharsets.UTF_8));
                    }
                });

                flowFile = session.putAttribute(flowFile, "cdc.type", message.get("type").toString());

                // Some messagens don't have LSN (e.g. Relation).
                if (message.containsKey("lsn")) {
                    flowFile = session.putAttribute(flowFile, "cdc.lsn", message.get("lsn").toString());
                }

                flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), MIME_TYPE_DEFAULT);

                listFlowFiles.add(flowFile);
            }

            session.transfer(listFlowFiles, REL_SUCCESS);

            this.lastLSN = this.replicationReader.getLastReceiveLSN();

            // Feedback is sent after the flowfiles transfer completes.
            this.replicationReader.sendFeedback(this.lastLSN);

            updateState(context.getStateManager());

        } catch (SQLException | IOException e) {
            context.yield();
            this.closeQueryConnection();
            this.closeReplicationConnection();
            throw new ProcessException(e.getMessage(), e);
        }
    }

    // Update last LSN received on processor state.
    private void updateState(StateManager stateManager) throws IOException {
        if (stateManager != null) {
            Map<String, String> newStateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());

            newStateMap.put(LAST_LSN_STATE_MAP_KEY, String.valueOf(this.lastLSN));
            stateManager.setState(newStateMap, Scope.CLUSTER);
        }
    }

    protected void stop(StateManager stateManager) throws CDCException {
        try {
            this.replicationReader = null;

            if (lastLSN != null) {
                updateState(stateManager);
            }

            if (queryConnHolder != null) {
                queryConnHolder.close();
            }

            if (replicationConnHolder != null) {
                replicationConnHolder.close();
            }
        } catch (Exception e) {
            throw new CDCException("Closing CDC Connection failed", e);
        }
    }

    @OnStopped
    public void onStopped(ProcessContext context) {
        try {
            stop(context.getStateManager());
        } catch (CDCException ioe) {
            throw new ProcessException(ioe);
        }
    }

    @OnShutdown
    public void onShutdown(ProcessContext context) {
        try {
            // In case we get shutdown while still running, save off the current state,
            // disconnect, and shut down gracefully.
            stop(context.getStateManager());
        } catch (CDCException ioe) {
            throw new ProcessException(ioe);
        }
    }

    protected void createReplicationReader(String slot, boolean dropSlotIfExists, String publication, Long lsn,
            boolean includeBeginCommit, boolean includeAllMetadata, Connection replicationConn,
            Connection queryConn) throws SQLException {
        this.replicationReader = new Reader(slot, dropSlotIfExists, publication, lsn, includeBeginCommit,
                includeAllMetadata, replicationConn, queryConn);
    }

    protected Long getMaxFlowFileListSize() {
        return this.maxFlowFileListSize;
    }

    protected void connect(String host, String port, String database, String username, String password,
            String driverLocation, String driverName, long connectionTimeout)
            throws IOException, TimeoutException {
        try {
            // Ensure driverLocation and driverName are correct
            // before establishing connection.
            registerDriver(driverLocation, driverName);
        } catch (InitializationException e) {
            throw new RuntimeException(
                    "Failed to register JDBC driver. Ensure PostgreSQL Driver Location(s) and PostgreSQL Driver Class Name "
                            + "are configured correctly",
                    e);
        }

        // Connection expects a non-null password.
        if (password == null) {
            password = "";
        }

        // Connection expects a timeout.
        if (connectionTimeout == 0) {
            connectionTimeout = Long.MAX_VALUE;
        }

        InetSocketAddress address = getAddress(host, port);

        queryConnHolder = new JDBCConnectionHolder(address, database, username, password, false, connectionTimeout);
        try {
            // Ensure Query connection can be created.
            this.getQueryConnection();
        } catch (SQLException e) {
            throw new IOException("Failed to create SQL connection to specified host and port", e);
        }

        replicationConnHolder = new JDBCConnectionHolder(address, database, username, password, true,
                connectionTimeout);
        try {
            // Ensure Replication connection can be created.
            this.getReplicationConnection();
        } catch (SQLException e) {
            throw new IOException("Failed to create Replication connection to specified host and port", e);
        }
    }

    protected Connection getQueryConnection() throws SQLException {
        return queryConnHolder.getConnection();
    }

    protected Connection getReplicationConnection() throws SQLException {
        return replicationConnHolder.getConnection();
    }

    protected void closeQueryConnection() {
        if (queryConnHolder != null)
            queryConnHolder.close();
    }

    protected void closeReplicationConnection() {
        if (replicationConnHolder != null)
            replicationConnHolder.close();
    }

    /**
     * Ensures that you are using the ClassLoader for you NAR.
     *
     * @param driverLocation
     *                       Driver's location.
     *
     * @param driverName
     *                       Driver's class name.
     *
     * @throws InitializationException
     *                                 if there is a problem obtaining the
     *                                 ClassLoader
     **/
    protected void registerDriver(String driverLocation, String driverName) throws InitializationException {
        if (driverLocation != null && driverLocation.length() > 0) {
            try {
                // Split and trim the entries
                final ClassLoader classLoader = ClassLoaderUtils.getCustomClassLoader(driverLocation,
                        this.getClass().getClassLoader(), (dir, name) -> name != null && name.endsWith(".jar"));

                final Class<?> clazz = Class.forName(driverName, true, classLoader);
                if (clazz == null) {
                    throw new InitializationException("Can't load Database Driver " + driverName);
                }
                final Driver driver = (Driver) clazz.getDeclaredConstructor().newInstance();
                DriverManager.registerDriver(new DriverShim(driver));

            } catch (final InitializationException e) {
                throw e;
            } catch (final MalformedURLException e) {
                throw new InitializationException("Invalid Database Driver Jar Url", e);
            } catch (final Exception e) {
                throw new InitializationException("Can't load Database Driver", e);
            }
        }
    }

    /**
     * Returns the Address from HOST and PORT properties.
     *
     * @param host
     *             Hostname of PostgreSQL Cluster.
     * @param port
     *             Port that PostgreSQL Cluster is listening on.
     *
     * @return InetSocketAddresses
     */
    protected InetSocketAddress getAddress(String host, String port) {
        if (host == null || host.trim().isEmpty()) {
            return null;
        }

        if (port == null || port.trim().isEmpty()) {
            port = POSTGRESQL_PORT_DEFAULT;
        }

        return new InetSocketAddress(host.trim(), Integer.parseInt(port.trim()));
    }

    private class JDBCConnectionHolder {
        private final String connectionUrl;
        private final Properties connectionProps = new Properties();
        private final long connectionTimeoutMillis;

        private Connection connection;
        private boolean isReplicationConnection = false;

        private JDBCConnectionHolder(InetSocketAddress address, String database, String username, String password,
                boolean isReplicationConnection, long connectionTimeoutMillis) {
            this.connectionUrl = "jdbc:postgresql://" + address.getHostString() + ":" + address.getPort() + "/"
                    + database;
            this.connectionTimeoutMillis = connectionTimeoutMillis;

            if (isReplicationConnection) {
                this.isReplicationConnection = isReplicationConnection;
                this.connectionProps.put("assumeMinServerVersion", POSTGRESQL_MIN_VERSION);
                this.connectionProps.put("replication", "database");
                this.connectionProps.put("preferQueryMode", "simple");
            }

            this.connectionProps.put("user", username);
            this.connectionProps.put("password", password);
        }

        private Connection getConnection() throws SQLException {
            if (connection != null && connection.isValid((int) (connectionTimeoutMillis / 1000))) {
                getLogger().trace("Returning the pooled JDBC connection.");
                return connection;
            }

            // Close the existing connection just in case.
            close();

            getLogger().trace("Creating a new JDBC connection.");
            connection = DriverManager.getConnection(connectionUrl, connectionProps);

            // Set auto commit for query connection.
            if (!isReplicationConnection) {
                connection.setAutoCommit(true);
            }

            return connection;
        }

        private void close() {
            if (connection != null) {
                try {
                    getLogger().trace("Closing the pooled JDBC connection.");
                    connection.close();
                } catch (SQLException e) {
                    getLogger().warn("Failed to close JDBC connection due to " + e, e);
                }
            }
        }
    }

    private static class DriverShim implements Driver {
        private Driver driver;

        DriverShim(Driver d) {
            this.driver = d;
        }

        @Override
        public boolean acceptsURL(String u) throws SQLException {
            return this.driver.acceptsURL(u);
        }

        @Override
        public Connection connect(String u, Properties p) throws SQLException {
            return this.driver.connect(u, p);
        }

        @Override
        public int getMajorVersion() {
            return this.driver.getMajorVersion();
        }

        @Override
        public int getMinorVersion() {
            return this.driver.getMinorVersion();
        }

        @Override
        public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
            return this.driver.getPropertyInfo(u, p);
        }

        @Override
        public boolean jdbcCompliant() {
            return this.driver.jdbcCompliant();
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            return driver.getParentLogger();
        }
    }
}