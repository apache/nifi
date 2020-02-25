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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Logger;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.cdc.CDCException;
import org.apache.nifi.cdc.postgresql.pgEasyReplication.ConnectionManager;
import org.apache.nifi.cdc.postgresql.pgEasyReplication.PGEasyReplication;
import org.apache.nifi.cdc.postgresql.pgEasyReplication.Event;
import org.apache.nifi.components.PropertyDescriptor;
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
 * A processor to retrieve Change Data Capture (CDC) events and send them as flow files.
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@PrimaryNodeOnly
@Tags({ "sql", "jdbc", "cdc", "postgresql" })
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from a PostgreSQL database. Works for PostgreSQL version 10+. CDC Events include INSERT, UPDATE, DELETE operations. Events "
        + "are output as individual flow files ordered by the time at which the operation occurred. This processor use a replication connection to stream data and sql connection to snapshot.")
@Stateful(scopes = Scope.CLUSTER, description = "Information such as a 'pointer' to the current CDC event in the database is stored by this processor, such "
        + "that it can continue from the same location if restarted.")
@WritesAttributes({
        @WritesAttribute(attribute = "last.lsn.received", description = "A Log Sequence Number  (i.e. strictly increasing integer value) specifying the order "
                + "of the CDC event flow file relative to the other event flow file(s)."),
        @WritesAttribute(attribute = "mime.type", description = "The processor outputs flow file content in JSON format, and sets the mime.type attribute to " + "application/json") })

public class CaptureChangePostgreSQL extends AbstractProcessor {
    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success").description("Successfully created FlowFile from CDC event.").build();
    // Properties
    public static final PropertyDescriptor HOST = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-host")
            .displayName("PostgreSQL Host")
            .description("A list of hostname/port entries corresponding to nodes in a PostgreSQL cluster. The entries should be comma separated "
                    + "using a colon such as host1:port,host2:port,....  For example postgresql.myhost.com:5432. This processor will attempt to connect to "
                    + "the hosts in the list in order. If one node goes down and failover is enabled for the cluster, then the processor will connect "
                    + "to the active node (assuming its host entry is specified in this property.  The default port for PostgreSQL connections is 5432.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

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
            .description("Comma-separated list of files/folders and/or URLs containing the PostgreSQL driver JAR and its dependencies (if any)."
                    + "For example '/var/tmp/postgresql-42.2.9.jar'")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DATABASE_NAME = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-database")
            .displayName("Database")
            .description("Specifies the name of the database to connect to.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-user")
            .displayName("Username")
            .description("Username to access PostgreSQL cluster.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-password")
            .displayName("Password")
            .description("Password to access PostgreSQL cluster.")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PUBLICATION = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-publication")
            .displayName("Publication")
            .description("PostgreSQL publication name. A publication is essentially a group of tables whose data changes are intended to be replicated through logical replication.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SLOT_NAME = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-slot-name")
            .displayName("Slot Name")
            .description("A unique, cluster-wide identifier for the replication slot. Each replication slot has a name, which can contain lower-case letters, numbers, "
                    + "and the underscore character. Existing replication slots and their state can be seen in the pg_replication_slots view.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SNAPSHOT = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-snapshot")
            .displayName("Make Snapshot")
            .description("The initial data in existing subscribed tables are snapshotted and copied in a parallel instance of a special kind of apply "
                    + "process. This process will create its own temporary replication slot and copy the existing data. Once existing data is copied, "
                    + "the worker enters synchronization mode, which ensures that the table is brought up to a synchronized state with the main apply "
                    + "process by streaming any changes that happened during the initial data copy using standard logical replication. Once the "
                    + "synchronization is done, the control of the replication of the table is given back to the main apply process where the replication " + "continues as normal.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_BEGIN_COMMIT = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-include-begin-commit")
            .displayName("Include Begin/Commit Events")
            .description("Specifies whether to emit events corresponding to a BEGIN or COMMIT event. Set to true if the BEGIN/COMMIT events are necessary in the downstream flow, "
                    + "otherwise set to false, which suppresses generation of these events and can increase flow performance.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INIT_LSN = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-init-lsn")
            .displayName("Initial Log Sequence Number - LSN")
            .description("Specifies an initial Log Sequence Number - LSN to use if this processor's State does not have a current "
                    + "sequence identifier. If a Log Sequence Number - LSN  is present in the processor's State, this property is ignored. Log Sequence Number - LSN are "
                    + "monotonically increasing integers that record the order of flow files generated by the processor. They can be used with the EnforceOrder "
                    + "processor to guarantee ordered delivery of CDC events.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .build();

    public static final PropertyDescriptor DROP_SLOT_IF_EXISTS = new PropertyDescriptor.Builder()
            .name("cdc-postgresql-drop-slot-if-exists")
            .displayName("Drop If Exists Replication Slot")
            .description("The initial data in existing subscribed tables are snapshotted and copied in a parallel instance of a special kind of apply "
                    + "process. This process will create its own temporary replication slot and copy the existing data. Once existing data is copied, "
                    + "the worker enters synchronization mode, which ensures that the table is brought up to a synchronized state with the main apply "
                    + "process by streaming any changes that happened during the initial data copy using standard logical replication. Once the "
                    + "synchronization is done, the control of the replication of the table is given back to the main apply process where the replication " + "continues as normal.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    // Attribute keys
    public static final String MIME_TYPE_VALUE = "application/json";
    public static final String LAST_LSN_RECEIVED = "last.lsn.received";

    // Attributes of Processor
    private AtomicBoolean hasRun = new AtomicBoolean(false);
    private volatile Long lastLSNReceived = null;
    private volatile Boolean includeBeginCommit = false;
    private Long initialLSN = 0L;
    private PGEasyReplication pgEasyReplication = null;
    boolean hasSnapshot = false;
    private ConnectionManager connectionManager;

    // update lastLSNReceived on processor state
    private void updateState(StateManager stateManager) throws IOException {
        // Update state with latest values
        if (stateManager != null) {
            Map<String, String> newStateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());

            newStateMap.put(LAST_LSN_RECEIVED, String.valueOf(this.lastLSNReceived));
            stateManager.setState(newStateMap, Scope.CLUSTER);
        }
    }

    /**
     * using Thread.currentThread().getContextClassLoader(); will ensure that you are using the ClassLoader for you NAR.
     *
     * @throws InitializationException
     *             if there is a problem obtaining the ClassLoader
     */
    protected void registerDriver(String locationString, String drvName) throws InitializationException {
        if (locationString != null && locationString.length() > 0) {
            try {
                // Split and trim the entries
                final ClassLoader classLoader = ClassLoaderUtils.getCustomClassLoader(locationString, this.getClass().getClassLoader(), (dir, name) -> name != null && name.endsWith(".jar"));

                // Workaround which allows to use URLClassLoader for JDBC driver loading.
                // (Because the DriverManager will refuse to use a driver not loaded by the
                // system ClassLoader.)
                final Class<?> clazz = Class.forName(drvName, true, classLoader);
                if (clazz == null) {
                    throw new InitializationException("Can't load Database Driver " + drvName + " location" + locationString + "classLoader" + classLoader.toString());
                }
                final Driver driver = (Driver) clazz.newInstance();
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

    protected void stop(StateManager stateManager) throws CDCException {
        try {
            this.connectionManager.closeReplicationConnection();
            this.connectionManager.closeSQLConnection();
            this.pgEasyReplication = null;

            if (this.lastLSNReceived != null)
                updateState(stateManager);
        } catch (Exception e) {
            throw new CDCException("Error closing CDC connection", e);
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
            // disconnect, and shut down gracefully
            stop(context.getStateManager());
        } catch (CDCException ioe) {
            throw new ProcessException(ioe);
        }
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(HOST);
        descriptors.add(DRIVER_NAME);
        descriptors.add(DRIVER_LOCATION);
        descriptors.add(DATABASE_NAME);
        descriptors.add(USERNAME);
        descriptors.add(PASSWORD);
        descriptors.add(PUBLICATION);
        descriptors.add(SLOT_NAME);
        descriptors.add(SNAPSHOT);
        descriptors.add(INCLUDE_BEGIN_COMMIT);
        descriptors.add(INIT_LSN);
        descriptors.add(DROP_SLOT_IF_EXISTS);

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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    // runs the initial configuration of processor
    public void setup(ProcessContext context) {

        final ComponentLog logger = getLogger();
        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        final String host = context.getProperty(HOST).evaluateAttributeExpressions().getValue();
        final String database = context.getProperty(DATABASE_NAME).evaluateAttributeExpressions().getValue();
        final String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
        final boolean dropSlotIfExists = context.getProperty(DROP_SLOT_IF_EXISTS).evaluateAttributeExpressions().asBoolean();
        final String publicationName = context.getProperty(PUBLICATION).evaluateAttributeExpressions().getValue();
        final String replicationSlotName = context.getProperty(SLOT_NAME).evaluateAttributeExpressions().getValue();

        // Save off PostgreSQL cluster and JDBC driver information, will be used to
        // connect for event
        final String driverName = context.getProperty(DRIVER_NAME).evaluateAttributeExpressions().getValue();
        final String driverLocation = context.getProperty(DRIVER_LOCATION).evaluateAttributeExpressions().getValue();

        String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

        // Connection expects a non-null password, so set it to the empty string if it
        // is not provided
        if (password == null) {
            password = "";
        }

        this.hasSnapshot = context.getProperty(SNAPSHOT).asBoolean();
        this.includeBeginCommit = context.getProperty(INCLUDE_BEGIN_COMMIT).asBoolean();
        this.initialLSN = context.getProperty(INIT_LSN).asLong() == null ? null : context.getProperty(INIT_LSN).asLong();

        try {

            // Ensure driverLocation and driverName are correct before establishing
            // PostgreSQL connection to avoid failing after PostgreSQL messages are received.
            // Actual JDBC connection is created after PostgreSQL client gets started,
            // because we need the connect-able host same as the PostgreSQL client.
            registerDriver(driverLocation, driverName);
            this.connectionManager = new ConnectionManager();
            this.connectionManager.setProperties(host, database, username, password, driverName);
            this.connectionManager.createSQLConnection();
            this.connectionManager.createReplicationConnection();

            stateMap = stateManager.getState(Scope.CLUSTER);

            this.pgEasyReplication = new PGEasyReplication(publicationName, replicationSlotName, dropSlotIfExists, this.connectionManager);
            this.pgEasyReplication.initializeLogicalReplication();

            if (stateMap.get(LAST_LSN_RECEIVED) != null) {
                this.lastLSNReceived = Long.parseLong(stateMap.get(LAST_LSN_RECEIVED));
                this.hasRun.set(true);
            } else {
                this.hasRun.set(false);
            }

        } catch (final Exception ioe) {
            logger.error("Failed to retrieve values from the State Manager. Will not attempt " + "connection until this is accomplished.", ioe);
            context.yield();
            throw new ProcessException(ioe.getMessage(), ioe);
        }
    }

    List<Event> getData() {

        List<Event> events = new ArrayList<>();

        if (!this.hasRun.get() && this.hasSnapshot) {
            events.add(this.pgEasyReplication.getSnapshot());
        }

        if (!this.hasRun.get() && this.initialLSN != null) {
            events.add(this.pgEasyReplication.readEvent(true, this.includeBeginCommit, initialLSN));
        }

        events.add(this.pgEasyReplication.readEvent(true, this.includeBeginCommit));

        if (events != null)
            this.hasRun.set(true);

        return events;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        // Indicate that this processor has executed at least once, so we know whether
        // or not the state values are valid and should be updated
        if (this.pgEasyReplication == null) {
            setup(context);
        }

        List<FlowFile> listFlowFiles = new ArrayList<>();

        List<Event> events = getData();

        if (events == null) {
            logger.error("Failed to retrieve events.");
            context.yield();
            throw new ProcessException();
        }

        for (Event event : events) {
            if (event != null) {
                LinkedList<String> listData = event.getData();

                if (!listData.isEmpty()) {
                    for (String data : listData) {
                        FlowFile flowFile = null;
                        flowFile = session.create();

                        flowFile = session.write(flowFile, new OutputStreamCallback() {
                            @Override
                            public void process(final OutputStream out) throws IOException {
                                out.write(data.getBytes(StandardCharsets.UTF_8));
                            }
                        });

                        flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), MIME_TYPE_VALUE);
                        listFlowFiles.add(flowFile);
                    }
                }

                this.lastLSNReceived = event.getLastLSN();
            }
        }

        session.transfer(listFlowFiles, REL_SUCCESS);
        // getLogger().trace("...");

    }
}