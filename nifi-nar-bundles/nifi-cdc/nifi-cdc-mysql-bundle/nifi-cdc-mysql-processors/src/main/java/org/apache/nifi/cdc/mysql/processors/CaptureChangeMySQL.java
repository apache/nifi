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
package org.apache.nifi.cdc.mysql.processors;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.github.shyiko.mysql.binlog.event.QueryEventData;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import com.github.shyiko.mysql.binlog.event.TableMapEventData;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.cdc.CDCException;
import org.apache.nifi.cdc.event.ColumnDefinition;
import org.apache.nifi.cdc.event.RowEventException;
import org.apache.nifi.cdc.event.TableInfo;
import org.apache.nifi.cdc.event.TableInfoCacheKey;
import org.apache.nifi.cdc.event.io.EventWriter;
import org.apache.nifi.cdc.mysql.event.BeginTransactionEventInfo;
import org.apache.nifi.cdc.mysql.event.BinlogEventInfo;
import org.apache.nifi.cdc.mysql.event.BinlogEventListener;
import org.apache.nifi.cdc.mysql.event.BinlogLifecycleListener;
import org.apache.nifi.cdc.mysql.event.CommitTransactionEventInfo;
import org.apache.nifi.cdc.mysql.event.DeleteRowsEventInfo;
import org.apache.nifi.cdc.mysql.event.InsertRowsEventInfo;
import org.apache.nifi.cdc.mysql.event.RawBinlogEvent;
import org.apache.nifi.cdc.mysql.event.DDLEventInfo;
import org.apache.nifi.cdc.mysql.event.UpdateRowsEventInfo;
import org.apache.nifi.cdc.mysql.event.io.BeginTransactionEventWriter;
import org.apache.nifi.cdc.mysql.event.io.CommitTransactionEventWriter;
import org.apache.nifi.cdc.mysql.event.io.DeleteRowsWriter;
import org.apache.nifi.cdc.mysql.event.io.InsertRowsWriter;
import org.apache.nifi.cdc.mysql.event.io.DDLEventWriter;
import org.apache.nifi.cdc.mysql.event.io.UpdateRowsWriter;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import static com.github.shyiko.mysql.binlog.event.EventType.DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.EXT_DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.EXT_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.FORMAT_DESCRIPTION;
import static com.github.shyiko.mysql.binlog.event.EventType.PRE_GA_DELETE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.PRE_GA_WRITE_ROWS;
import static com.github.shyiko.mysql.binlog.event.EventType.ROTATE;
import static com.github.shyiko.mysql.binlog.event.EventType.WRITE_ROWS;


/**
 * A processor to retrieve Change Data Capture (CDC) events and send them as flow files.
 */
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "jdbc", "cdc", "mysql"})
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from a MySQL database. CDC Events include INSERT, UPDATE, DELETE operations. Events "
        + "are output as individual flow files ordered by the time at which the operation occurred.")
@Stateful(scopes = Scope.CLUSTER, description = "Information such as a 'pointer' to the current CDC event in the database is stored by this processor, such "
        + "that it can continue from the same location if restarted.")
@WritesAttributes({
        @WritesAttribute(attribute = EventWriter.SEQUENCE_ID_KEY, description = "A sequence identifier (i.e. strictly increasing integer value) specifying the order "
                + "of the CDC event flow file relative to the other event flow file(s)."),
        @WritesAttribute(attribute = EventWriter.CDC_EVENT_TYPE_ATTRIBUTE, description = "A string indicating the type of CDC event that occurred, including (but not limited to) "
                + "'begin', 'insert', 'update', 'delete', 'ddl' and 'commit'."),
        @WritesAttribute(attribute = "mime.type", description = "The processor outputs flow file content in JSON format, and sets the mime.type attribute to "
                + "application/json")
})
public class CaptureChangeMySQL extends AbstractSessionFactoryProcessor {

    // Random invalid constant used as an indicator to not set the binlog position on the client (thereby using the latest available)
    private static final int DO_NOT_SET = -1000;

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    protected static Set<Relationship> relationships;

    // Properties
    public static final PropertyDescriptor DATABASE_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-db-name-pattern")
            .displayName("Database/Schema Name Pattern")
            .description("A regular expression (regex) for matching databases (or schemas, depending on your RDBMS' terminology) against the list of CDC events. The regex must match "
                    + "the database name as it is stored in the RDBMS. If the property is not set, the database name will not be used to filter the CDC events. "
                    + "NOTE: DDL events, even if they affect different databases, are associated with the database used by the session to execute the DDL. "
                    + "This means if a connection is made to one database, but the DDL is issued against another, then the connected database will be the one matched against "
                    + "the specified pattern.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TABLE_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-name-pattern")
            .displayName("Table Name Pattern")
            .description("A regular expression (regex) for matching CDC events affecting matching tables. The regex must match the table name as it is stored in the database. "
                    + "If the property is not set, no events will be filtered based on table name.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-max-wait-time")
            .displayName("Max Wait Time")
            .description("The maximum amount of time allowed for a connection to be established, zero means there is effectively no limit.")
            .defaultValue("30 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor HOSTS = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-hosts")
            .displayName("MySQL Hosts")
            .description("A list of hostname/port entries corresponding to nodes in a MySQL cluster. The entries should be comma separated "
                    + "using a colon such as host1:port,host2:port,....  For example mysql.myhost.com:3306. This processor will attempt to connect to "
                    + "the hosts in the list in order. If one node goes down and failover is enabled for the cluster, then the processor will connect "
                    + "to the active node (assuming its host entry is specified in this property.  The default port for MySQL connections is 3306.")
            .required(true)
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DRIVER_NAME = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-driver-class")
            .displayName("MySQL Driver Class Name")
            .description("The class name of the MySQL database driver class")
            .defaultValue("com.mysql.jdbc.Driver")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DRIVER_LOCATION = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-driver-locations")
            .displayName("MySQL Driver Location(s)")
            .description("Comma-separated list of files/folders and/or URLs containing the MySQL driver JAR and its dependencies (if any). "
                    + "For example '/var/tmp/mysql-connector-java-5.1.38-bin.jar'")
            .defaultValue(null)
            .required(false)
            .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-username")
            .displayName("Username")
            .description("Username to access the MySQL cluster")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-password")
            .displayName("Password")
            .description("Password to access the MySQL cluster")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor SERVER_ID = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-server-id")
            .displayName("Server ID")
            .description("The client connecting to the MySQL replication group is actually a simplified slave (server), and the Server ID value must be unique across the whole replication "
                    + "group (i.e. different from any other Server ID being used by any master or slave). Thus, each instance of CaptureChangeMySQL must have a Server ID unique across "
                    + "the replication group. If the Server ID is not specified, it defaults to 65535.")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor DIST_CACHE_CLIENT = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-dist-map-cache-client")
            .displayName("Distributed Map Cache Client")
            .description("Identifies a Distributed Map Cache Client controller service to be used for keeping information about the various tables, columns, etc. "
                    + "needed by the processor. If a client is not specified, the generated events will not include column type or name information.")
            .identifiesControllerService(DistributedMapCacheClient.class)
            .required(false)
            .build();

    public static final PropertyDescriptor RETRIEVE_ALL_RECORDS = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-retrieve-all-records")
            .displayName("Retrieve All Records")
            .description("Specifies whether to get all available CDC events, regardless of the current binlog filename and/or position. If binlog filename and position values are present "
                    + "in the processor's State, this property's value is ignored. This allows for 4 different configurations: 1) If binlog data is available in processor State, that is used "
                    + "to determine the start location and the value of Retrieve All Records is ignored. 2) If no binlog data is in processor State, then Retrieve All Records set to true "
                    + "means start at the beginning of the binlog history. 3) If no binlog data is in processor State and Initial Binlog Filename/Position are not set, then "
                    + "Retrieve All Records set to false means start at the end of the binlog history. 4) If no binlog data is in processor State and Initial Binlog Filename/Position "
                    + "are set, then Retrieve All Records set to false means start at the specified initial binlog file/position. "
                    + "To reset the behavior, clear the processor state (refer to the State Management section of the processor's documentation).")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("true")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_BEGIN_COMMIT = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-include-begin-commit")
            .displayName("Include Begin/Commit Events")
            .description("Specifies whether to emit events corresponding to a BEGIN or COMMIT event in the binary log. Set to true if the BEGIN/COMMIT events are necessary in the downstream flow, "
                    + "otherwise set to false, which suppresses generation of these events and can increase flow performance.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor INCLUDE_DDL_EVENTS = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-include-ddl-events")
            .displayName("Include DDL Events")
            .description("Specifies whether to emit events corresponding to Data Definition Language (DDL) events such as ALTER TABLE, TRUNCATE TABLE, e.g. in the binary log. Set to true "
                    + "if the DDL events are desired/necessary in the downstream flow, otherwise set to false, which suppresses generation of these events and can increase flow performance.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final PropertyDescriptor STATE_UPDATE_INTERVAL = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-state-update-interval")
            .displayName("State Update Interval")
            .description("Indicates how often to update the processor's state with binlog file/position values. A value of zero means that state will only be updated when the processor is "
                    + "stopped or shutdown. If at some point the processor state does not contain the desired binlog values, the last flow file emitted will contain the last observed values, "
                    + "and the processor can be returned to that state by using the Initial Binlog File, Initial Binlog Position, and Initial Sequence ID properties.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor INIT_SEQUENCE_ID = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-init-seq-id")
            .displayName("Initial Sequence ID")
            .description("Specifies an initial sequence identifier to use if this processor's State does not have a current "
                    + "sequence identifier. If a sequence identifier is present in the processor's State, this property is ignored. Sequence identifiers are "
                    + "monotonically increasing integers that record the order of flow files generated by the processor. They can be used with the EnforceOrder "
                    + "processor to guarantee ordered delivery of CDC events.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor INIT_BINLOG_FILENAME = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-init-binlog-filename")
            .displayName("Initial Binlog Filename")
            .description("Specifies an initial binlog filename to use if this processor's State does not have a current binlog filename. If a filename is present "
                    + "in the processor's State, this property is ignored. This can be used along with Initial Binlog Position to \"skip ahead\" if previous events are not desired. "
                    + "Note that NiFi Expression Language is supported, but this property is evaluated when the processor is configured, so FlowFile attributes may not be used. Expression "
                    + "Language is supported to enable the use of the Variable Registry and/or environment properties.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor INIT_BINLOG_POSITION = new PropertyDescriptor.Builder()
            .name("capture-change-mysql-init-binlog-position")
            .displayName("Initial Binlog Position")
            .description("Specifies an initial offset into a binlog (specified by Initial Binlog Filename) to use if this processor's State does not have a current "
                    + "binlog filename. If a filename is present in the processor's State, this property is ignored. This can be used along with Initial Binlog Filename "
                    + "to \"skip ahead\" if previous events are not desired. Note that NiFi Expression Language is supported, but this property is evaluated when the "
                    + "processor is configured, so FlowFile attributes may not be used. Expression Language is supported to enable the use of the Variable Registry "
                    + "and/or environment properties.")
            .required(false)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static List<PropertyDescriptor> propDescriptors;

    private volatile ProcessSession currentSession;
    private BinaryLogClient binlogClient;
    private BinlogEventListener eventListener;
    private BinlogLifecycleListener lifecycleListener;

    private volatile LinkedBlockingQueue<RawBinlogEvent> queue = new LinkedBlockingQueue<>();
    private volatile String currentBinlogFile = null;
    private volatile long currentBinlogPosition = 4;

    // The following variables save the value of the binlog filename and position (and sequence id) at the beginning of a transaction. Used for rollback
    private volatile String xactBinlogFile = null;
    private volatile long xactBinlogPosition = 4;
    private volatile long xactSequenceId = 0;

    private volatile TableInfo currentTable = null;
    private volatile String currentDatabase = null;
    private volatile Pattern databaseNamePattern;
    private volatile Pattern tableNamePattern;
    private volatile boolean includeBeginCommit = false;
    private volatile boolean includeDDLEvents = false;

    private volatile boolean inTransaction = false;
    private volatile boolean skipTable = false;
    private AtomicBoolean doStop = new AtomicBoolean(false);
    private AtomicBoolean hasRun = new AtomicBoolean(false);

    private int currentHost = 0;
    private String transitUri = "<unknown>";

    private volatile long lastStateUpdate = 0L;
    private volatile long stateUpdateInterval = -1L;
    private AtomicLong currentSequenceId = new AtomicLong(0);

    private volatile DistributedMapCacheClient cacheClient = null;
    private final Serializer<TableInfoCacheKey> cacheKeySerializer = new TableInfoCacheKey.Serializer();
    private final Serializer<TableInfo> cacheValueSerializer = new TableInfo.Serializer();
    private final Deserializer<TableInfo> cacheValueDeserializer = new TableInfo.Deserializer();

    private JDBCConnectionHolder jdbcConnectionHolder = null;

    private final BeginTransactionEventWriter beginEventWriter = new BeginTransactionEventWriter();
    private final CommitTransactionEventWriter commitEventWriter = new CommitTransactionEventWriter();
    private final DDLEventWriter ddlEventWriter = new DDLEventWriter();
    private final InsertRowsWriter insertRowsWriter = new InsertRowsWriter();
    private final DeleteRowsWriter deleteRowsWriter = new DeleteRowsWriter();
    private final UpdateRowsWriter updateRowsWriter = new UpdateRowsWriter();

    static {

        final Set<Relationship> r = new HashSet<>();
        r.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(r);

        final List<PropertyDescriptor> pds = new ArrayList<>();
        pds.add(HOSTS);
        pds.add(DRIVER_NAME);
        pds.add(DRIVER_LOCATION);
        pds.add(USERNAME);
        pds.add(PASSWORD);
        pds.add(SERVER_ID);
        pds.add(DATABASE_NAME_PATTERN);
        pds.add(TABLE_NAME_PATTERN);
        pds.add(CONNECT_TIMEOUT);
        pds.add(DIST_CACHE_CLIENT);
        pds.add(RETRIEVE_ALL_RECORDS);
        pds.add(INCLUDE_BEGIN_COMMIT);
        pds.add(INCLUDE_DDL_EVENTS);
        pds.add(STATE_UPDATE_INTERVAL);
        pds.add(INIT_SEQUENCE_ID);
        pds.add(INIT_BINLOG_FILENAME);
        pds.add(INIT_BINLOG_POSITION);
        propDescriptors = Collections.unmodifiableList(pds);
    }


    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propDescriptors;
    }

    public void setup(ProcessContext context) {

        final ComponentLog logger = getLogger();

        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;

        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            logger.error("Failed to retrieve observed maximum values from the State Manager. Will not attempt "
                    + "connection until this is accomplished.", ioe);
            context.yield();
            return;
        }

        PropertyValue dbNameValue = context.getProperty(DATABASE_NAME_PATTERN);
        databaseNamePattern = dbNameValue.isSet() ? Pattern.compile(dbNameValue.getValue()) : null;

        PropertyValue tableNameValue = context.getProperty(TABLE_NAME_PATTERN);
        tableNamePattern = tableNameValue.isSet() ? Pattern.compile(tableNameValue.getValue()) : null;

        stateUpdateInterval = context.getProperty(STATE_UPDATE_INTERVAL).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

        boolean getAllRecords = context.getProperty(RETRIEVE_ALL_RECORDS).asBoolean();

        includeBeginCommit = context.getProperty(INCLUDE_BEGIN_COMMIT).asBoolean();
        includeDDLEvents = context.getProperty(INCLUDE_DDL_EVENTS).asBoolean();

        // Set current binlog filename to whatever is in State, falling back to the Retrieve All Records then Initial Binlog Filename if no State variable is present
        currentBinlogFile = stateMap.get(BinlogEventInfo.BINLOG_FILENAME_KEY);
        if (currentBinlogFile == null) {
            if (!getAllRecords) {
                if (context.getProperty(INIT_BINLOG_FILENAME).isSet()) {
                    currentBinlogFile = context.getProperty(INIT_BINLOG_FILENAME).evaluateAttributeExpressions().getValue();
                }
            } else {
                // If we're starting from the beginning of all binlogs, the binlog filename must be the empty string (not null)
                currentBinlogFile = "";
            }
        }

        // Set current binlog position to whatever is in State, falling back to the Retrieve All Records then Initial Binlog Filename if no State variable is present
        String binlogPosition = stateMap.get(BinlogEventInfo.BINLOG_POSITION_KEY);
        if (binlogPosition != null) {
            currentBinlogPosition = Long.valueOf(binlogPosition);
        } else if (!getAllRecords) {
            if (context.getProperty(INIT_BINLOG_POSITION).isSet()) {
                currentBinlogPosition = context.getProperty(INIT_BINLOG_POSITION).evaluateAttributeExpressions().asLong();
            } else {
                currentBinlogPosition = DO_NOT_SET;
            }
        } else {
            currentBinlogPosition = -1;
        }

        // Get current sequence ID from state
        String seqIdString = stateMap.get(EventWriter.SEQUENCE_ID_KEY);
        if (StringUtils.isEmpty(seqIdString)) {
            // Use Initial Sequence ID property if none is found in state
            PropertyValue seqIdProp = context.getProperty(INIT_SEQUENCE_ID);
            if (seqIdProp.isSet()) {
                currentSequenceId.set(seqIdProp.evaluateAttributeExpressions().asInteger());
            }
        } else {
            currentSequenceId.set(Integer.parseInt(seqIdString));
        }

        // Get reference to Distributed Cache if one exists. If it does not, no enrichment (resolution of column names, e.g.) will be performed
        boolean createEnrichmentConnection = false;
        if (context.getProperty(DIST_CACHE_CLIENT).isSet()) {
            cacheClient = context.getProperty(DIST_CACHE_CLIENT).asControllerService(DistributedMapCacheClient.class);
            createEnrichmentConnection = true;
        } else {
            logger.warn("No Distributed Map Cache Client is specified, so no event enrichment (resolution of column names, e.g.) will be performed.");
            cacheClient = null;
        }


        // Save off MySQL cluster and JDBC driver information, will be used to connect for event enrichment as well as for the binlog connector
        try {
            List<InetSocketAddress> hosts = getHosts(context.getProperty(HOSTS).evaluateAttributeExpressions().getValue());

            String username = context.getProperty(USERNAME).evaluateAttributeExpressions().getValue();
            String password = context.getProperty(PASSWORD).evaluateAttributeExpressions().getValue();

            // BinaryLogClient expects a non-null password, so set it to the empty string if it is not provided
            if (password == null) {
                password = "";
            }

            long connectTimeout = context.getProperty(CONNECT_TIMEOUT).evaluateAttributeExpressions().asTimePeriod(TimeUnit.MILLISECONDS);

            String driverLocation = context.getProperty(DRIVER_LOCATION).evaluateAttributeExpressions().getValue();
            String driverName = context.getProperty(DRIVER_NAME).evaluateAttributeExpressions().getValue();

            Long serverId = context.getProperty(SERVER_ID).evaluateAttributeExpressions().asLong();

            connect(hosts, username, password, serverId, createEnrichmentConnection, driverLocation, driverName, connectTimeout);
        } catch (IOException | IllegalStateException e) {
            context.yield();
            binlogClient = null;
            throw new ProcessException(e.getMessage(), e);
        }
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {

        // Indicate that this processor has executed at least once, so we know whether or not the state values are valid and should be updated
        hasRun.set(true);
        ComponentLog log = getLogger();
        StateManager stateManager = context.getStateManager();

        // Create a client if we don't have one
        if (binlogClient == null) {
            setup(context);
        }

        // If the client has been disconnected, try to reconnect
        if (!binlogClient.isConnected()) {
            Exception e = lifecycleListener.getException();
            // If there's no exception, the listener callback might not have been executed yet, so try again later. Otherwise clean up and start over next time
            if (e != null) {
                // Communications failure, disconnect and try next time
                log.error("Binlog connector communications failure: " + e.getMessage(), e);
                try {
                    stop(stateManager);
                } catch (CDCException ioe) {
                    throw new ProcessException(ioe);
                }
            }

            // Try again later
            context.yield();
            return;
        }

        if (currentSession == null) {
            currentSession = sessionFactory.createSession();
        }

        try {
            outputEvents(currentSession, stateManager, log);
            long now = System.currentTimeMillis();
            long timeSinceLastUpdate = now - lastStateUpdate;

            if (stateUpdateInterval != 0 && timeSinceLastUpdate >= stateUpdateInterval) {
                updateState(stateManager, currentBinlogFile, currentBinlogPosition, currentSequenceId.get());
                lastStateUpdate = now;
            }
        } catch (IOException ioe) {
            try {
                // Perform some processor-level "rollback", then rollback the session
                currentBinlogFile = xactBinlogFile == null ? "" : xactBinlogFile;
                currentBinlogPosition = xactBinlogPosition;
                currentSequenceId.set(xactSequenceId);
                inTransaction = false;
                stop(stateManager);
                queue.clear();
                currentSession.rollback();
            } catch (Exception e) {
                // Not much we can recover from here
                log.warn("Error occurred during rollback", e);
            }
            throw new ProcessException(ioe);
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
            // In case we get shutdown while still running, save off the current state, disconnect, and shut down gracefully
            stop(context.getStateManager());
        } catch (CDCException ioe) {
            throw new ProcessException(ioe);
        }
    }

    /**
     * Get a list of hosts from a NiFi property, e.g.
     *
     * @param hostsString A comma-separated list of hosts (host:port,host2:port2, etc.)
     * @return List of InetSocketAddresses for the hosts
     */
    private List<InetSocketAddress> getHosts(String hostsString) {

        if (hostsString == null) {
            return null;
        }
        final List<String> hostsSplit = Arrays.asList(hostsString.split(","));
        List<InetSocketAddress> hostsList = new ArrayList<>();

        for (String item : hostsSplit) {
            String[] addresses = item.split(":");
            if (addresses.length != 2) {
                throw new ArrayIndexOutOfBoundsException("Not in host:port format");
            }

            hostsList.add(new InetSocketAddress(addresses[0].trim(), Integer.parseInt(addresses[1].trim())));
        }
        return hostsList;
    }

    protected void connect(List<InetSocketAddress> hosts, String username, String password, Long serverId, boolean createEnrichmentConnection,
                           String driverLocation, String driverName, long connectTimeout) throws IOException {

        int connectionAttempts = 0;
        final int numHosts = hosts.size();
        InetSocketAddress connectedHost = null;
        Exception lastConnectException = new Exception("Unknown connection error");

        if (createEnrichmentConnection) {
            try {
                // Ensure driverLocation and driverName are correct before establishing binlog connection
                // to avoid failing after binlog messages are received.
                // Actual JDBC connection is created after binlog client gets started, because we need
                // the connect-able host same as the binlog client.
                registerDriver(driverLocation, driverName);
            } catch (InitializationException e) {
                throw new RuntimeException("Failed to register JDBC driver. Ensure MySQL Driver Location(s)" +
                        " and MySQL Driver Class Name are configured correctly. " + e, e);
            }
        }

        while (connectedHost == null && connectionAttempts < numHosts) {
            if (binlogClient == null) {

                connectedHost = hosts.get(currentHost);
                binlogClient = createBinlogClient(connectedHost.getHostString(), connectedHost.getPort(), username, password);
            }

            // Add an event listener and lifecycle listener for binlog and client events, respectively
            if (eventListener == null) {
                eventListener = createBinlogEventListener(binlogClient, queue);
            }
            eventListener.start();
            binlogClient.registerEventListener(eventListener);

            if (lifecycleListener == null) {
                lifecycleListener = createBinlogLifecycleListener();
            }
            binlogClient.registerLifecycleListener(lifecycleListener);

            binlogClient.setBinlogFilename(currentBinlogFile);
            if (currentBinlogPosition != DO_NOT_SET) {
                binlogClient.setBinlogPosition(currentBinlogPosition);
            }

            if (serverId != null) {
                binlogClient.setServerId(serverId);
            }

            try {
                if (connectTimeout == 0) {
                    connectTimeout = Long.MAX_VALUE;
                }
                binlogClient.connect(connectTimeout);
                transitUri = "mysql://" + connectedHost.getHostString() + ":" + connectedHost.getPort();

            } catch (IOException | TimeoutException te) {
                // Try the next host
                connectedHost = null;
                transitUri = "<unknown>";
                currentHost = (currentHost + 1) % numHosts;
                connectionAttempts++;
                lastConnectException = te;
            }
        }
        if (!binlogClient.isConnected()) {
            binlogClient.disconnect();
            binlogClient = null;
            throw new IOException("Could not connect binlog client to any of the specified hosts due to: " + lastConnectException.getMessage(), lastConnectException);
        }

        if (createEnrichmentConnection) {
            jdbcConnectionHolder = new JDBCConnectionHolder(connectedHost, username, password, null, connectTimeout);
            try {
                // Ensure connection can be created.
                getJdbcConnection();
            } catch (SQLException e) {
                binlogClient.disconnect();
                binlogClient = null;
                throw new IOException("Error creating binlog enrichment JDBC connection to any of the specified hosts", e);
            }
        }

        doStop.set(false);
    }


    public void outputEvents(ProcessSession session, StateManager stateManager, ComponentLog log) throws IOException {
        RawBinlogEvent rawBinlogEvent;

        // Drain the queue
        while ((rawBinlogEvent = queue.poll()) != null && !doStop.get()) {
            Event event = rawBinlogEvent.getEvent();
            EventHeaderV4 header = event.getHeader();
            long timestamp = header.getTimestamp();
            EventType eventType = header.getEventType();
            // Advance the current binlog position. This way if no more events are received and the processor is stopped, it will resume at the event about to be processed.
            // We always get ROTATE and FORMAT_DESCRIPTION messages no matter where we start (even from the end), and they won't have the correct "next position" value, so only
            // advance the position if it is not that type of event. ROTATE events don't generate output CDC events and have the current binlog position in a special field, which
            // is filled in during the ROTATE case
            if (eventType != ROTATE && eventType != FORMAT_DESCRIPTION) {
                currentBinlogPosition = header.getPosition();
            }
            log.debug("Got message event type: {} ", new Object[]{header.getEventType().toString()});
            switch (eventType) {
                case TABLE_MAP:
                    // This is sent to inform which table is about to be changed by subsequent events
                    TableMapEventData data = event.getData();

                    // Should we skip this table? Yes if we've specified a DB or table name pattern and they don't match
                    skipTable = (databaseNamePattern != null && !databaseNamePattern.matcher(data.getDatabase()).matches())
                            || (tableNamePattern != null && !tableNamePattern.matcher(data.getTable()).matches());

                    if (!skipTable) {
                        TableInfoCacheKey key = new TableInfoCacheKey(this.getIdentifier(), data.getDatabase(), data.getTable(), data.getTableId());
                        if (cacheClient != null) {
                            try {
                                currentTable = cacheClient.get(key, cacheKeySerializer, cacheValueDeserializer);
                            } catch (ConnectException ce) {
                                throw new IOException("Could not connect to Distributed Map Cache server to get table information", ce);
                            }

                            if (currentTable == null) {
                                // We don't have an entry for this table yet, so fetch the info from the database and populate the cache
                                try {
                                    currentTable = loadTableInfo(key);
                                    try {
                                        cacheClient.put(key, currentTable, cacheKeySerializer, cacheValueSerializer);
                                    } catch (ConnectException ce) {
                                        throw new IOException("Could not connect to Distributed Map Cache server to put table information", ce);
                                    }
                                } catch (SQLException se) {
                                    // Propagate the error up, so things like rollback and logging/bulletins can be handled
                                    throw new IOException(se.getMessage(), se);
                                }
                            }
                        }
                    } else {
                        // Clear the current table, to force a reload next time we get a TABLE_MAP event we care about
                        currentTable = null;
                    }
                    break;
                case QUERY:
                    QueryEventData queryEventData = event.getData();
                    currentDatabase = queryEventData.getDatabase();

                    String sql = queryEventData.getSql();

                    // Is this the start of a transaction?
                    if ("BEGIN".equals(sql)) {
                        // If we're already in a transaction, something bad happened, alert the user
                        if (inTransaction) {
                            throw new IOException("BEGIN event received while already processing a transaction. This could indicate that your binlog position is invalid.");
                        }
                        // Mark the current binlog position in case we have to rollback the transaction (if the processor is stopped, e.g.)
                        xactBinlogFile = currentBinlogFile;
                        xactBinlogPosition = currentBinlogPosition;
                        xactSequenceId = currentSequenceId.get();

                        if (includeBeginCommit && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
                            BeginTransactionEventInfo beginEvent = new BeginTransactionEventInfo(currentDatabase, timestamp, currentBinlogFile, currentBinlogPosition);
                            currentSequenceId.set(beginEventWriter.writeEvent(currentSession, transitUri, beginEvent, currentSequenceId.get(), REL_SUCCESS));
                        }
                        inTransaction = true;
                    } else if ("COMMIT".equals(sql)) {
                        if (!inTransaction) {
                            throw new IOException("COMMIT event received while not processing a transaction (i.e. no corresponding BEGIN event). "
                                    + "This could indicate that your binlog position is invalid.");
                        }
                        // InnoDB generates XID events for "commit", but MyISAM generates Query events with "COMMIT", so handle that here
                        if (includeBeginCommit && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
                            CommitTransactionEventInfo commitTransactionEvent = new CommitTransactionEventInfo(currentDatabase, timestamp, currentBinlogFile, currentBinlogPosition);
                            currentSequenceId.set(commitEventWriter.writeEvent(currentSession, transitUri, commitTransactionEvent, currentSequenceId.get(), REL_SUCCESS));
                        }
                        // Commit the NiFi session
                        session.commit();
                        inTransaction = false;
                        currentTable = null;

                    } else {
                        // Check for DDL events (alter table, e.g.). Normalize the query to do string matching on the type of change
                        String normalizedQuery = sql.toLowerCase().trim().replaceAll(" {2,}", " ");
                        if (normalizedQuery.startsWith("alter table")
                                || normalizedQuery.startsWith("alter ignore table")
                                || normalizedQuery.startsWith("create table")
                                || normalizedQuery.startsWith("truncate table")
                                || normalizedQuery.startsWith("rename table")
                                || normalizedQuery.startsWith("drop table")
                                || normalizedQuery.startsWith("drop database")) {

                            if (includeDDLEvents && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
                                // If we don't have table information, we can still use the database name
                                TableInfo ddlTableInfo = (currentTable != null) ? currentTable : new TableInfo(currentDatabase, null, null, null);
                                DDLEventInfo ddlEvent = new DDLEventInfo(ddlTableInfo, timestamp, currentBinlogFile, currentBinlogPosition, sql);
                                currentSequenceId.set(ddlEventWriter.writeEvent(currentSession, transitUri, ddlEvent, currentSequenceId.get(), REL_SUCCESS));
                            }
                            // Remove all the keys from the cache that this processor added
                            if (cacheClient != null) {
                                cacheClient.removeByPattern(this.getIdentifier() + ".*");
                            }
                            // If not in a transaction, commit the session so the DDL event(s) will be transferred
                            if (includeDDLEvents && !inTransaction) {
                                session.commit();
                            }
                        }
                    }
                    break;

                case XID:
                    if (!inTransaction) {
                        throw new IOException("COMMIT event received while not processing a transaction (i.e. no corresponding BEGIN event). "
                                + "This could indicate that your binlog position is invalid.");
                    }
                    if (includeBeginCommit && (databaseNamePattern == null || databaseNamePattern.matcher(currentDatabase).matches())) {
                        CommitTransactionEventInfo commitTransactionEvent = new CommitTransactionEventInfo(currentDatabase, timestamp, currentBinlogFile, currentBinlogPosition);
                        currentSequenceId.set(commitEventWriter.writeEvent(currentSession, transitUri, commitTransactionEvent, currentSequenceId.get(), REL_SUCCESS));
                    }
                    // Commit the NiFi session
                    session.commit();
                    inTransaction = false;
                    currentTable = null;
                    currentDatabase = null;
                    break;

                case WRITE_ROWS:
                case EXT_WRITE_ROWS:
                case PRE_GA_WRITE_ROWS:
                case UPDATE_ROWS:
                case EXT_UPDATE_ROWS:
                case PRE_GA_UPDATE_ROWS:
                case DELETE_ROWS:
                case EXT_DELETE_ROWS:
                case PRE_GA_DELETE_ROWS:
                    // If we are skipping this table, then don't emit any events related to its modification
                    if (skipTable) {
                        break;
                    }
                    if (!inTransaction) {
                        // These events should only happen inside a transaction, warn the user otherwise
                        log.warn("Table modification event occurred outside of a transaction.");
                        break;
                    }
                    if (currentTable == null && cacheClient != null) {
                        // No Table Map event was processed prior to this event, which should not happen, so throw an error
                        throw new RowEventException("No table information is available for this event, cannot process further.");
                    }

                    if (eventType == WRITE_ROWS
                            || eventType == EXT_WRITE_ROWS
                            || eventType == PRE_GA_WRITE_ROWS) {

                        InsertRowsEventInfo eventInfo = new InsertRowsEventInfo(currentTable, timestamp, currentBinlogFile, currentBinlogPosition, event.getData());
                        currentSequenceId.set(insertRowsWriter.writeEvent(currentSession, transitUri, eventInfo, currentSequenceId.get(), REL_SUCCESS));

                    } else if (eventType == DELETE_ROWS
                            || eventType == EXT_DELETE_ROWS
                            || eventType == PRE_GA_DELETE_ROWS) {

                        DeleteRowsEventInfo eventInfo = new DeleteRowsEventInfo(currentTable, timestamp, currentBinlogFile, currentBinlogPosition, event.getData());
                        currentSequenceId.set(deleteRowsWriter.writeEvent(currentSession, transitUri, eventInfo, currentSequenceId.get(), REL_SUCCESS));

                    } else {
                        // Update event
                        UpdateRowsEventInfo eventInfo = new UpdateRowsEventInfo(currentTable, timestamp, currentBinlogFile, currentBinlogPosition, event.getData());
                        currentSequenceId.set(updateRowsWriter.writeEvent(currentSession, transitUri, eventInfo, currentSequenceId.get(), REL_SUCCESS));
                    }
                    break;

                case ROTATE:
                    // Update current binlog filename
                    RotateEventData rotateEventData = event.getData();
                    currentBinlogFile = rotateEventData.getBinlogFilename();
                    currentBinlogPosition = rotateEventData.getBinlogPosition();
                    break;
                default:
                    break;
            }

            // Advance the current binlog position. This way if no more events are received and the processor is stopped, it will resume after the event that was just processed.
            // We always get ROTATE and FORMAT_DESCRIPTION messages no matter where we start (even from the end), and they won't have the correct "next position" value, so only
            // advance the position if it is not that type of event.
            if (eventType != ROTATE && eventType != FORMAT_DESCRIPTION) {
                currentBinlogPosition = header.getNextPosition();
            }
        }
    }

    protected void stop(StateManager stateManager) throws CDCException {
        try {
            if (binlogClient != null) {
                binlogClient.disconnect();
            }
            if (eventListener != null) {
                eventListener.stop();
                if (binlogClient != null) {
                    binlogClient.unregisterEventListener(eventListener);
                }
            }
            doStop.set(true);

            if (hasRun.getAndSet(false)) {
                updateState(stateManager, currentBinlogFile, currentBinlogPosition, currentSequenceId.get());
            }
            currentBinlogPosition = -1;

        } catch (IOException e) {
            throw new CDCException("Error closing CDC connection", e);
        } finally {
            binlogClient = null;

            if (jdbcConnectionHolder != null) {
                jdbcConnectionHolder.close();
            }
        }
    }

    private void updateState(StateManager stateManager, String binlogFile, long binlogPosition, long sequenceId) throws IOException {
        // Update state with latest values
        if (stateManager != null) {
            Map<String, String> newStateMap = new HashMap<>(stateManager.getState(Scope.CLUSTER).toMap());

            // Save current binlog filename and position to the state map
            if (binlogFile != null) {
                newStateMap.put(BinlogEventInfo.BINLOG_FILENAME_KEY, binlogFile);
            }
            newStateMap.put(BinlogEventInfo.BINLOG_POSITION_KEY, Long.toString(binlogPosition));
            newStateMap.put(EventWriter.SEQUENCE_ID_KEY, String.valueOf(sequenceId));
            stateManager.setState(newStateMap, Scope.CLUSTER);
        }
    }


    /**
     * Creates and returns a BinlogEventListener instance, associated with the specified binlog client and event queue.
     *
     * @param client A reference to a BinaryLogClient. The listener is associated with the given client, such that the listener is notified when
     *               events are available to the given client.
     * @param q      A queue used to communicate events between the listener and the NiFi processor thread.
     * @return A BinlogEventListener instance, which will be notified of events associated with the specified client
     */
    BinlogEventListener createBinlogEventListener(BinaryLogClient client, LinkedBlockingQueue<RawBinlogEvent> q) {
        return new BinlogEventListener(client, q);
    }

    /**
     * Creates and returns a BinlogLifecycleListener instance, associated with the specified binlog client and event queue.
     *
     * @return A BinlogLifecycleListener instance, which will be notified of events associated with the specified client
     */
    BinlogLifecycleListener createBinlogLifecycleListener() {
        return new BinlogLifecycleListener();
    }


    BinaryLogClient createBinlogClient(String hostname, int port, String username, String password) {
        return new BinaryLogClient(hostname, port, username, password);
    }

    /**
     * Retrieves the column information for the specified database and table. The column information can be used to enrich CDC events coming from the RDBMS.
     *
     * @param key A TableInfoCacheKey reference, which contains the database and table names
     * @return A TableInfo instance with the ColumnDefinitions provided (if retrieved successfully from the database)
     */
    protected TableInfo loadTableInfo(TableInfoCacheKey key) throws SQLException {
        TableInfo tableInfo = null;
        if (jdbcConnectionHolder != null) {

            try (Statement s = getJdbcConnection().createStatement()) {
                s.execute("USE `" + key.getDatabaseName() + "`");
                ResultSet rs = s.executeQuery("SELECT * FROM `" + key.getTableName() + "` LIMIT 0");
                ResultSetMetaData rsmd = rs.getMetaData();
                int numCols = rsmd.getColumnCount();
                List<ColumnDefinition> columnDefinitions = new ArrayList<>();
                for (int i = 1; i <= numCols; i++) {
                    // Use the column label if it exists, otherwise use the column name. We're not doing aliasing here, but it's better practice.
                    String columnLabel = rsmd.getColumnLabel(i);
                    columnDefinitions.add(new ColumnDefinition(rsmd.getColumnType(i), columnLabel != null ? columnLabel : rsmd.getColumnName(i)));
                }

                tableInfo = new TableInfo(key.getDatabaseName(), key.getTableName(), key.getTableId(), columnDefinitions);
            }
        }

        return tableInfo;
    }

    protected Connection getJdbcConnection() throws SQLException {
        return jdbcConnectionHolder.getConnection();
    }

    private class JDBCConnectionHolder {
        private String connectionUrl;
        private Properties connectionProps = new Properties();
        private long connectionTimeoutMillis;

        private Connection connection;

        private JDBCConnectionHolder(InetSocketAddress host, String username, String password, Map<String, String> customProperties, long connectionTimeoutMillis) {
            this.connectionUrl = "jdbc:mysql://" + host.getHostString() + ":" + host.getPort();
            if (customProperties != null) {
                connectionProps.putAll(customProperties);
            }
            connectionProps.put("user", username);
            connectionProps.put("password", password);
            this.connectionTimeoutMillis = connectionTimeoutMillis;
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


    /**
     * using Thread.currentThread().getContextClassLoader(); will ensure that you are using the ClassLoader for you NAR.
     *
     * @throws InitializationException if there is a problem obtaining the ClassLoader
     */
    protected void registerDriver(String locationString, String drvName) throws InitializationException {
        if (locationString != null && locationString.length() > 0) {
            try {
                // Split and trim the entries
                final ClassLoader classLoader = ClassLoaderUtils.getCustomClassLoader(
                        locationString,
                        this.getClass().getClassLoader(),
                        (dir, name) -> name != null && name.endsWith(".jar")
                );

                // Workaround which allows to use URLClassLoader for JDBC driver loading.
                // (Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.)
                final Class<?> clazz = Class.forName(drvName, true, classLoader);
                if (clazz == null) {
                    throw new InitializationException("Can't load Database Driver " + drvName);
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
}
