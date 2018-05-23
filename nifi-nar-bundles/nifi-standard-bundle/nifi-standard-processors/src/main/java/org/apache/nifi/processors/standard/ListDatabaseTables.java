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
package org.apache.nifi.processors.standard;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A processor to retrieve a list of tables (and their metadata) from a database connection
 */
@PrimaryNodeOnly
@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "list", "jdbc", "table", "database"})
@CapabilityDescription("Generates a set of flow files, each containing attributes corresponding to metadata about a table from a database connection. Once "
        + "metadata about a table has been fetched, it will not be fetched again until the Refresh Interval (if set) has elapsed, or until state has been "
        + "manually cleared.")
@WritesAttributes({
        @WritesAttribute(attribute = "db.table.name", description = "Contains the name of a database table from the connection"),
        @WritesAttribute(attribute = "db.table.catalog", description = "Contains the name of the catalog to which the table belongs (may be null)"),
        @WritesAttribute(attribute = "db.table.schema", description = "Contains the name of the schema to which the table belongs (may be null)"),
        @WritesAttribute(attribute = "db.table.fullname", description = "Contains the fully-qualifed table name (possibly including catalog, schema, etc.)"),
        @WritesAttribute(attribute = "db.table.type",
                description = "Contains the type of the database table from the connection. Typical types are \"TABLE\", \"VIEW\", \"SYSTEM TABLE\", "
                        + "\"GLOBAL TEMPORARY\", \"LOCAL TEMPORARY\", \"ALIAS\", \"SYNONYM\""),
        @WritesAttribute(attribute = "db.table.remarks", description = "Contains the name of a database table from the connection"),
        @WritesAttribute(attribute = "db.table.count", description = "Contains the number of rows in the table")
})
@Stateful(scopes = {Scope.CLUSTER}, description = "After performing a listing of tables, the timestamp of the query is stored. "
        + "This allows the Processor to not re-list tables the next time that the Processor is run. Specifying the refresh interval in the processor properties will "
        + "indicate that when the processor detects the interval has elapsed, the state will be reset and tables will be re-listed as a result. "
        + "This processor is meant to be run on the primary node only.")
public class ListDatabaseTables extends AbstractProcessor {

    // Attribute names
    public static final String DB_TABLE_NAME = "db.table.name";
    public static final String DB_TABLE_CATALOG = "db.table.catalog";
    public static final String DB_TABLE_SCHEMA = "db.table.schema";
    public static final String DB_TABLE_FULLNAME = "db.table.fullname";
    public static final String DB_TABLE_TYPE = "db.table.type";
    public static final String DB_TABLE_REMARKS = "db.table.remarks";
    public static final String DB_TABLE_COUNT = "db.table.count";

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that are received are routed to success")
            .build();

    // Property descriptors
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("list-db-tables-db-connection")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor CATALOG = new PropertyDescriptor.Builder()
            .name("list-db-tables-catalog")
            .displayName("Catalog")
            .description("The name of a catalog from which to list database tables. The name must match the catalog name as it is stored in the database. "
                    + "If the property is not set, the catalog name will not be used to narrow the search for tables. If the property is set to an empty string, "
                    + "tables without a catalog will be listed.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor SCHEMA_PATTERN = new PropertyDescriptor.Builder()
            .name("list-db-tables-schema-pattern")
            .displayName("Schema Pattern")
            .description("A pattern for matching schemas in the database. Within a pattern, \"%\" means match any substring of 0 or more characters, "
                    + "and \"_\" means match any one character. The pattern must match the schema name as it is stored in the database. "
                    + "If the property is not set, the schema name will not be used to narrow the search for tables. If the property is set to an empty string, "
                    + "tables without a schema will be listed.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor TABLE_NAME_PATTERN = new PropertyDescriptor.Builder()
            .name("list-db-tables-name-pattern")
            .displayName("Table Name Pattern")
            .description("A pattern for matching tables in the database. Within a pattern, \"%\" means match any substring of 0 or more characters, "
                    + "and \"_\" means match any one character. The pattern must match the table name as it is stored in the database. "
                    + "If the property is not set, all tables will be retrieved.")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor TABLE_TYPES = new PropertyDescriptor.Builder()
            .name("list-db-tables-types")
            .displayName("Table Types")
            .description("A comma-separated list of table types to include. For example, some databases support TABLE and VIEW types. If the property is not set, "
                    + "tables of all types will be returned.")
            .required(false)
            .defaultValue("TABLE")
            .addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor INCLUDE_COUNT = new PropertyDescriptor.Builder()
            .name("list-db-include-count")
            .displayName("Include Count")
            .description("Whether to include the table's row count as a flow file attribute. This affects performance as a database query will be generated "
                    + "for each table in the retrieved list.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();

    public static final PropertyDescriptor REFRESH_INTERVAL = new PropertyDescriptor.Builder()
            .name("list-db-refresh-interval")
            .displayName("Refresh Interval")
            .description("The amount of time to elapse before resetting the processor state, thereby causing all current tables to be listed. "
                    + "During this interval, the processor may continue to run, but tables that have already been listed will not be re-listed. However new/added "
                    + "tables will be listed as the processor runs. A value of zero means the state will never be automatically reset, the user must "
                    + "Clear State manually.")
            .required(true)
            .defaultValue("0 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> propertyDescriptors;
    private static final Set<Relationship> relationships;

    /*
     * Will ensure that the list of property descriptors is build only once.
     * Will also create a Set of relationships
     */
    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.add(DBCP_SERVICE);
        _propertyDescriptors.add(CATALOG);
        _propertyDescriptors.add(SCHEMA_PATTERN);
        _propertyDescriptors.add(TABLE_NAME_PATTERN);
        _propertyDescriptors.add(TABLE_TYPES);
        _propertyDescriptors.add(INCLUDE_COUNT);
        _propertyDescriptors.add(REFRESH_INTERVAL);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        final String catalog = context.getProperty(CATALOG).getValue();
        final String schemaPattern = context.getProperty(SCHEMA_PATTERN).getValue();
        final String tableNamePattern = context.getProperty(TABLE_NAME_PATTERN).getValue();
        final String[] tableTypes = context.getProperty(TABLE_TYPES).isSet()
                ? context.getProperty(TABLE_TYPES).getValue().split("\\s*,\\s*")
                : null;
        final boolean includeCount = context.getProperty(INCLUDE_COUNT).asBoolean();
        final long refreshInterval = context.getProperty(REFRESH_INTERVAL).asTimePeriod(TimeUnit.MILLISECONDS);

        final StateManager stateManager = context.getStateManager();
        final StateMap stateMap;
        final Map<String, String> stateMapProperties;
        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
            stateMapProperties = new HashMap<>(stateMap.toMap());
        } catch (IOException ioe) {
            throw new ProcessException(ioe);
        }

        try (final Connection con = dbcpService.getConnection(Collections.emptyMap())) {

            DatabaseMetaData dbMetaData = con.getMetaData();
            ResultSet rs = dbMetaData.getTables(catalog, schemaPattern, tableNamePattern, tableTypes);
            while (rs.next()) {
                final String tableCatalog = rs.getString(1);
                final String tableSchema = rs.getString(2);
                final String tableName = rs.getString(3);
                final String tableType = rs.getString(4);
                final String tableRemarks = rs.getString(5);

                // Build fully-qualified name
                String fqn = Stream.of(tableCatalog, tableSchema, tableName)
                        .filter(segment -> !StringUtils.isEmpty(segment))
                        .collect(Collectors.joining("."));

                String lastTimestampForTable = stateMapProperties.get(fqn);
                boolean refreshTable = true;
                try {
                    // Refresh state if the interval has elapsed
                    long lastRefreshed = -1;
                    final long currentTime = System.currentTimeMillis();
                    if (!StringUtils.isEmpty(lastTimestampForTable)) {
                        lastRefreshed = Long.parseLong(lastTimestampForTable);
                    }
                    if (lastRefreshed == -1 || (refreshInterval > 0 && currentTime >= (lastRefreshed + refreshInterval))) {
                        stateMapProperties.remove(lastTimestampForTable);
                    } else {
                        refreshTable = false;
                    }
                } catch (final NumberFormatException nfe) {
                    getLogger().error("Failed to retrieve observed last table fetches from the State Manager. Will not perform "
                            + "query until this is accomplished.", nfe);
                    context.yield();
                    return;
                }
                if (refreshTable) {
                    FlowFile flowFile = session.create();
                    logger.info("Found {}: {}", new Object[]{tableType, fqn});
                    if (includeCount) {
                        try (Statement st = con.createStatement()) {
                            final String countQuery = "SELECT COUNT(1) FROM " + fqn;

                            logger.debug("Executing query: {}", new Object[]{countQuery});
                            ResultSet countResult = st.executeQuery(countQuery);
                            if (countResult.next()) {
                                flowFile = session.putAttribute(flowFile, DB_TABLE_COUNT, Long.toString(countResult.getLong(1)));
                            }
                        } catch (SQLException se) {
                            logger.error("Couldn't get row count for {}", new Object[]{fqn});
                            session.remove(flowFile);
                            continue;
                        }
                    }
                    if (tableCatalog != null) {
                        flowFile = session.putAttribute(flowFile, DB_TABLE_CATALOG, tableCatalog);
                    }
                    if (tableSchema != null) {
                        flowFile = session.putAttribute(flowFile, DB_TABLE_SCHEMA, tableSchema);
                    }
                    flowFile = session.putAttribute(flowFile, DB_TABLE_NAME, tableName);
                    flowFile = session.putAttribute(flowFile, DB_TABLE_FULLNAME, fqn);
                    flowFile = session.putAttribute(flowFile, DB_TABLE_TYPE, tableType);
                    if (tableRemarks != null) {
                        flowFile = session.putAttribute(flowFile, DB_TABLE_REMARKS, tableRemarks);
                    }

                    String transitUri;
                    try {
                        transitUri = dbMetaData.getURL();
                    } catch (SQLException sqle) {
                        transitUri = "<unknown>";
                    }
                    session.getProvenanceReporter().receive(flowFile, transitUri);
                    session.transfer(flowFile, REL_SUCCESS);
                    stateMapProperties.put(fqn, Long.toString(System.currentTimeMillis()));
                }
            }
            // Update the timestamps for listed tables
            if (stateMap.getVersion() == -1) {
                stateManager.setState(stateMapProperties, Scope.CLUSTER);
            } else {
                stateManager.replace(stateMap, stateMapProperties, Scope.CLUSTER);
            }

        } catch (final SQLException | IOException e) {
            throw new ProcessException(e);
        }
    }
}
