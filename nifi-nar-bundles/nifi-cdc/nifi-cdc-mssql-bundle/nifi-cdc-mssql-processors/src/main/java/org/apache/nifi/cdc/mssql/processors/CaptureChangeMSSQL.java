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
package org.apache.nifi.cdc.mssql.processors;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.cdc.CDCException;
import org.apache.nifi.cdc.mssql.MSSQLCDCUtils;
import org.apache.nifi.cdc.mssql.event.MSSQLTableInfo;
import org.apache.nifi.cdc.mssql.event.TableCapturePlan;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.ResultSetRecordSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@TriggerSerially
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@Tags({"sql", "jdbc", "cdc", "mssql"})
@CapabilityDescription("Retrieves Change Data Capture (CDC) events from a Microsoft SQL database. CDC Events include INSERT, UPDATE, DELETE operations. Events "
        + "for each table are output as Record Sets, ordered by the time, and sequence, at which the operation occurred.")
@Stateful(scopes = Scope.CLUSTER, description = "Information including the timestamp of the last CDC event per table in the database is stored by this processor, so "
        + "that it can continue from the same point in time if restarted.")
@WritesAttributes({
        @WritesAttribute(attribute = "tablename", description="Name of the table this changeset was captured from."),
        @WritesAttribute(attribute="mssqlcdc.row.count", description="The number of rows in this changeset"),
        @WritesAttribute(attribute="fullsnapshot", description="Whether this was a full snapshot of the base table or not..")})
@DynamicProperty(name = "Initial Timestamp", value = "Attribute Expression Language", supportsExpressionLanguage = false, description = "Specifies an initial "
        + "timestamp for reading CDC data from MS SQL. Properties should be added in the format `initial.timestamp.{table_name}`, one for each table. "
        + "This property is ignored after the first successful run for a table writes to the state manager, and is only used again if state is cleared.")
public class CaptureChangeMSSQL extends AbstractSessionFactoryProcessor {
    public static final String INITIAL_TIMESTAMP_PROP_START = "initial.timestamp.";

    public static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Specifies the Controller Service to use for writing out the records")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("cdcmssql-dbcp-service")
            .displayName("Database Connection Pooling Service")
            .description("The Controller Service that is used to obtain connection to database")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor CDC_TABLES = new PropertyDescriptor.Builder()
            .name("cdcmssql-cdc-table-list")
            .displayName("CDC Table List")
            .description("The comma delimited list of tables in the source database to monitor for changes. If no tables "
                    + "are specified the [cdc].[change_tables] table is queried for all of the available tables with change tracking enabled in the database.")
            .required(false)
            .expressionLanguageSupported(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor TAKE_INITIAL_SNAPSHOT = new PropertyDescriptor.Builder()
            .name("cdcmssql-initial-snapshot")
            .displayName("Generate an Initial Source Table Snapshot")
            .description("Usually CDC only includes recent historic changes. Setting this property to true will cause a snapshot of the "
                + "source table to be taken using the same schema as the CDC extracts. The snapshot time will be used as the starting point "
                + "for extracting CDC changes.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .required(true)
            .build();

    public static final PropertyDescriptor FULL_SNAPSHOT_ROW_LIMIT = new PropertyDescriptor
            .Builder().name("cdcmssql-full-snapshot-row-limit")
            .displayName("Change Set Row Limit")
            .description("If a very large change occurs on the source table, "
                    + "the generated change set may be too large too quickly merge into a destination system. "
                    + "Use this property to set a cut-off point where instead of returning a changeset a full snapshot will be generated instead. "
                    + "The fullsnapshot attribute will be set to true when this happens.")
            .required(true)
            .defaultValue("0")
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully created FlowFile from SQL query result set.")
            .build();

    protected List<PropertyDescriptor> descriptors;
    protected Set<Relationship> relationships;

    protected final Map<String, MSSQLTableInfo> schemaCache = new ConcurrentHashMap<String, MSSQLTableInfo>(1000);

    // A Map (name to value) of initial maximum-value properties, filled at schedule-time and used at trigger-time
    protected Map<String,String> maxValueProperties;
    protected MSSQLCDCUtils mssqlcdcUtils = new MSSQLCDCUtils();

    public MSSQLCDCUtils getMssqlcdcUtils(){
        return mssqlcdcUtils;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(RECORD_WRITER);
        descriptors.add(DBCP_SERVICE);
        descriptors.add(CDC_TABLES);
        descriptors.add(TAKE_INITIAL_SNAPSHOT);
        descriptors.add(FULL_SNAPSHOT_ROW_LIMIT);
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
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        if(!propertyDescriptorName.startsWith("initial.timestamp.")){
            return null;
        }

        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(false)
                .dynamic(true)
                .build();
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSessionFactory processSessionFactory) throws ProcessException {
        ProcessSession session = processSessionFactory.createSession();

        final ComponentLog logger = getLogger();
        final RecordSetWriterFactory writerFactory = processContext.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final DBCPService dbcpService = processContext.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);

        final boolean takeInitialSnapshot = processContext.getProperty(TAKE_INITIAL_SNAPSHOT).asBoolean();
        final int fullSnapshotRowLimit = processContext.getProperty(FULL_SNAPSHOT_ROW_LIMIT).asInteger();

        final String[] allTables = schemaCache.keySet().toArray(new String[schemaCache.size()]);

        String[] tables = StringUtils
                .split(processContext.getProperty(CDC_TABLES).evaluateAttributeExpressions().getValue(), ",");

        if(tables == null || tables.length == 0){
            tables = allTables;
        }

        final StateManager stateManager = processContext.getStateManager();
        final StateMap stateMap;
        try {
            stateMap = stateManager.getState(Scope.CLUSTER);
        } catch (final IOException ioe) {
            logger.error("Failed to retrieve observed current timestamp values from the State Manager. Will not perform "
                    + "query until this is accomplished.", ioe);
            processContext.yield();
            return;
        }

        // Make a mutable copy of the current state property map. This will be updated and eventually
        // set as the current state map (after the session has been committed)
        final Map<String, String> statePropertyMap = new HashMap<>(stateMap.toMap());

        // If an initial max value for the table has been specified using properties, and this table is not in the state manager, sync it to the state property map
        for (final Map.Entry<String, String> maxProp : maxValueProperties.entrySet()) {
            String maxPropKey = maxProp.getKey().toLowerCase();
            String tableName = getStateKey(INITIAL_TIMESTAMP_PROP_START, maxPropKey);
            if (!statePropertyMap.containsKey(tableName)) {
                String newMaxPropValue = maxProp.getValue();

                statePropertyMap.put(tableName, newMaxPropValue);
            }
        }

        //Build a capture plan for each table
        ArrayList<TableCapturePlan> tableCapturePlans = new ArrayList<>();
        try (final Connection con = dbcpService.getConnection()){
            for (String t : tables) {
                String tableKey = t.toLowerCase();
                if (!schemaCache.containsKey(tableKey)) {
                    throw new ProcessException("Unknown CDC enabled table named " + t + ". Known table names: " + String.join(", ", allTables));
                }

                MSSQLTableInfo tableInfo = schemaCache.get(tableKey);

                //Get Max Timestamp from state (if it exists)
                String sTime = null;
                if (statePropertyMap.containsKey(tableKey)) {
                    sTime = statePropertyMap.get(tableKey);
                }

                TableCapturePlan tableCapturePlan = new TableCapturePlan(tableInfo, fullSnapshotRowLimit, takeInitialSnapshot, sTime);

                //Determine Plan Type
                tableCapturePlan.ComputeCapturePlan(con, getMssqlcdcUtils());

                tableCapturePlans.add(tableCapturePlan);
            }

            for (TableCapturePlan capturePlan : tableCapturePlans) {
                final String selectQuery;

                if(capturePlan.getPlanType() == TableCapturePlan.PlanTypes.CDC){
                    selectQuery = getMssqlcdcUtils().getCDCSelectStatement(capturePlan.getTable(), capturePlan.getMaxTime());
                } else if(capturePlan.getPlanType() == TableCapturePlan.PlanTypes.SNAPSHOT){
                    selectQuery = getMssqlcdcUtils().getSnapshotSelectStatement(capturePlan.getTable());
                } else {
                    throw new ProcessException("Unknown Capture Plan type, '" + capturePlan.getPlanType() + "'.");
                }

                FlowFile cdcFlowFile = session.create();
                try(final PreparedStatement st = con.prepareStatement(selectQuery)) {
                    if(capturePlan.getPlanType() == TableCapturePlan.PlanTypes.CDC && capturePlan.getMaxTime() != null){
                        st.setTimestamp(1, capturePlan.getMaxTime());
                    }

                    final ResultSet resultSet = st.executeQuery();
                    ResultSetRecordSet resultSetRecordSet = new ResultSetRecordSet(resultSet);

                    final AtomicReference<Timestamp> maxTimestamp = new AtomicReference<>();
                    final AtomicLong rowCount = new AtomicLong();

                    cdcFlowFile = session.write(cdcFlowFile, new StreamCallback() {
                        @Override
                        public void process(final InputStream in, final OutputStream out) throws IOException {
                            Long rows=0L;
                            final RecordSchema writeSchema = resultSetRecordSet.getSchema();
                            try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writeSchema, out)) {
                                writer.beginRecordSet();

                                Record record;
                                while ((record = resultSetRecordSet.next()) != null) {
                                    writer.write(record);

                                    rows++;
                                    maxTimestamp.set((Timestamp)record.getValue("tran_end_time"));
                                }

                                final WriteResult writeResult = writer.finishRecordSet();
                            } catch (SchemaNotFoundException e) {
                                e.printStackTrace();
                            }

                            rowCount.set(rows);
                        }
                    });

                    if(rowCount.get() == 0){
                        session.remove(cdcFlowFile);
                        continue;
                    }

                    Map<String, String> attributes = new HashMap<>();
                    attributes.put("tablename", capturePlan.getTable().getSourceTableName());
                    attributes.put("mssqlcdc.row.count", rowCount.toString());
                    attributes.put("maxvalue.tran_end_time", maxTimestamp.toString());
                    attributes.put("fullsnapshot", Boolean.toString(capturePlan.getPlanType() == TableCapturePlan.PlanTypes.SNAPSHOT));

                    cdcFlowFile = session.putAllAttributes(cdcFlowFile, attributes);
                    session.transfer(cdcFlowFile, REL_SUCCESS);

                    statePropertyMap.put(capturePlan.getTable().getSourceTableName().toLowerCase(), maxTimestamp.toString());
                    stateManager.setState(statePropertyMap, Scope.CLUSTER);

                    session.commit();
                } catch (IOException e) {
                    session.remove(cdcFlowFile);
                    throw new ProcessException("Failed to update cluster state with new timestamps.", e);
                }
            }
        } catch (SQLException e) {
            throw new ProcessException("Error working with MS SQL CDC Database.", e);
        }
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        //Prefetch list of all CDC tables and their schemas.
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        try (final Connection con = dbcpService.getConnection()) {
            List<MSSQLTableInfo> tableSchemas = getMssqlcdcUtils().getCDCTableList(con);

            for (MSSQLTableInfo ti:tableSchemas) {
                schemaCache.put(ti.getSourceTableName().toLowerCase(), ti);
            }
        } catch (SQLException e) {
            throw new ProcessException("Unable to communicate with database in order to determine CDC tables", e);
        } catch (CDCException e) {
            throw new ProcessException(e.getMessage(), e);
        }

        maxValueProperties = getDefaultMaxValueProperties(context.getProperties());
    }

    protected static String getStateKey(String prefix, String tableName) {
        return tableName.toLowerCase().replace(prefix,"");
    }

    protected Map<String,String> getDefaultMaxValueProperties(final Map<PropertyDescriptor, String> properties){
        final Map<String,String> defaultMaxValues = new HashMap<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : properties.entrySet()) {
            final String key = entry.getKey().getName();

            if(!key.startsWith(INITIAL_TIMESTAMP_PROP_START)) {
                continue;
            }

            defaultMaxValues.put(key.substring(INITIAL_TIMESTAMP_PROP_START.length()), entry.getValue());
        }

        return defaultMaxValues;
    }
}
