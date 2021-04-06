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
package org.apache.nifi.accumulo.processors;


import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.accumulo.controllerservices.BaseAccumuloService;
import org.apache.nifi.accumulo.data.AccumuloRecordConfiguration;

import javax.xml.bind.DatatypeConverter;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@EventDriven
@SupportsBatching
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"hadoop", "accumulo", "put", "record"})
@DynamicProperties({
        @DynamicProperty(name = "visibility.<COLUMN FAMILY>", description = "Visibility label for everything under that column family " +
                "when a specific label for a particular column qualifier is not available.", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                value = "visibility label for <COLUMN FAMILY>"
        ),
        @DynamicProperty(name = "visibility.<COLUMN FAMILY>.<COLUMN QUALIFIER>", description = "Visibility label for the specified column qualifier " +
                "qualified by a configured column family.", expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                value = "visibility label for <COLUMN FAMILY>:<COLUMN QUALIFIER>."
        )
})
/**
 * Purpose and Design: Requires a connector be defined by way of an AccumuloService object. This class
 * simply extens BaseAccumuloProcessor to extract records from a flow file. The location of a record field value can be
 * placed into the value or part of the column qualifier ( this can/may change )
 *
 * Supports deletes. If the delete flag is used we'll delete keys found within that flow file.
 */
public class PutAccumuloRecord extends BaseAccumuloProcessor {

    protected static final PropertyDescriptor MEMORY_SIZE = new PropertyDescriptor.Builder()
            .name("Memory Size")
            .description("The maximum memory size Accumulo at any one time from the record set.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("10 MB")
            .build();

    protected static final PropertyDescriptor COLUMN_FAMILY = new PropertyDescriptor.Builder()
            .name("Column Family")
            .description("The Column Family to use when inserting data into Accumulo")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor COLUMN_FAMILY_FIELD = new PropertyDescriptor.Builder()
            .name("Column Family Field")
            .description("Field name used as the column family if one is not specified above.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor DELETE_KEY = new PropertyDescriptor.Builder()
            .name("delete-key")
            .displayName("Delete Key")
            .description("Deletes the key")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    protected static final PropertyDescriptor RECORD_IN_QUALIFIER = new PropertyDescriptor.Builder()
            .name("record-value-in-qualifier")
            .displayName("Record Value In Qualifier")
            .description("Places the record value into the column qualifier instead of the value.")
            .required(false)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor FLUSH_ON_FLOWFILE = new PropertyDescriptor.Builder()
            .name("flush-on-flow-file")
            .displayName("Flush Every FlowFile")
            .description("Flushes the table writer on every flow file.")
            .required(true)
            .defaultValue("True")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor FIELD_DELIMITER_AS_HEX = new PropertyDescriptor.Builder()
            .name("field-delimiter-as-hex")
            .displayName("Hex Encode Field Delimiter")
            .description("Allows you to hex encode the delimiter as a character. So 0x00 places a null character between the record name and value.")
            .required(false)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    protected static final PropertyDescriptor FIELD_DELIMITER = new PropertyDescriptor.Builder()
            .name("field-delimiter")
            .displayName("Field Delimiter")
            .description("Delimiter between the record value and name. ")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("record-reader")
            .displayName("Record Reader")
            .description("Specifies the Controller Service to use for parsing incoming data and determining the data's schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    protected static final PropertyDescriptor ROW_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("Row Identifier Field Name")
            .description("Specifies the name of a record field whose value should be used as the row id for the given record." +
                    " If EL defines a value that is not a field name that will be used as the row identifier.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("timestamp-field")
            .displayName("Timestamp Field")
            .description("Specifies the name of a record field whose value should be used as the timestamp. If empty a timestamp will be recorded as the time of insertion")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    protected static final PropertyDescriptor VISIBILITY_PATH = new PropertyDescriptor.Builder()
            .name("visibility-path")
            .displayName("Visibility String Record Path Root")
            .description("A record path that points to part of the record which contains a path to a mapping of visibility strings to record paths")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    protected static final PropertyDescriptor DEFAULT_VISIBILITY = new PropertyDescriptor.Builder()
            .name("default-visibility")
            .displayName("Default Visibility")
            .description("Default visibility when VISIBILITY_PATH is not defined. ")
            .required(false)
            .addValidator(Validator.VALID)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("A FlowFile is routed to this relationship after it has been successfully stored in Accumulo")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("A FlowFile is routed to this relationship if it cannot be sent to Accumulo")
            .build();


    /**
     * Connector service which provides us a connector if the configuration is correct.
     */
    protected BaseAccumuloService accumuloConnectorService;

    /**
     * Connector that we need to persist while we are operational.
     */
    protected AccumuloClient client;

    /**
     * Table writer that will close when we shutdown or upon error.
     */
    private MultiTableBatchWriter tableWriter = null;

    /**
     * Record path cache
     */
    protected RecordPathCache recordPathCache;


    /**
     * Flushes the tableWriter on every flow file if true.
     */
    protected boolean flushOnEveryFlow;

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        rels.add(REL_FAILURE);
        return rels;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Collection<ValidationResult> set = new HashSet<>();
        if (!validationContext.getProperty(COLUMN_FAMILY).isSet() && !validationContext.getProperty(COLUMN_FAMILY_FIELD).isSet())
            set.add(new ValidationResult.Builder().explanation("Column Family OR Column family field name must be defined").build());
        else if (validationContext.getProperty(COLUMN_FAMILY).isSet() && validationContext.getProperty(COLUMN_FAMILY_FIELD).isSet())
            set.add(new ValidationResult.Builder().explanation("Column Family OR Column family field name must be defined, but not both").build());
        return set;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        accumuloConnectorService = context.getProperty(ACCUMULO_CONNECTOR_SERVICE).asControllerService(BaseAccumuloService.class);
        final Double maxBytes = context.getProperty(MEMORY_SIZE).asDataSize(DataUnit.B);
        this.client = accumuloConnectorService.getClient();
        BatchWriterConfig writerConfig = new BatchWriterConfig();
        writerConfig.setMaxWriteThreads(context.getProperty(THREADS).asInteger());
        writerConfig.setMaxMemory(maxBytes.longValue());
        writerConfig.setTimeout(context.getProperty(ACCUMULO_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).longValue(), TimeUnit.SECONDS);
        tableWriter = client.createMultiTableBatchWriter(writerConfig);
        flushOnEveryFlow = context.getProperty(FLUSH_ON_FLOWFILE).asBoolean();
        if (!flushOnEveryFlow){
            writerConfig.setMaxLatency(60, TimeUnit.SECONDS);
        }

        if (context.getProperty(CREATE_TABLE).asBoolean() && !context.getProperty(TABLE_NAME).isExpressionLanguagePresent()) {
            final Map<String, String> flowAttributes = new HashMap<>();
            final String table = context.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowAttributes).getValue();
              final TableOperations tableOps = this.client.tableOperations();
              if (!tableOps.exists(table)) {
                  getLogger().info("Creating " + table + " table.");
                  try {
                      tableOps.create(table);
                  } catch (TableExistsException te) {
                      // can safely ignore
                  } catch (AccumuloSecurityException | AccumuloException e) {
                      getLogger().info("Accumulo or Security error creating. Continuing... " + table + ". ", e);
                  }
              }
        }
    }


    @OnUnscheduled
    @OnDisabled
    public synchronized void shutdown(){
        /**
         * Close the writer when we are shut down.
         */
        if (null != tableWriter){
            try {
                tableWriter.close();
            } catch (MutationsRejectedException e) {
                getLogger().error("Mutations were rejected",e);
            }
            tableWriter = null;
        }
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(baseProperties);
        properties.add(RECORD_READER_FACTORY);
        properties.add(ROW_FIELD_NAME);
        properties.add(ROW_FIELD_NAME);
        properties.add(COLUMN_FAMILY);
        properties.add(COLUMN_FAMILY_FIELD);
        properties.add(DELETE_KEY);
        properties.add(FLUSH_ON_FLOWFILE);
        properties.add(FIELD_DELIMITER);
        properties.add(FIELD_DELIMITER_AS_HEX);
        properties.add(MEMORY_SIZE);
        properties.add(RECORD_IN_QUALIFIER);
        properties.add(TIMESTAMP_FIELD);
        properties.add(VISIBILITY_PATH);
        properties.add(DEFAULT_VISIBILITY);
        return properties;
    }


    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        final FlowFile flowFile = processSession.get();
        if (flowFile == null) {
            return;
        }

        final RecordReaderFactory recordParserFactory = processContext.getProperty(RECORD_READER_FACTORY)
                .asControllerService(RecordReaderFactory.class);

        final String recordPathText = processContext.getProperty(VISIBILITY_PATH).getValue();
        final String defaultVisibility = processContext.getProperty(DEFAULT_VISIBILITY).isSet() ? processContext.getProperty(DEFAULT_VISIBILITY).getValue() : null;

        final String tableName = processContext.getProperty(TABLE_NAME).evaluateAttributeExpressions(flowFile).getValue();

        accumuloConnectorService.renewTgtIfNecessary();

        // create the table if EL is present, create table is true and the table does not exist.
        if (processContext.getProperty(TABLE_NAME).isExpressionLanguagePresent() && processContext.getProperty(CREATE_TABLE).asBoolean()) {
            final TableOperations tableOps = this.client.tableOperations();
            if (!tableOps.exists(tableName)) {
                getLogger().info("Creating " + tableName + " table.");
                try {
                    tableOps.create(tableName);
                } catch (TableExistsException te) {
                    // can safely ignore, though we shouldn't arrive here due to table.exists called, but it's possible
                    // that with multiple threads two could attempt table creation concurrently. We don't want that
                    // to be a failure.
                } catch (AccumuloSecurityException | AccumuloException e) {
                    throw new ProcessException("Accumulo or Security error creating. Continuing... " + tableName + ". ",e);
                }
            }
        }

        AccumuloRecordConfiguration builder = AccumuloRecordConfiguration.Builder.newBuilder()
                .setTableName(tableName)
                .setColumnFamily(processContext.getProperty(COLUMN_FAMILY).evaluateAttributeExpressions(flowFile).getValue())
                .setColumnFamilyField(processContext.getProperty(COLUMN_FAMILY_FIELD).evaluateAttributeExpressions(flowFile).getValue())
                .setRowField(processContext.getProperty(ROW_FIELD_NAME).evaluateAttributeExpressions(flowFile).getValue())
                .setEncodeFieldDelimiter(processContext.getProperty(FIELD_DELIMITER_AS_HEX).asBoolean())
                .setFieldDelimiter(processContext.getProperty(FIELD_DELIMITER).isSet() ? processContext.getProperty(FIELD_DELIMITER).evaluateAttributeExpressions(flowFile).getValue() : "")
                .setQualifierInKey(processContext.getProperty(RECORD_IN_QUALIFIER).isSet() ? processContext.getProperty(RECORD_IN_QUALIFIER).asBoolean() : false)
                .setDelete(processContext.getProperty(DELETE_KEY).isSet() ? processContext.getProperty(DELETE_KEY).evaluateAttributeExpressions(flowFile).asBoolean() : false)
                .setTimestampField(processContext.getProperty(TIMESTAMP_FIELD).evaluateAttributeExpressions(flowFile).getValue()).build();


        RecordPath recordPath = null;
        if (recordPathCache != null && !StringUtils.isEmpty(recordPathText)) {
            recordPath = recordPathCache.getCompiled(recordPathText);
        }

        boolean failed = false;
        Mutation prevMutation=null;
        try (final InputStream in = processSession.read(flowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(flowFile, in, getLogger())) {
            Record record;
            /**
             * HBase supports a restart point. This may be something that we can/should add if needed.
             */
            while ((record = reader.nextRecord()) != null) {
                prevMutation = createMutation(prevMutation, processContext, record, reader.getSchema(), recordPath, flowFile,defaultVisibility,  builder);

            }
            addMutation(builder.getTableName(),prevMutation);
        } catch (Exception ex) {
            getLogger().error("Failed to put records to Accumulo.", ex);
            failed = true;
        }

        if (flushOnEveryFlow){
            try {
                tableWriter.flush();
            } catch (MutationsRejectedException e) {
                throw new ProcessException(e);
            }
        }


        if (failed) {
            processSession.transfer(processSession.penalize(flowFile), REL_FAILURE);
        } else {
            processSession.transfer(flowFile, REL_SUCCESS);
        }
    }

    /**
     * Adapted from HBASEUtils. Their approach seemed ideal for what our intent is here.
     * @param columnFamily column family from which to extract the visibility or to execute an expression against
     * @param columnQualifier column qualifier from which to extract the visibility or to execute an expression against
     * @param flowFile flow file being written
     * @param context process context
     * @return
     */
    public static String produceVisibility(String columnFamily, String columnQualifier, FlowFile flowFile, ProcessContext context) {
        if (org.apache.commons.lang3.StringUtils.isNotEmpty(columnFamily)) {
            return null;
        }
        String lookupKey = String.format("visibility.%s%s%s", columnFamily, !org.apache.commons.lang3.StringUtils.isNotEmpty(columnQualifier) ? "." : "", columnQualifier);
        String fromAttribute = flowFile.getAttribute(lookupKey);

        if (fromAttribute == null && !org.apache.commons.lang3.StringUtils.isBlank(columnQualifier)) {
            String lookupKeyFam = String.format("visibility.%s", columnFamily);
            fromAttribute = flowFile.getAttribute(lookupKeyFam);
        }

        if (fromAttribute != null) {
            return fromAttribute;
        } else {
            PropertyValue descriptor = context.getProperty(lookupKey);
            if (descriptor == null || !descriptor.isSet()) {
                descriptor = context.getProperty(String.format("visibility.%s", columnFamily));
            }

            String retVal = descriptor != null ? descriptor.evaluateAttributeExpressions(flowFile).getValue() : null;

            return retVal;
        }
    }

    private void addMutation(final String tableName, final Mutation m) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        tableWriter.getBatchWriter(tableName).addMutation(m);

    }

    /**
     * Returns the row provided the record schema
     * @param record record against which we are evaluating
     * @param schema Record schema
     * @param rowOrFieldName Row identifier or field name
     * @return Text object containing the resulting row.
     */
    private Text getRow(final Record record,
                        final RecordSchema schema,
                        final String rowOrFieldName){
        if ( !schema.getFieldNames().contains(rowOrFieldName) ){
            return new Text(rowOrFieldName);
        } else{
            return new Text(record.getAsString(rowOrFieldName));
        }
    }

    /**
     * Creates a mutation with the provided arguments
     * @param prevMutation previous mutation, to append to if in the same row.
     * @param context process context.
     * @param record record object extracted from the flow file
     * @param schema schema for this record
     * @param recordPath record path for visibility extraction
     * @param flowFile flow file
     * @param defaultVisibility default visibility
     * @param config configuration of this instance.
     * @return Returns the Mutation to insert
     * @throws AccumuloSecurityException Error accessing Accumulo
     * @throws AccumuloException Non security ( or table ) related Accumulo exceptions writing to the store.
     * @throws TableNotFoundException Table not found on the cluster
     */
    protected Mutation createMutation(final Mutation prevMutation,
                                      final ProcessContext context,
                                      final Record record,
                                      final RecordSchema schema,
                                      final RecordPath recordPath,
                                      final FlowFile flowFile,
                                      final String defaultVisibility,
                                      AccumuloRecordConfiguration config) throws AccumuloSecurityException, AccumuloException, TableNotFoundException {
        Mutation m=null;
        if (record != null) {

            final Long timestamp;
            Set<String> fieldsToSkip = new HashSet<>();
            if (!StringUtils.isBlank(config.getTimestampField())) {
                try {
                    timestamp = record.getAsLong(config.getTimestampField());
                    fieldsToSkip.add(config.getTimestampField());
                } catch (Exception e) {
                    throw new AccumuloException("Could not convert " + config.getTimestampField() + " to a long", e);
                }

                if (timestamp == null) {
                    getLogger().warn("The value of timestamp field " + config.getTimestampField() + " was null, record will be inserted with latest timestamp");
                }
            } else {
                timestamp = null;
            }



            RecordField visField = null;
            Map visSettings = null;
            if (recordPath != null) {
                final RecordPathResult result = recordPath.evaluate(record);
                FieldValue fv = result.getSelectedFields().findFirst().get();
                visField = fv.getField();
                if (null != visField)
                fieldsToSkip.add(visField.getFieldName());
                visSettings = (Map)fv.getValue();
            }


            if (null != prevMutation){
                Text row = new Text(prevMutation.getRow());
                Text curRow = getRow(record,schema,config.getRowField());
                if (row.equals(curRow)){
                    m = prevMutation;
                } else{
                    m = new Mutation(curRow);
                    addMutation(config.getTableName(),prevMutation);
                }
            } else{
                Text row = getRow(record,schema,config.getRowField());
                m = new Mutation(row);
            }

            fieldsToSkip.add(config.getRowField());

            String columnFamily = config.getColumnFamily();
            if (StringUtils.isBlank(columnFamily) && !StringUtils.isBlank(config.getColumnFamilyField())) {
                final String cfField = config.getColumnFamilyField();
                columnFamily = record.getAsString(cfField);
                fieldsToSkip.add(cfField);
            } else if (StringUtils.isBlank(columnFamily) && StringUtils.isBlank(config.getColumnFamilyField())){
                throw new IllegalArgumentException("Invalid configuration for column family " + columnFamily + " and " + config.getColumnFamilyField());
            }
            final Text cf = new Text(columnFamily);

            for (String name : schema.getFieldNames().stream().filter(p->!fieldsToSkip.contains(p)).collect(Collectors.toList())) {
                String visString = (visField != null && visSettings != null && visSettings.containsKey(name))
                        ? (String)visSettings.get(name) : defaultVisibility;

                Text cq = new Text(name);
                final Value value;
                String recordValue  = record.getAsString(name);
                if (config.getQualifierInKey()){
                    final String delim = config.getFieldDelimiter();
                    if (!StringUtils.isEmpty(delim)) {
                        if (config.getEncodeDelimiter()) {
                            byte [] asHex = DatatypeConverter.parseHexBinary(delim);
                            cq.append(asHex, 0, asHex.length);
                        }else{
                            cq.append(delim.getBytes(), 0, delim.length());
                        }
                    }
                    cq.append(recordValue.getBytes(),0,recordValue.length());
                    value = new Value();
                } else{
                    value = new Value(recordValue.getBytes());
                }

                if (StringUtils.isBlank(visString)) {
                    visString = produceVisibility(cf.toString(), cq.toString(), flowFile, context);
                }

                ColumnVisibility cv = new ColumnVisibility();
                if (StringUtils.isBlank(visString)) {
                    if (!StringUtils.isBlank(defaultVisibility)) {
                        cv = new ColumnVisibility(defaultVisibility);
                    }
                } else {
                    cv = new ColumnVisibility(visString);
                }

                if (null != timestamp) {
                    if (config.isDeleteKeys()) {
                        m.putDelete(cf, cq, cv, timestamp);
                    } else {
                        m.put(cf, cq, cv, timestamp, value);
                    }
                } else{
                    if (config.isDeleteKeys())
                        m.putDelete(cf, cq, cv);
                    else
                        m.put(cf, cq, cv, value);
                }
            }



        }

        return m;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        /**
         * Adapted from HBase puts. This is a good approach and one that we should adopt here, too.
         */
        if (propertyDescriptorName.startsWith("visibility.")) {
            String[] parts = propertyDescriptorName.split("\\.");
            String displayName;
            String description;

            if (parts.length == 2) {
                displayName = String.format("Column Family %s Default Visibility", parts[1]);
                description = String.format("Default visibility setting for %s", parts[1]);
            } else if (parts.length == 3) {
                displayName = String.format("Column Qualifier %s.%s Default Visibility", parts[1], parts[2]);
                description = String.format("Default visibility setting for %s.%s", parts[1], parts[2]);
            } else {
                return null;
            }

            return new PropertyDescriptor.Builder()
                    .name(propertyDescriptorName)
                    .displayName(displayName)
                    .description(description)
                    .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .dynamic(true)
                    .build();
        }

        return null;
    }
}
