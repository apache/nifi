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
package org.apache.nifi.processors.cassandra;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;

@Tags({"cassandra", "cql", "put", "insert", "update", "set", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This is a record aware processor that reads the content of the incoming FlowFile as individual records using the " +
        "configured 'Record Reader' and writes them to Apache Cassandra using native protocol version 3 or higher.")
@ReadsAttributes({
        @ReadsAttribute(attribute = "cql.statement.type", description = "If 'Use cql.statement.type Attribute' is selected for the Statement " +
                "Type property, the value of the cql.statement.type Attribute will be used to determine which type of statement (UPDATE, INSERT) " +
                "will be generated and executed"),
        @ReadsAttribute(attribute = "cql.update.method", description = "If 'Use cql.update.method Attribute' is selected for the Update " +
                "Method property, the value of the cql.update.method Attribute will be used to determine which operation (Set, Increment, Decrement) " +
                "will be used to generate and execute the Update statement. Ignored if the Statement Type property is not set to UPDATE"),
        @ReadsAttribute(attribute = "cql.batch.statement.type", description = "If 'Use cql.batch.statement.type Attribute' is selected for the Batch " +
                "Statement Type property, the value of the cql.batch.statement.type Attribute will be used to determine which type of batch statement " +
                "(LOGGED, UNLOGGED, COUNTER) will be generated and executed")
})
public class PutCassandraRecord extends AbstractCassandraProcessor {
    static final AllowableValue UPDATE_TYPE = new AllowableValue("UPDATE", "UPDATE",
            "Use an UPDATE statement.");
    static final AllowableValue INSERT_TYPE = new AllowableValue("INSERT", "INSERT",
            "Use an INSERT statement.");
    static final AllowableValue STATEMENT_TYPE_USE_ATTR_TYPE = new AllowableValue("USE_ATTR", "Use cql.statement.type Attribute",
            "The value of the cql.statement.type Attribute will be used to determine which type of statement (UPDATE, INSERT) " +
                    "will be generated and executed");
    static final String STATEMENT_TYPE_ATTRIBUTE = "cql.statement.type";

    static final AllowableValue INCR_TYPE = new AllowableValue("INCREMENT", "Increment",
            "Use an increment operation (+=) for the Update statement.");
    static final AllowableValue SET_TYPE = new AllowableValue("SET", "Set",
            "Use a set operation (=) for the Update statement.");
    static final AllowableValue DECR_TYPE = new AllowableValue("DECREMENT", "Decrement",
            "Use a decrement operation (-=) for the Update statement.");
    static final AllowableValue UPDATE_METHOD_USE_ATTR_TYPE = new AllowableValue("USE_ATTR", "Use cql.update.method Attribute",
            "The value of the cql.update.method Attribute will be used to determine which operation (Set, Increment, Decrement) " +
                    "will be used to generate and execute the Update statement.");
    static final String UPDATE_METHOD_ATTRIBUTE = "cql.update.method";

    static final AllowableValue LOGGED_TYPE = new AllowableValue("LOGGED", "LOGGED",
            "Use a LOGGED batch statement");
    static final AllowableValue UNLOGGED_TYPE = new AllowableValue("UNLOGGED", "UNLOGGED",
            "Use an UNLOGGED batch statement");
    static final AllowableValue COUNTER_TYPE = new AllowableValue("COUNTER", "COUNTER",
            "Use a COUNTER batch statement");
    static final AllowableValue BATCH_STATEMENT_TYPE_USE_ATTR_TYPE = new AllowableValue("USE_ATTR", "Use cql.batch.statement.type Attribute",
            "The value of the cql.batch.statement.type Attribute will be used to determine which type of batch statement (LOGGED, UNLOGGED or COUNTER) " +
                    "will be used to generate and execute the Update statement.");
    static final String BATCH_STATEMENT_TYPE_ATTRIBUTE = "cql.batch.statement.type";

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("Specifies the type of Record Reader controller service to use for parsing the incoming data " +
                    "and determining the schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Statement Type")
            .description("Specifies the type of CQL Statement to generate.")
            .required(true)
            .defaultValue(INSERT_TYPE.getValue())
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, STATEMENT_TYPE_USE_ATTR_TYPE)
            .build();

    static final PropertyDescriptor UPDATE_METHOD = new PropertyDescriptor.Builder()
            .name("Update Method")
            .description("Specifies the method to use to SET the values. This property is used if the Statement Type is " +
                    "UPDATE and ignored otherwise.")
            .required(false)
            .defaultValue(SET_TYPE.getValue())
            .allowableValues(INCR_TYPE, DECR_TYPE, SET_TYPE, UPDATE_METHOD_USE_ATTR_TYPE)
            .build();

    static final PropertyDescriptor UPDATE_KEYS = new PropertyDescriptor.Builder()
            .name("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
                    + "If the Statement Type is UPDATE and this property is not set, the conversion to CQL will fail. "
                    + "This property is ignored if the Statement Type is not UPDATE.")
            .addValidator(StandardValidators.createListValidator(true, false, StandardValidators.NON_EMPTY_VALIDATOR))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table Name")
            .description("The name of the Cassandra table to which the records have to be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("Specifies the number of 'Insert statements' to be grouped together to execute as a batch (BatchStatement).")
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor BATCH_STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Batch Statement Type")
            .description("Specifies the type of 'Batch Statement' to be used.")
            .allowableValues(LOGGED_TYPE, UNLOGGED_TYPE, COUNTER_TYPE, BATCH_STATEMENT_TYPE_USE_ATTR_TYPE)
            .defaultValue(LOGGED_TYPE.getValue())
            .required(false)
            .build();

    static final PropertyDescriptor CONSISTENCY_LEVEL = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractCassandraProcessor.CONSISTENCY_LEVEL)
            .allowableValues(ConsistencyLevel.SERIAL.name(), ConsistencyLevel.LOCAL_SERIAL.name())
            .defaultValue(ConsistencyLevel.SERIAL.name())
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS =
            Stream.concat(
                    COMMON_PROPERTY_DESCRIPTORS.stream(),
                    Stream.of(
                            TABLE,
                            STATEMENT_TYPE,
                            UPDATE_KEYS,
                            UPDATE_METHOD,
                            RECORD_READER_FACTORY,
                            BATCH_SIZE,
                            BATCH_STATEMENT_TYPE
                    )
            ).toList();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            REL_SUCCESS,
            REL_FAILURE
    );

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile inputFlowFile = session.get();

        if (inputFlowFile == null) {
            return;
        }

        final String cassandraTable = context.getProperty(TABLE).evaluateAttributeExpressions(inputFlowFile).getValue();
        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final String serialConsistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
        final String updateKeys = context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(inputFlowFile).getValue();

        String statementType = resolveAttributeOrProperty(context, inputFlowFile,
                STATEMENT_TYPE, STATEMENT_TYPE_USE_ATTR_TYPE, STATEMENT_TYPE_ATTRIBUTE);

        String updateMethod = resolveAttributeOrProperty(context, inputFlowFile,
                UPDATE_METHOD, UPDATE_METHOD_USE_ATTR_TYPE, UPDATE_METHOD_ATTRIBUTE);

        String batchStatementType = resolveAttributeOrProperty(context, inputFlowFile,
                BATCH_STATEMENT_TYPE, BATCH_STATEMENT_TYPE_USE_ATTR_TYPE, BATCH_STATEMENT_TYPE_ATTRIBUTE).toUpperCase();

        if (StringUtils.isEmpty(batchStatementType)) {
            throw new IllegalArgumentException(
                    format("Batch Statement Type is not specified, FlowFile %s", inputFlowFile));
        }

        final CqlSession connectionSession = cassandraSession.get();
        final AtomicInteger recordsAdded = new AtomicInteger(0);
        final StopWatch stopWatch = new StopWatch(true);

        boolean error = false;

        try (final InputStream inputStream = session.read(inputFlowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(inputFlowFile, inputStream, getLogger())) {

            if (StringUtils.isEmpty(statementType)) {
                throw new IllegalArgumentException(format("Statement Type is not specified, FlowFile %s", inputFlowFile));
            }

            if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType) && StringUtils.isEmpty(updateKeys)) {
                throw new IllegalArgumentException(format("Update Keys are not specified, FlowFile %s", inputFlowFile));
            }

            if (INCR_TYPE.getValue().equalsIgnoreCase(updateMethod) || DECR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                if (!(UNLOGGED_TYPE.getValue().equalsIgnoreCase(batchStatementType) || COUNTER_TYPE.getValue().equalsIgnoreCase(batchStatementType))) {
                    throw new IllegalArgumentException(format("Increment/Decrement Update Method can only be used with COUNTER " +
                            "or UNLOGGED Batch Statement Type, FlowFile %s", inputFlowFile));
                }
            }

            final RecordSchema schema = reader.getSchema();
            Record record;

            final BatchStatementBuilder batchBuilder =
                    BatchStatement.builder(DefaultBatchType.valueOf(batchStatementType))
                            .setSerialConsistencyLevel(resolveConsistencyLevel(serialConsistencyLevel));

            while ((record = reader.nextRecord()) != null) {

                Map<String, Object> recordContentMap =
                        (Map<String, Object>) DataTypeUtils.convertRecordFieldtoObject(
                                record,
                                RecordFieldType.RECORD.getRecordDataType(record.getSchema())
                        );

                BatchableStatement<?> query;

                if (INSERT_TYPE.getValue().equalsIgnoreCase(statementType)) {
                    query = (BatchableStatement<?>) generateInsert(cassandraTable, schema, recordContentMap);
                } else if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType)) {
                    query = (BatchableStatement<?>) generateUpdate(cassandraTable, schema, updateKeys, updateMethod, recordContentMap);
                } else {
                    throw new IllegalArgumentException(format("Statement Type %s is not valid, FlowFile %s", statementType, inputFlowFile));
                }

                if (getLogger().isDebugEnabled()) {
                    getLogger().debug("Query: {}", query);
                }

                batchBuilder.addStatement(query);

                if (recordsAdded.incrementAndGet() == batchSize) {
                    connectionSession.execute(batchBuilder.build());
                    batchBuilder.clearStatements();
                    recordsAdded.set(0);
                }
            }

            if (recordsAdded.get() > 0) {
                connectionSession.execute(batchBuilder.build());
                batchBuilder.clearStatements();
            }

        } catch (Exception e) {
            error = true;
            getLogger().error("Unable to write the records into Cassandra table", e);
            session.transfer(inputFlowFile, REL_FAILURE);
        } finally {
            if (!error) {
                stopWatch.stop();
                long duration = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                String transitUri = "cassandra://" + connectionSession.getMetadata().getClusterName() + "." + cassandraTable;
                session.getProvenanceReporter().send(inputFlowFile, transitUri, "Inserted " + recordsAdded.get() + " records", duration);
                session.transfer(inputFlowFile, REL_SUCCESS);
            }
        }

    }

    private String resolveAttributeOrProperty(ProcessContext context, FlowFile flowFile, PropertyDescriptor descriptor,
                                              AllowableValue useAttrValue, String attributeName) throws ProcessException {
        String propValue = context.getProperty(descriptor).getValue();
        if (useAttrValue.getValue().equals(propValue)) {
            String attrValue = flowFile.getAttribute(attributeName);
            if (StringUtils.isEmpty(attrValue)) {
                throw new ProcessException(format("%s is required on FlowFile when using USE_ATTR.", attributeName));
            }
            return attrValue;
        }
        return propValue;
    }

    private ConsistencyLevel resolveConsistencyLevel(String level) {
        switch (level.toUpperCase()) {
            case "ANY": return ConsistencyLevel.ANY;
            case "ONE": return ConsistencyLevel.ONE;
            case "TWO": return ConsistencyLevel.TWO;
            case "THREE": return ConsistencyLevel.THREE;
            case "QUORUM": return ConsistencyLevel.QUORUM;
            case "ALL": return ConsistencyLevel.ALL;
            case "LOCAL_ONE": return ConsistencyLevel.LOCAL_ONE;
            case "LOCAL_QUORUM": return ConsistencyLevel.LOCAL_QUORUM;
            case "EACH_QUORUM": return ConsistencyLevel.EACH_QUORUM;
            case "SERIAL": return ConsistencyLevel.SERIAL;
            case "LOCAL_SERIAL": return ConsistencyLevel.LOCAL_SERIAL;
            default: throw new IllegalArgumentException("Unknown Consistency Level: " + level);
        }
    }

    protected Statement<?> generateUpdate(String cassandraTable,
                                          RecordSchema schema,
                                          String updateKeys,
                                          String updateMethod,
                                          Map<String, Object> recordContentMap) {


        Set<String> keyFields = Arrays.stream(updateKeys.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        if (keyFields.isEmpty()) {
            throw new IllegalArgumentException("No Update Keys were specified");
        }

        UpdateStart updateStart;
        String keyspace = null;
        String table;

        if (cassandraTable.contains(".")) {
            String[] parts = cassandraTable.split("\\.");
            keyspace = parts[0];
            table = parts[1];
            updateStart = QueryBuilder.update(keyspace, table);
        } else {
            table = cassandraTable;
            updateStart = QueryBuilder.update(table);
        }

        Map<String, DataType> cassandraColumnTypes =
                cassandraSession.get().getMetadata()
                        .getKeyspace(keyspace != null
                                ? keyspace
                                : cassandraSession.get().getKeyspace().get().asInternal())
                        .flatMap(ks -> ks.getTable(table))
                        .orElseThrow(() -> new ProcessException("Table metadata not found for " + cassandraTable))
                        .getColumns()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                e -> e.getKey().asInternal(),
                                e -> e.getValue().getType()
                        ));

        UpdateWithAssignments updateAssignments = null;
        List<Object> bindValues = new ArrayList<>();

        for (Map.Entry<String, Object> entry : recordContentMap.entrySet()) {

            String fieldName = entry.getKey();

            if (keyFields.contains(fieldName)) {
                continue;
            }

            Object value = entry.getValue();
            if (value == null) {
                continue;
            }

            DataType cassandraType = cassandraColumnTypes.get(fieldName);
            if (cassandraType != null) {
                value = convertToCassandraWriteValue(value, cassandraType, fieldName);
            }

            if (SET_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                updateAssignments = updateAssignments == null
                        ? updateStart.setColumn(fieldName, QueryBuilder.bindMarker())
                        : updateAssignments.setColumn(fieldName, QueryBuilder.bindMarker());

            } else if (INCR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                updateAssignments = updateAssignments == null
                        ? updateStart.increment(fieldName, QueryBuilder.bindMarker())
                        : updateAssignments.increment(fieldName, QueryBuilder.bindMarker());

            } else if (DECR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                updateAssignments = updateAssignments == null
                        ? updateStart.decrement(fieldName, QueryBuilder.bindMarker())
                        : updateAssignments.decrement(fieldName, QueryBuilder.bindMarker());
            } else {
                throw new IllegalArgumentException(format("Update Method '%s' is not valid.", updateMethod));
            }

            bindValues.add(value);
        }

        if (updateAssignments == null) {
            throw new ProcessException("No update assignments found");
        }

        Update update = null;

        for (String key : keyFields) {
            Object keyValue = recordContentMap.get(key);

            if (keyValue == null) {
                throw new IllegalArgumentException(String.format("Update key '%s' missing from record data", key));
            }

            DataType cassandraType = cassandraColumnTypes.get(key);
            keyValue = convertToCassandraWriteValue(keyValue, cassandraType, key);

            update = (update == null)
                    ? updateAssignments.whereColumn(key).isEqualTo(QueryBuilder.bindMarker())
                    : update.whereColumn(key).isEqualTo(QueryBuilder.bindMarker());

            bindValues.add(keyValue);
        }

        return SimpleStatement.builder(update.asCql())
                .addPositionalValues(bindValues)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext)  {
        Set<ValidationResult> results = (Set<ValidationResult>) super.customValidate(validationContext);

        String statementType = validationContext.getProperty(STATEMENT_TYPE).getValue();

        if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType)) {
            // Check that update keys are set
            String updateKeys = validationContext.getProperty(UPDATE_KEYS).getValue();
            if (StringUtils.isEmpty(updateKeys)) {
                results.add(new ValidationResult.Builder().subject("Update statement configuration").valid(false).explanation(
                        "if the Statement Type is set to Update, then the Update Keys must be specified as well").build());
            }

            // Check that if the update method is set to increment or decrement that the batch statement type is set to
            // unlogged or counter (or USE_ATTR_TYPE, which we cannot check at this point).
            String updateMethod = validationContext.getProperty(UPDATE_METHOD).getValue();
            String batchStatementType = validationContext.getProperty(BATCH_STATEMENT_TYPE).getValue();
            if (INCR_TYPE.getValue().equalsIgnoreCase(updateMethod)
                    || DECR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                if (!(COUNTER_TYPE.getValue().equalsIgnoreCase(batchStatementType)
                        || UNLOGGED_TYPE.getValue().equalsIgnoreCase(batchStatementType)
                        || BATCH_STATEMENT_TYPE_USE_ATTR_TYPE.getValue().equalsIgnoreCase(batchStatementType))) {
                    results.add(new ValidationResult.Builder().subject("Update method configuration").valid(false).explanation(
                            "if the Update Method is set to Increment or Decrement, then the Batch Statement Type must be set " +
                                    "to either COUNTER or UNLOGGED").build());
                }
            }
        }

        return results;
    }

    protected Statement<?> generateInsert(String cassandraTable,
                                          RecordSchema schema,
                                          Map<String, Object> recordContentMap) {

        InsertInto insertInto;
        String keyspace = null;
        String table;

        if (cassandraTable.contains(".")) {
            String[] parts = cassandraTable.split("\\.");
            keyspace = parts[0];
            table = parts[1];
            insertInto = QueryBuilder.insertInto(keyspace, table);
        } else {
            table = cassandraTable;
            insertInto = QueryBuilder.insertInto(table);
        }

        Map<String, DataType> cassandraColumnTypes =
                cassandraSession.get().getMetadata()
                        .getKeyspace(keyspace != null ? keyspace : cassandraSession.get().getKeyspace().get().asInternal())
                        .flatMap(ks -> ks.getTable(table))
                        .orElseThrow(() -> new ProcessException("Table metadata not found for " + cassandraTable))
                        .getColumns()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(
                                e -> e.getKey().asInternal(),
                                e -> e.getValue().getType()
                        ));

        RegularInsert insert = null;
        List<Object> values = new ArrayList<>();

        for (String fieldName : schema.getFieldNames()) {
            Object value = recordContentMap.get(fieldName);

            if (value instanceof Object[] arr && arr.length > 0 && arr[0] instanceof Byte) {
                byte[] bytes = new byte[arr.length];
                for (int i = 0; i < arr.length; i++) {
                    bytes[i] = (Byte) arr[i];
                }
                value = ByteBuffer.wrap(bytes);
            }

            if (schema.getDataType(fieldName).isPresent()) {
                org.apache.nifi.serialization.record.DataType dt = schema.getDataType(fieldName).get();

                if (dt instanceof ArrayDataType) {
                    ArrayDataType arrayType = (ArrayDataType) dt;

                    if (arrayType.getElementType().getFieldType() == RecordFieldType.STRING) {
                        value = Arrays.stream((Object[]) value)
                                .map(Object::toString)
                                .toList();
                    }
                }
            }

            DataType cassandraType = cassandraColumnTypes.get(fieldName);

            if (value != null && cassandraType != null) {
                value = convertToCassandraWriteValue(value, cassandraType, fieldName);
            }

            insert = (insert == null)
                    ? insertInto.value(fieldName, QueryBuilder.bindMarker())
                    : insert.value(fieldName, QueryBuilder.bindMarker());

            values.add(value);
        }

        if (insert == null) {
            throw new ProcessException("No fields available to build INSERT statement");
        }

        return SimpleStatement.builder(insert.asCql())
                .addPositionalValues(values)
                .build();
    }

    @OnUnscheduled
    @Override
    public void stop(ProcessContext context) {
        super.stop(context);
    }

    @OnShutdown
    public void shutdown(ProcessContext context) {
        super.stop(context);
    }

}
