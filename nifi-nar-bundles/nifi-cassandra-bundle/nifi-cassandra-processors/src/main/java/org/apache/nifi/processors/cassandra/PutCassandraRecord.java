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

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
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
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
            .name("put-cassandra-record-reader")
            .displayName("Record Reader")
            .description("Specifies the type of Record Reader controller service to use for parsing the incoming data " +
                    "and determining the schema")
            .identifiesControllerService(RecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-statement-type")
            .displayName("Statement Type")
            .description("Specifies the type of CQL Statement to generate.")
            .required(true)
            .defaultValue(INSERT_TYPE.getValue())
            .allowableValues(UPDATE_TYPE, INSERT_TYPE, STATEMENT_TYPE_USE_ATTR_TYPE)
            .build();

    static final PropertyDescriptor UPDATE_METHOD = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-update-method")
            .displayName("Update Method")
            .description("Specifies the method to use to SET the values. This property is used if the Statement Type is " +
                    "UPDATE and ignored otherwise.")
            .required(false)
            .defaultValue(SET_TYPE.getValue())
            .allowableValues(INCR_TYPE, DECR_TYPE, SET_TYPE, UPDATE_METHOD_USE_ATTR_TYPE)
            .build();

    static final PropertyDescriptor UPDATE_KEYS = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-update-keys")
            .displayName("Update Keys")
            .description("A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. "
                    + "If the Statement Type is UPDATE and this property is not set, the conversion to CQL will fail. "
                    + "This property is ignored if the Statement Type is not UPDATE.")
            .addValidator(StandardValidators.createListValidator(true, false, StandardValidators.NON_EMPTY_VALIDATOR))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-table")
            .displayName("Table name")
            .description("The name of the Cassandra table to which the records have to be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-batch-size")
            .displayName("Batch size")
            .description("Specifies the number of 'Insert statements' to be grouped together to execute as a batch (BatchStatement)")
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor BATCH_STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("put-cassandra-record-batch-statement-type")
            .displayName("Batch Statement Type")
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

    private final static List<PropertyDescriptor> propertyDescriptors = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_PROVIDER_SERVICE, CONTACT_POINTS, KEYSPACE, TABLE, STATEMENT_TYPE, UPDATE_KEYS, UPDATE_METHOD, CLIENT_AUTH, USERNAME, PASSWORD,
            RECORD_READER_FACTORY, BATCH_SIZE, CONSISTENCY_LEVEL, BATCH_STATEMENT_TYPE, PROP_SSL_CONTEXT_SERVICE));

    private final static Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

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
        FlowFile inputFlowFile = session.get();

        if (inputFlowFile == null) {
            return;
        }

        final String cassandraTable = context.getProperty(TABLE).evaluateAttributeExpressions(inputFlowFile).getValue();
        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final String serialConsistencyLevel = context.getProperty(CONSISTENCY_LEVEL).getValue();
        final String updateKeys = context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(inputFlowFile).getValue();

        // Get the statement type from the attribute if necessary
        final String statementTypeProperty = context.getProperty(STATEMENT_TYPE).getValue();
        String statementType = statementTypeProperty;
        if (STATEMENT_TYPE_USE_ATTR_TYPE.getValue().equals(statementTypeProperty)) {
            statementType = inputFlowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
        }

        // Get the update method from the attribute if necessary
        final String updateMethodProperty = context.getProperty(UPDATE_METHOD).getValue();
        String updateMethod = updateMethodProperty;
        if (UPDATE_METHOD_USE_ATTR_TYPE.getValue().equals(updateMethodProperty)) {
            updateMethod = inputFlowFile.getAttribute(UPDATE_METHOD_ATTRIBUTE);
        }


        // Get the batch statement type from the attribute if necessary
        final String batchStatementTypeProperty = context.getProperty(BATCH_STATEMENT_TYPE).getValue();
        String batchStatementType = batchStatementTypeProperty;
        if (BATCH_STATEMENT_TYPE_USE_ATTR_TYPE.getValue().equals(batchStatementTypeProperty)) {
            batchStatementType = inputFlowFile.getAttribute(BATCH_STATEMENT_TYPE_ATTRIBUTE).toUpperCase();
        }
        if (StringUtils.isEmpty(batchStatementType)) {
            throw new IllegalArgumentException(format("Batch Statement Type is not specified, FlowFile %s", inputFlowFile));
        }

        final BatchStatement batchStatement;
        final Session connectionSession = cassandraSession.get();
        final AtomicInteger recordsAdded = new AtomicInteger(0);
        final StopWatch stopWatch = new StopWatch(true);

        boolean error = false;

        try (final InputStream inputStream = session.read(inputFlowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(inputFlowFile, inputStream, getLogger())){

            // throw an exception if statement type is not set
            if (StringUtils.isEmpty(statementType)) {
                throw new IllegalArgumentException(format("Statement Type is not specified, FlowFile %s", inputFlowFile));
            }

            // throw an exception if the statement type is set to update and updateKeys is empty
            if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType) && StringUtils.isEmpty(updateKeys)) {
                    throw new IllegalArgumentException(format("Update Keys are not specified, FlowFile %s", inputFlowFile));
            }

            // throw an exception if the Update Method is Increment or Decrement and the batch statement type is not UNLOGGED or COUNTER
            if (INCR_TYPE.getValue().equalsIgnoreCase(updateMethod) || DECR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                if (!(UNLOGGED_TYPE.getValue().equalsIgnoreCase(batchStatementType) || COUNTER_TYPE.getValue().equalsIgnoreCase(batchStatementType))) {
                    throw new IllegalArgumentException(format("Increment/Decrement Update Method can only be used with COUNTER " +
                            "or UNLOGGED Batch Statement Type, FlowFile %s", inputFlowFile));
                }
            }

            final RecordSchema schema = reader.getSchema();
            Record record;

            batchStatement = new BatchStatement(BatchStatement.Type.valueOf(batchStatementType));
            batchStatement.setSerialConsistencyLevel(ConsistencyLevel.valueOf(serialConsistencyLevel));

            while((record = reader.nextRecord()) != null) {
                Map<String, Object> recordContentMap = (Map<String, Object>) DataTypeUtils
                        .convertRecordFieldtoObject(record, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));

                Statement query;
                if (INSERT_TYPE.getValue().equalsIgnoreCase(statementType)) {
                    query = generateInsert(cassandraTable, schema, recordContentMap);
                } else if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType)) {
                    query = generateUpdate(cassandraTable, schema, updateKeys, updateMethod, recordContentMap);
                } else {
                    throw new IllegalArgumentException(format("Statement Type %s is not valid, FlowFile %s", statementType, inputFlowFile));
                }
                batchStatement.add(query);

                if (recordsAdded.incrementAndGet() == batchSize) {
                    connectionSession.execute(batchStatement);
                    batchStatement.clear();
                    recordsAdded.set(0);
                }
            }

            if (batchStatement.size() != 0) {
                connectionSession.execute(batchStatement);
                batchStatement.clear();
            }

        } catch (Exception e) {
            error = true;
            getLogger().error("Unable to write the records into Cassandra table due to {}", new Object[] {e});
            session.transfer(inputFlowFile, REL_FAILURE);
        } finally {
            if (!error) {
                stopWatch.stop();
                long duration = stopWatch.getDuration(TimeUnit.MILLISECONDS);
                String transitUri = "cassandra://" + connectionSession.getCluster().getMetadata().getClusterName() + "." + cassandraTable;

                session.getProvenanceReporter().send(inputFlowFile, transitUri, "Inserted " + recordsAdded.get() + " records", duration);
                session.transfer(inputFlowFile, REL_SUCCESS);
            }
        }

    }

    protected Statement generateUpdate(String cassandraTable, RecordSchema schema, String updateKeys, String updateMethod, Map<String, Object> recordContentMap) {
        Update updateQuery;

        // Split up the update key names separated by a comma, should not be empty
        final Set<String> updateKeyNames;
        updateKeyNames = Arrays.stream(updateKeys.split(","))
                .map(String::trim)
                .filter(StringUtils::isNotEmpty)
                .collect(Collectors.toSet());
        if (updateKeyNames.isEmpty()) {
            throw new IllegalArgumentException("No Update Keys were specified");
        }

        // Verify if all update keys are present in the record
        for (String updateKey : updateKeyNames) {
            if (!schema.getFieldNames().contains(updateKey)) {
                throw new IllegalArgumentException("Update key '" + updateKey + "' is not present in the record schema");
            }
        }

        // Prepare keyspace/table names
        if (cassandraTable.contains(".")) {
            String[] keyspaceAndTable = cassandraTable.split("\\.");
            updateQuery = QueryBuilder.update(keyspaceAndTable[0], keyspaceAndTable[1]);
        } else {
            updateQuery = QueryBuilder.update(cassandraTable);
        }

        // Loop through the field names, setting those that are not in the update key set, and using those
        // in the update key set as conditions.
        for (String fieldName : schema.getFieldNames()) {
            Object fieldValue = recordContentMap.get(fieldName);

            if (updateKeyNames.contains(fieldName)) {
                updateQuery.where(QueryBuilder.eq(fieldName, fieldValue));
            } else {
                Assignment assignment;
                if (SET_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                    assignment = QueryBuilder.set(fieldName, fieldValue);
                } else if (INCR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                    assignment = QueryBuilder.incr(fieldName, convertFieldObjectToLong(fieldName, fieldValue));
                } else if (DECR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                    assignment = QueryBuilder.decr(fieldName, convertFieldObjectToLong(fieldName, fieldValue));
                } else {
                    throw new IllegalArgumentException("Update Method '" + updateMethod + "' is not valid.");
                }
                updateQuery.with(assignment);
            }
        }
        return updateQuery;
    }

    private Long convertFieldObjectToLong(String name, Object value) {
        if (!(value instanceof Number)) {
            throw new IllegalArgumentException("Field '" + name + "' is not of type Number");
        }
        return ((Number) value).longValue();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
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

    protected Statement generateInsert(String cassandraTable, RecordSchema schema, Map<String, Object> recordContentMap) {
        Insert insertQuery;
        if (cassandraTable.contains(".")) {
            String[] keyspaceAndTable = cassandraTable.split("\\.");
            insertQuery = QueryBuilder.insertInto(keyspaceAndTable[0], keyspaceAndTable[1]);
        } else {
            insertQuery = QueryBuilder.insertInto(cassandraTable);
        }
        for (String fieldName : schema.getFieldNames()) {
            Object value = recordContentMap.get(fieldName);

            if (value != null && value.getClass().isArray()) {
                Object[] array = (Object[]) value;

                if (array.length > 0) {
                    if (array[0] instanceof Byte) {
                        Object[] temp = (Object[]) value;
                        byte[] newArray = new byte[temp.length];
                        for (int x = 0; x < temp.length; x++) {
                            newArray[x] = (Byte) temp[x];
                        }
                        value = ByteBuffer.wrap(newArray);
                    }
                }
            }

            if (schema.getDataType(fieldName).isPresent()) {
                DataType fieldDataType = schema.getDataType(fieldName).get();
                if (fieldDataType.getFieldType() == RecordFieldType.ARRAY) {
                    if (((ArrayDataType)fieldDataType).getElementType().getFieldType() == RecordFieldType.STRING) {
                        value = Arrays.stream((Object[])value).toArray(String[]::new);
                    }
                }
            }

            insertQuery.value(fieldName, value);
        }
        return insertQuery;
    }

    @OnUnscheduled
    public void stop(ProcessContext context) {
        super.stop(context);
    }

    @OnShutdown
    public void shutdown(ProcessContext context) {
        super.stop(context);
    }

}
