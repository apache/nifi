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
package org.apache.nifi.processors.cql;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
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
import org.apache.nifi.processors.cql.constants.BatchStatementType;
import org.apache.nifi.processors.cql.constants.StatementType;
import org.apache.nifi.processors.cql.constants.UpdateType;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.service.cql.api.CQLExecutionService;
import org.apache.nifi.service.cql.api.UpdateMethod;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static java.lang.String.format;
import static java.util.Collections.EMPTY_LIST;
import static org.apache.nifi.processors.cql.constants.BatchStatementType.BATCH_STATEMENT_TYPE_USE_ATTR_TYPE;
import static org.apache.nifi.processors.cql.constants.BatchStatementType.COUNTER_TYPE;
import static org.apache.nifi.processors.cql.constants.BatchStatementType.LOGGED_TYPE;
import static org.apache.nifi.processors.cql.constants.BatchStatementType.UNLOGGED_TYPE;
import static org.apache.nifi.processors.cql.constants.StatementType.INSERT_TYPE;
import static org.apache.nifi.processors.cql.constants.StatementType.STATEMENT_TYPE_USE_ATTR_TYPE;
import static org.apache.nifi.processors.cql.constants.StatementType.UPDATE_TYPE;
import static org.apache.nifi.processors.cql.constants.UpdateType.DECR_TYPE;
import static org.apache.nifi.processors.cql.constants.UpdateType.INCR_TYPE;

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
public class PutCQLRecord extends AbstractCQLProcessor {
    static final String STATEMENT_TYPE_ATTRIBUTE = "cql.statement.type";

    static final String UPDATE_METHOD_ATTRIBUTE = "cql.update.method";

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
            .allowableValues(StatementType.class)
            .build();

    static final PropertyDescriptor UPDATE_METHOD = new PropertyDescriptor.Builder()
            .name("Update Method")
            .description("Specifies the method to use to SET the values. This property is used if the Statement Type is " +
                    "UPDATE and ignored otherwise.")
            .required(false)
            .defaultValue(UpdateType.SET_TYPE.getValue())
            .allowableValues(UpdateType.class)
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
            .name("Table name")
            .description("The name of the Cassandra table to which the records have to be written.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch size")
            .description("Specifies the number of 'Insert statements' to be grouped together to execute as a batch (BatchStatement)")
            .defaultValue("100")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(true)
            .build();

    static final PropertyDescriptor BATCH_STATEMENT_TYPE = new PropertyDescriptor.Builder()
            .name("Batch Statement Type")
            .description("Specifies the type of 'Batch Statement' to be used.")
            .allowableValues(BatchStatementType.class)
            .defaultValue(LOGGED_TYPE.getValue())
            .required(false)
            .build();

    private final static List<PropertyDescriptor> propertyDescriptors = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_PROVIDER_SERVICE, TABLE, STATEMENT_TYPE, UPDATE_KEYS, UPDATE_METHOD,
            RECORD_READER_FACTORY, BATCH_SIZE, BATCH_STATEMENT_TYPE));

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
        if (UpdateType.UPDATE_METHOD_USE_ATTR_TYPE.getValue().equals(updateMethodProperty)) {
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

        final AtomicInteger recordsAdded = new AtomicInteger(0);
        final StopWatch stopWatch = new StopWatch(true);

        boolean error = false;

        try (final InputStream inputStream = session.read(inputFlowFile);
             final RecordReader reader = recordParserFactory.createRecordReader(inputFlowFile, inputStream, getLogger())) {

            // throw an exception if statement type is not set
            if (StringUtils.isEmpty(statementType)) {
                throw new IllegalArgumentException(format("Statement Type is not specified, FlowFile %s", inputFlowFile));
            }

            // throw an exception if the statement type is set to update and updateKeys is empty
            if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType) && StringUtils.isEmpty(updateKeys)) {
                    throw new IllegalArgumentException(format("Update Keys are not specified, FlowFile %s", inputFlowFile));
            }

            final List<String> updateKeyNames = UPDATE_TYPE.getValue().equalsIgnoreCase(statementType) ? Stream.of(updateKeys
                            .split(","))
                    .map(key -> key.trim())
                    .filter(key -> StringUtils.isNotEmpty(key))
                    .toList() : EMPTY_LIST;

            // throw an exception if the Update Method is Increment or Decrement and the batch statement type is not UNLOGGED or COUNTER
            if (INCR_TYPE.getValue().equalsIgnoreCase(updateMethod) || DECR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                if (!(UNLOGGED_TYPE.getValue().equalsIgnoreCase(batchStatementType) || COUNTER_TYPE.getValue().equalsIgnoreCase(batchStatementType))) {
                    throw new IllegalArgumentException(format("Increment/Decrement Update Method can only be used with COUNTER " +
                            "or UNLOGGED Batch Statement Type, FlowFile %s", inputFlowFile));
                }
            }

            Record record;
            List<Record> recordsBatch = new ArrayList<>();
            CQLExecutionService sessionProviderService = super.cqlSessionService.get();

            while ((record = reader.nextRecord()) != null) {
                recordsBatch.add(record);

                if (recordsBatch.size() == batchSize) {
                    if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType)) {
                        sessionProviderService.update(cassandraTable, recordsBatch, updateKeyNames, UpdateMethod.valueOf(updateMethod));
                    } else {
                        sessionProviderService.insert(cassandraTable, recordsBatch);
                    }
                    recordsBatch.clear();
                }
            }

            if (!recordsBatch.isEmpty()) {
                if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType)) {
                    sessionProviderService.update(cassandraTable, recordsBatch, updateKeyNames, UpdateMethod.valueOf(updateMethod));
                } else {
                    sessionProviderService.insert(cassandraTable, recordsBatch);
                }
                recordsBatch.clear();
            }

        } catch (Exception e) {
            error = true;
            getLogger().error("Unable to write the records into Cassandra table", e);
            session.transfer(inputFlowFile, REL_FAILURE);
        } finally {
            if (!error) {
                stopWatch.stop();
                long duration = stopWatch.getDuration(TimeUnit.MILLISECONDS);

                session.getProvenanceReporter().send(inputFlowFile, super.cqlSessionService.get().getTransitUrl(cassandraTable), "Inserted " + recordsAdded.get() + " records", duration);
                session.transfer(inputFlowFile, REL_SUCCESS);
            }
        }

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


    @OnUnscheduled
    public void stop(ProcessContext context) {
        super.stop(context);
    }

    @OnShutdown
    public void shutdown(ProcessContext context) {
        super.stop(context);
    }

}
