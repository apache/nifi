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
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
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
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.record.path.validation.RecordPathValidator;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.service.cql.api.constants.CqlBatchType;
import org.apache.nifi.service.cql.api.constants.UpdateMethod;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.metadata.PrimaryKeyIdentifier;
import org.apache.nifi.service.cql.api.metadata.QualifiedTableName;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.WriteOverrides;
import org.apache.nifi.util.StopWatch;

import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.apache.nifi.processors.cql.constants.BatchStatementType.BATCH_STATEMENT_TYPE_USE_ATTR_TYPE;
import static org.apache.nifi.processors.cql.constants.BatchStatementType.COUNTER_TYPE;
import static org.apache.nifi.processors.cql.constants.BatchStatementType.UNLOGGED_TYPE;
import static org.apache.nifi.processors.cql.constants.StatementType.INSERT_TYPE;
import static org.apache.nifi.processors.cql.constants.StatementType.STATEMENT_TYPE_USE_ATTR_TYPE;
import static org.apache.nifi.processors.cql.constants.StatementType.UPDATE_TYPE;
import static org.apache.nifi.processors.cql.constants.UpdateType.DECR_TYPE;
import static org.apache.nifi.processors.cql.constants.UpdateType.INCR_TYPE;

@Tags({"cassandra", "scylladb", "cql", "put", "insert", "update", "set", "record"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("This is a record aware processor that reads the content of the incoming FlowFile as individual records using the " +
        "configured 'Record Reader' and writes them to a data store that supports CQL (Cassandra or ScyllaDB primarily), as inserts or " +
        "updates, individually or in batches.")
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
@WritesAttributes({
        @WritesAttribute(attribute = "cql.records.written", description = "On failure or retry, the number of records that were already successfully "
                + "written to Cassandra/ScyllaDB before the error occurred. Since records are written in batches as they're read, earlier batches "
                + "may have already been committed even though the FlowFile as a whole did not succeed.")
})
@DynamicProperty(name = "<keyspace>.<table>.<field>", value = "A RecordPath expression",
        expressionLanguageScope = ExpressionLanguageScope.NONE,
        description = "Overrides how the named primary key column's value is resolved for records written to the given "
                + "keyspace-qualified table, in place of the default behavior of matching a record field with the same "
                + "name as the column. The RecordPath is evaluated once per record and must resolve to exactly one "
                + "value; zero or more than one is a configuration error for that record. See 'Additional Details' for "
                + "the full name format and examples, including how to supply a valid version-1 (time-based) UUID for "
                + "a timeuuid primary key column.")
@SupportsBatching
@SystemResourceConsideration(resource = SystemResource.MEMORY,
        description = "Up to 'Batch size' records are held in memory at once, per concurrent task, in their parsed form "
                + "rather than as the FlowFile's serialized bytes. Raising Batch size or the number of concurrent tasks "
                + "raises heap use proportionally.")
@SeeAlso(
        value = {ExecuteCQLQueryRecord.class},
        // The session provider services cannot be referenced by class: this module is barred from depending on either
        // of them - and so on the database drivers they carry - by the ban-database-client-dependencies enforcer rule.
        classNames = {
                "org.apache.nifi.service.cassandra.CassandraCQLExecutionService",
                "org.apache.nifi.service.scylladb.ScyllaDBCQLExecutionService"
        })
@UseCase(
        description = "Insert records from a FlowFile into a Cassandra or ScyllaDB table.",
        keywords = {"cassandra", "scylladb", "cql", "insert", "record"},
        configuration = """
                Configure a Record Reader appropriate to the incoming data and point "Cassandra Connection Provider" at \
                the session provider service for the cluster.

                Set "Table name" to the target table, either as <keyspace>.<table> or as an unqualified <table> if the \
                connection service already names a keyspace.

                Leave "Statement Type" at INSERT. Each record field is matched to the column of the same name; use a \
                <keyspace>.<table>.<field> dynamic property to resolve a primary key column from somewhere else in the \
                record instead.

                "Batch size" controls how many records are grouped into a single batch statement. It is a throughput \
                and heap trade-off, not a correctness one - a FlowFile larger than the batch size is written across \
                several batches.
                """)
@UseCase(
        description = "Update existing rows in a Cassandra or ScyllaDB table from the records in a FlowFile.",
        keywords = {"cassandra", "scylladb", "cql", "update", "record"},
        notes = "Cassandra and ScyllaDB do not distinguish an update from an insert: an UPDATE against a primary key "
                + "that does not exist creates the row. Statement Type UPDATE differs from INSERT in that it writes "
                + "only the columns present in the record, rather than replacing the whole row.",
        configuration = """
                Set "Statement Type" to UPDATE and list the primary key columns in "Update Keys" as a comma-separated \
                list. Both are required together - an UPDATE with no update keys fails the FlowFile at runtime rather \
                than failing validation, because the statement type may itself come from a FlowFile attribute.

                Leave "Update Method" at SET to assign the record's values to the columns.

                "Time To Live" and "Timestamp Field" both apply to SET-method updates. Setting "Timestamp Field" to a \
                record field holding a stable timestamp makes reprocessing the same record a true no-op rather than a \
                write that races whatever else has touched the row since.
                """)
@UseCase(
        description = "Increment or decrement counter columns in a Cassandra or ScyllaDB counter table.",
        keywords = {"cassandra", "scylladb", "cql", "counter", "increment", "decrement"},
        notes = "Counter mutations are the one CQL write that is not idempotent: applying the same increment twice "
                + "counts twice. Leave Run Duration at 0 for counter flows, since a longer Run Duration allows the "
                + "framework to batch session commits, and a rolled-back batch is reprocessed from the queue. For the "
                + "same reason, prefer counter flows that can tolerate at-least-once delivery.",
        configuration = """
                Set "Statement Type" to UPDATE, list the counter table's primary key columns in "Update Keys", and set \
                "Update Method" to Increment or Decrement. Each record field matching a counter column supplies the \
                amount to add or subtract.

                Set "Batch Statement Type" to COUNTER, which is the type Cassandra and ScyllaDB require for counter \
                mutations. UNLOGGED is also accepted; LOGGED is rejected.

                Statement Type INSERT cannot be used against a counter table and is rejected.

                "Time To Live" and "Timestamp Field" are ignored here - neither is supported on counter columns.
                """)
public class PutCQLRecord extends AbstractCQLProcessor {
    static final String STATEMENT_TYPE_ATTRIBUTE = "cql.statement.type";

    static final String UPDATE_METHOD_ATTRIBUTE = "cql.update.method";

    static final String BATCH_STATEMENT_TYPE_ATTRIBUTE = "cql.batch.statement.type";

    static final String RECORDS_WRITTEN_ATTRIBUTE = "cql.records.written";

    static final PropertyDescriptor RECORD_READER_FACTORY = new PropertyDescriptor.Builder()
            .name("Record Reader")
            .description("""
                    Specifies the type of Record Reader controller service to use for parsing the incoming data \
                    and determining the schema""")
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
            .description("""
                    Specifies the method to use to SET the values. This property is used if the Statement Type is \
                    UPDATE and ignored otherwise.""")
            .required(false)
            .defaultValue(UpdateType.SET_TYPE.getValue())
            .allowableValues(UpdateType.class)
            .build();

    static final PropertyDescriptor UPDATE_KEYS = new PropertyDescriptor.Builder()
            .name("Update Keys")
            .description("""
                    A comma-separated list of column names that uniquely identifies a row in the database for UPDATE statements. \
                    If the Statement Type is UPDATE and this property is not set, the conversion to CQL will fail. \
                    This property is ignored if the Statement Type is not UPDATE.""")
            .addValidator(StandardValidators.createListValidator(true, false, StandardValidators.NON_EMPTY_VALIDATOR))
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Table name")
            .description("""
                    The name of the Cassandra table to which the records have to be written. This can be expressed \
                    as either a raw table name or a qualified table name (ex. <keyspace>.<tablename>). Due to the dynamic \
                    nature of this property, it will be validated at runtime by this processor and raise an error if \
                    it is neither <tablename> nor <keyspace>.<tablename> when the value is retrieved.""")
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
            .defaultValue(UNLOGGED_TYPE.getValue())
            .required(false)
            .build();

    static final PropertyDescriptor TTL = new PropertyDescriptor.Builder()
            .name("Time To Live")
            .description("""
                    Overrides the connection service's configured Default Time To Live (TTL) for records written by this processor. \
                    Applies to INSERT statements and to UPDATE statements using the SET method; ignored for Increment/Decrement updates, \
                    since Cassandra/ScyllaDB do not support a TTL on counter columns. If not set, the connection service's configured \
                    default (if any) is used.""")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor TIMESTAMP_FIELD = new PropertyDescriptor.Builder()
            .name("Timestamp Field")
            .description("""
                    The name of a field in each record whose value supplies the CQL write timestamp for that record's INSERT or \
                    SET-method UPDATE statement, instead of the time the statement executes. Useful for safe retries/reprocessing: \
                    resubmitting the same record with the same timestamp is a true no-op rather than a write that could win a \
                    last-write-wins race against different data written to the same row in the meantime. Ignored for Increment/Decrement \
                    updates, since Cassandra/ScyllaDB do not support a custom write timestamp on counter columns. If not set, the current \
                    time is used, as usual.""")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            CONNECTION_PROVIDER_SERVICE, TABLE, STATEMENT_TYPE, UPDATE_KEYS, UPDATE_METHOD,
            RECORD_READER_FACTORY, BATCH_SIZE, BATCH_STATEMENT_TYPE, TTL, TIMESTAMP_FIELD));

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE, REL_RETRY)));

    private static final Pattern QUALIFIED_TABLE_PATTERN = Pattern.compile(
            "^(?<keyspace>[a-zA-Z][a-zA-Z0-9_]{0,47})\\.(?<table>[a-zA-Z][a-zA-Z0-9_]{0,47})\\.(?<field>[a-zA-Z][a-zA-Z0-9_]{0,47})$");
    private static final Pattern TABLE_REGEX = Pattern.compile(
            "^(?:(?<keyspace>[a-zA-Z][a-zA-Z0-9_]{0,47})\\.)?(?<table>[a-zA-Z][a-zA-Z0-9_]{0,47})$");

    private RecordPathCache recordPathCache;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        Matcher matcher = QUALIFIED_TABLE_PATTERN.matcher(propertyDescriptorName);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(format("%s is not a valid qualified table name and field name", propertyDescriptorName));
        }

        String keyspace = matcher.group("keyspace");
        String table = matcher.group("table");
        String field = matcher.group("field");

        return new PropertyDescriptor.Builder()
                .dynamic(true)
                .name(propertyDescriptorName)
                .description("""
                    This property sets a record path to be evaluated for field %s on table %s.%s
                """.formatted(field, keyspace, table).trim())
                .defaultValue("")
                .addValidator(new RecordPathValidator())
                .build();
    }

    @OnScheduled
    @Override
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);

        this.recordPathCache = new RecordPathCache(50);
        this.createRecordPathOverrides(context);
    }

    private Map<PrimaryKeyIdentifier, RecordPath> recordPathOverrides;

    private void createRecordPathOverrides(ProcessContext context) {
        final Map<PrimaryKeyIdentifier, RecordPath> overrides = new HashMap<>();

        context
                .getProperties()
                .keySet()
                .stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(p -> {
                    String rawPath = context.getProperty(p).getValue();
                    RecordPath compiledPath = recordPathCache.getCompiled(rawPath);
                    Matcher matcher = QUALIFIED_TABLE_PATTERN.matcher(p.getName());
                    if (matcher.matches()) {
                        PrimaryKeyIdentifier identifier = new PrimaryKeyIdentifier(matcher.group("keyspace"),
                                matcher.group("table"), matcher.group("field"));
                        overrides.put(identifier, compiledPath);
                    }
                });
        recordPathOverrides = new ConcurrentHashMap<>(overrides);
    }

    private Map<PrimaryKeyIdentifier, RecordPath> resolveSpecificOverrides(QualifiedTableName tableName) {
        return new HashMap<>(recordPathOverrides
                .entrySet()
                .stream()
                .filter(e -> e.getKey().keyspace().equals(tableName.keyspace())
                        && e.getKey().tableName().equals(tableName.table()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
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

        CQLExecutionService sessionProviderService = super.cqlSessionService.get();

        final String cassandraTable = context.getProperty(TABLE).evaluateAttributeExpressions(inputFlowFile).getValue();
        final QualifiedTableName qualifiedTableName;

        try {
            qualifiedTableName = qualifyTable(cassandraTable, sessionProviderService.getDefaultKeyspace());
        } catch (IllegalArgumentException e) {
            getLogger().error("Error processing", e);
            // Nothing has been read yet, but cql.records.written is promised on every failure, so say 0 explicitly.
            session.transfer(session.putAttribute(inputFlowFile, RECORDS_WRITTEN_ATTRIBUTE, "0"), REL_FAILURE);
            return;
        }

        Map<PrimaryKeyIdentifier, RecordPath> specificOverrides = resolveSpecificOverrides(qualifiedTableName);

        final RecordReaderFactory recordParserFactory = context.getProperty(RECORD_READER_FACTORY).asControllerService(RecordReaderFactory.class);
        final int batchSize = context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger();
        final String updateKeys = context.getProperty(UPDATE_KEYS).evaluateAttributeExpressions(inputFlowFile).getValue();

        final String statementTypeProperty = context.getProperty(STATEMENT_TYPE).getValue();
        String statementType = statementTypeProperty;
        if (STATEMENT_TYPE_USE_ATTR_TYPE.getValue().equals(statementTypeProperty)) {
            statementType = inputFlowFile.getAttribute(STATEMENT_TYPE_ATTRIBUTE);
        }

        final String updateMethodProperty = context.getProperty(UPDATE_METHOD).getValue();
        String updateMethod = updateMethodProperty;
        if (UpdateType.UPDATE_METHOD_USE_ATTR_TYPE.getValue().equals(updateMethodProperty)) {
            final String updateMethodAttr = inputFlowFile.getAttribute(UPDATE_METHOD_ATTRIBUTE);
            updateMethod = StringUtils.isBlank(updateMethodAttr) ? null : updateMethodAttr.toUpperCase();
        }


        final String batchStatementTypeProperty = context.getProperty(BATCH_STATEMENT_TYPE).getValue();
        String batchStatementType = batchStatementTypeProperty;

        if (BATCH_STATEMENT_TYPE_USE_ATTR_TYPE.getValue().equals(batchStatementTypeProperty)) {
            final String batchStatementTypeAttr = inputFlowFile.getAttribute(BATCH_STATEMENT_TYPE_ATTRIBUTE);

            if (StringUtils.isBlank(batchStatementTypeAttr)) {
                getLogger().error("Batch Statement Type is not specified, FlowFile {}", inputFlowFile);
                session.penalize(inputFlowFile);
                session.transfer(inputFlowFile, REL_FAILURE);
                return;
            }

            batchStatementType = StringUtils.isBlank(batchStatementTypeAttr) ? null : batchStatementTypeAttr.toUpperCase();
        }

        final PropertyValue ttlProperty = context.getProperty(TTL).evaluateAttributeExpressions(inputFlowFile);
        final Duration ttlOverride = ttlProperty.isSet() ? ttlProperty.asDuration() : null;

        final PropertyValue timestampFieldProperty = context.getProperty(TIMESTAMP_FIELD).evaluateAttributeExpressions(inputFlowFile);
        final String timestampField = timestampFieldProperty.isSet() ? timestampFieldProperty.getValue() : null;

        final WriteOverrides writeOverrides = new WriteOverrides(ttlOverride, timestampField);

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

            if (UPDATE_TYPE.getValue().equalsIgnoreCase(statementType) && StringUtils.isBlank(updateMethod)) {
                throw new IllegalArgumentException(format("Update Method is not specified, FlowFile %s", inputFlowFile));
            }

            final List<String> updateKeyNames = UPDATE_TYPE.getValue().equalsIgnoreCase(statementType) ? Stream.of(updateKeys
                            .split(","))
                    .map(key -> key.trim())
                    .filter(key -> StringUtils.isNotEmpty(key))
                    .toList() : List.of();

            if (INCR_TYPE.getValue().equalsIgnoreCase(updateMethod) || DECR_TYPE.getValue().equalsIgnoreCase(updateMethod)) {
                if (!(UNLOGGED_TYPE.getValue().equalsIgnoreCase(batchStatementType) || COUNTER_TYPE.getValue().equalsIgnoreCase(batchStatementType))) {
                    throw new IllegalArgumentException(format("Increment/Decrement Update Method can only be used with COUNTER " +
                            "or UNLOGGED Batch Statement Type, FlowFile %s", inputFlowFile));
                }
            }

            // throw an exception if the statement type is INSERT and the batch statement type is COUNTER: Cassandra/ScyllaDB
            // do not allow INSERT statements against counter tables, only UPDATE
            if (INSERT_TYPE.getValue().equalsIgnoreCase(statementType) && COUNTER_TYPE.getValue().equalsIgnoreCase(batchStatementType)) {
                throw new IllegalArgumentException(format("Statement Type INSERT cannot be used with COUNTER Batch Statement Type, FlowFile %s", inputFlowFile));
            }

            // throw an exception if a Timestamp Field is configured but isn't actually a field in the incoming
            // records - checked unconditionally (regardless of whether the resolved statement/update method
            // would even use it) so a mistyped field name always fails the same way, rather than only
            // sometimes, depending on which attribute-driven statement type a given FlowFile happens to take.
            if (StringUtils.isNotEmpty(timestampField)) {
                final RecordSchema recordSchema = reader.getSchema();
                if (!recordSchema.getFieldNames().contains(timestampField)) {
                    throw new IllegalArgumentException(format("Timestamp Field '%s' is not present in the record schema, FlowFile %s", timestampField, inputFlowFile));
                }
            }

            Record record;
            List<Record> recordsBatch = new ArrayList<>();

            final boolean isUpdate = UPDATE_TYPE.getValue().equalsIgnoreCase(statementType);
            final UpdateMethod resolvedUpdateMethod = isUpdate ? UpdateMethod.valueOf(updateMethod) : null;
            final CqlBatchType resolvedBatchType = CqlBatchType.valueOf(batchStatementType);

            // Writes and clears the current batch, tracking how many records have been written so far so that
            // a mid-stream failure can report an accurate count instead of leaving it at 0, and emits a
            // provenance event per batch so a partial failure still leaves a trail of what was actually written.
            final Runnable flushBatch = () -> {
                final int batchRecordCount = recordsBatch.size();

                if (isUpdate) {
                    sessionProviderService.update(qualifiedTableName, recordsBatch, specificOverrides, updateKeyNames, resolvedUpdateMethod, resolvedBatchType, writeOverrides);
                } else {
                    sessionProviderService.insert(qualifiedTableName, recordsBatch, specificOverrides, resolvedBatchType, writeOverrides);
                }

                recordsAdded.addAndGet(batchRecordCount);
                session.getProvenanceReporter().send(inputFlowFile, sessionProviderService.getTransitUrl(qualifiedTableName),
                        format("Wrote %d records (%d total)", batchRecordCount, recordsAdded.get()), stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                recordsBatch.clear();
            };

            while ((record = reader.nextRecord()) != null) {
                recordsBatch.add(record);

                if (recordsBatch.size() == batchSize) {
                    flushBatch.run();
                }
            }

            if (!recordsBatch.isEmpty()) {
                flushBatch.run();
            }

        } catch (QueryFailureException qfe) {
            error = true;
            final FlowFile flowFileToRetry = session.putAttribute(inputFlowFile, RECORDS_WRITTEN_ATTRIBUTE, String.valueOf(recordsAdded.get()));
            getLogger().error("Cassandra query failure writing records for {}, {} records already written this attempt, routing to retry",
                    inputFlowFile, recordsAdded.get(), qfe);
            session.transfer(session.penalize(flowFileToRetry), REL_RETRY);
        } catch (IllegalArgumentException e) {
            error = true;
            // Also reachable mid-stream (an override RecordPath resolving to zero values on a later record),
            // so earlier batches may already be committed - report them, as the failure paths below do.
            final FlowFile failedFlowFile = session.putAttribute(inputFlowFile, RECORDS_WRITTEN_ATTRIBUTE, String.valueOf(recordsAdded.get()));
            getLogger().error("Invalid PutCQLRecord configuration for {}", inputFlowFile, e);
            session.transfer(failedFlowFile, REL_FAILURE);
        } catch (Exception e) {
            error = true;
            final FlowFile failedFlowFile = session.putAttribute(inputFlowFile, RECORDS_WRITTEN_ATTRIBUTE, String.valueOf(recordsAdded.get()));
            getLogger().error("Unable to write the records into Cassandra table, {} records already written this attempt", recordsAdded.get(), e);
            session.transfer(failedFlowFile, REL_FAILURE);
        } finally {
            if (!error) {
                session.transfer(inputFlowFile, REL_SUCCESS);
            }
        }

    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        Set<ValidationResult> results = new HashSet<>(super.customValidate(validationContext));

        String statementType = validationContext.getProperty(STATEMENT_TYPE).getValue();
        String batchStatementType = validationContext.getProperty(BATCH_STATEMENT_TYPE).getValue();

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
        } else if (INSERT_TYPE.getValue().equalsIgnoreCase(statementType)
                && COUNTER_TYPE.getValue().equalsIgnoreCase(batchStatementType)) {
            // Cassandra/ScyllaDB do not allow INSERT statements against counter tables, only UPDATE, so this
            // combination can never succeed (USE_ATTR_TYPE for either property cannot be checked at this point).
            results.add(new ValidationResult.Builder().subject("Batch Statement Type configuration").valid(false).explanation(
                    "if the Statement Type is set to INSERT, then the Batch Statement Type cannot be set to COUNTER, since "
                            + "Cassandra/ScyllaDB only allow COUNTER batches against UPDATE statements").build());
        }

        return results;
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

    private QualifiedTableName qualifyTable(String tableName, String providerDefault) {
        Matcher matcher = TABLE_REGEX.matcher(tableName);
        if (!matcher.matches()) {
            throw new IllegalArgumentException(format("%s is not a valid table name", tableName));
        }

        String keyspace = matcher.group("keyspace");
        String table = matcher.group("table");

        return new QualifiedTableName(StringUtils.isNotBlank(keyspace) ? keyspace : providerDefault, table);
    }

}
