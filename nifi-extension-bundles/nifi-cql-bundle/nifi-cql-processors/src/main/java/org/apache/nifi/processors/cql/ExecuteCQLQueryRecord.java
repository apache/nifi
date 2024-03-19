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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.service.cql.api.exception.QueryFailureException;
import org.apache.nifi.service.cql.api.service.CQLExecutionService;
import org.apache.nifi.service.cql.api.service.QueryOverrides;
import org.apache.nifi.util.StopWatch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Tags({"cassandra", "scylladb", "cql", "select"})
@InputRequirement(InputRequirement.Requirement.INPUT_ALLOWED)
@CapabilityDescription("Execute provided Cassandra Query Language (CQL) select query on a data store that supports CQL (Cassandra or ScyllaDB primarily). Using a" +
        " configured record writer service, it will convert result rows into any output format supported by NiFi's record API.")
@WritesAttributes({
        @WritesAttribute(attribute = "fragment.identifier", description = "If 'Max Rows Per Flow File' is set then all FlowFiles from the same query result set "
                + "will have the same value for the fragment.identifier attribute. This can then be used to correlate the results."),
        @WritesAttribute(attribute = "fragment.count", description = "If 'Max Rows Per Flow File' is set then this is the total number of  "
                + "FlowFiles produced by a single ResultSet. This can be used in conjunction with the "
                + "fragment.identifier attribute in order to know how many FlowFiles belonged to the same incoming ResultSet. If Output Batch Size is set, then this "
                + "attribute will not be populated."),
        @WritesAttribute(attribute = "fragment.index", description = "If 'Max Rows Per Flow File' is set then the position of this FlowFile in the list of "
                + "outgoing FlowFiles that were all derived from the same result set FlowFile. This can be "
                + "used in conjunction with the fragment.identifier attribute to know which FlowFiles originated from the same query result set and in what order  "
                + "FlowFiles were produced")
})
@DynamicProperty(name = "cql.arg.<position>", value = "The value to bind to that bind marker",
        expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
        description = "Supplies the value for one '?' bind marker in the CQL select query, where <position> is the marker's "
                + "1-based position in the query - cql.arg.1 for the first, cql.arg.2 for the second, and so on. Positions "
                + "must run consecutively from 1, and the number of parameters must match the number of bind markers in the "
                + "query. Each value is sent to the cluster as data and is never parsed as CQL, so this is the safe way to "
                + "build a query around a value taken from a FlowFile attribute. See 'Additional Details'.")
@SystemResourceConsideration(resource = SystemResource.MEMORY,
        description = "With the default 'Max Rows Per Flow File' of 0, an entire result set is written to a single "
                + "FlowFile; with the default 'Output Batch Size' of 0, every output FlowFile is held in the session "
                + "until the whole result set has been read. Set both when querying large tables.")
@SeeAlso(
        value = {PutCQLRecord.class},
        // The session provider services cannot be referenced by class: this module is barred from depending on either
        // of them - and so on the database drivers they carry - by the ban-database-client-dependencies enforcer rule.
        classNames = {
                "org.apache.nifi.service.cassandra.CassandraCQLExecutionService",
                "org.apache.nifi.service.scylladb.ScyllaDBCQLExecutionService"
        })
@UseCase(
        description = "Run a fixed CQL query on a schedule and emit the results as records.",
        inputRequirement = InputRequirement.Requirement.INPUT_FORBIDDEN,
        keywords = {"cassandra", "scylladb", "cql", "select", "query", "source"},
        notes = "A scheduled processor runs on every node of a NiFi cluster, so the query is executed once per node and "
                + "each node emits its own copy of the result. Set the processor's Execution to 'Primary node only' if "
                + "a single copy is wanted. Note also that every run re-executes the whole query: this processor keeps "
                + "no state, so there is no built-in way to fetch only rows that are new since the last run.",
        configuration = """
                Give the processor no incoming connection and schedule it on a timer.

                Set "CQL select query" to the query to run and "Result Set Output Writer" to a record writer for the \
                desired output format.

                Set "Max Rows Per Flow File" to split a large result set across several FlowFiles, and "Output Batch \
                Size" to release those FlowFiles downstream as the result set is read rather than all at once when it \
                completes.
                """)
@UseCase(
        description = "Query a table using values taken from an incoming FlowFile, without exposing the query to CQL injection.",
        inputRequirement = InputRequirement.Requirement.INPUT_REQUIRED,
        keywords = {"cassandra", "scylladb", "cql", "select", "query", "parameter", "bind"},
        notes = "Anything interpolated into the query text with Expression Language is parsed as CQL, so a query built "
                + "that way from FlowFile attributes is injectable. Bind markers are not: each cql.arg.<position> value "
                + "is sent to the cluster as data. Prefer bind markers whenever a value originates outside the flow's "
                + "own configuration.",
        configuration = """
                Write the query with '?' bind markers in place of the values, for example: \
                SELECT * FROM my_keyspace.events WHERE id = ?

                Add one dynamic property per marker, named cql.arg.1, cql.arg.2 and so on in the order the markers \
                appear, with each value supplied by Expression Language against the FlowFile's attributes. The \
                positions must run consecutively from 1, and the count must match the number of markers, or the \
                processor is invalid.

                The incoming FlowFile is routed to 'original' once the query completes; result records leave via \
                'success'.
                """)
public class ExecuteCQLQueryRecord extends AbstractCQLProcessor {

    /**
     * Matches the dynamic property name for a positional query parameter - {@code cql.arg.1}, {@code cql.arg.2},
     * and so on - capturing the 1-based position of the bind marker the property supplies a value for.
     */
    private static final Pattern QUERY_PARAMETER_PATTERN = Pattern.compile("^cql\\.arg\\.(?<position>[1-9]\\d*)$");

    public static final PropertyDescriptor CQL_SELECT_QUERY = new PropertyDescriptor.Builder()
            .name("CQL select query")
            .description("CQL select query. Values that come from outside the flow's own configuration - a FlowFile "
                    + "attribute, for example - should be supplied as '?' bind markers with matching cql.arg.<position> "
                    + "dynamic properties rather than interpolated into this query text, since anything interpolated here "
                    + "is parsed as CQL.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for this query to run, overriding the Read Timeout configured on the "
                    + "connection service for this query only. Must be of format <duration> <TimeUnit> where <duration> is a "
                    + "non-negative integer and TimeUnit is a supported Time Unit, such as: nanos, millis, secs, mins, hrs, days. "
                    + "If not set, the connection service's configured Read Timeout is used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    public static final PropertyDescriptor FETCH_SIZE = new PropertyDescriptor.Builder()
            .name("Fetch Size")
            .description("The number of result rows to be fetched from the result set at a time, overriding the Fetch Size "
                    + "configured on the connection service for this query only. If not set, the connection service's "
                    + "configured Fetch Size is used.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor MAX_ROWS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("Max Rows Per Flow File")
            .description("The maximum number of result rows that will be included in a single FlowFile. This will allow you to break up very large "
                    + "result sets into multiple FlowFiles. If the value specified is zero, then all rows are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    public static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all result set rows "
                    + "have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles "
                    + "to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will "
                    + "be committed, thus releasing the FlowFiles to the downstream relationship. NOTE: The maxvalue.* and fragment.count attributes will not be set on FlowFiles when this "
                    + "property is set.")
            .defaultValue("0")
            .required(true)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .build();

    public static final PropertyDescriptor OUTPUT_WRITER = new PropertyDescriptor.Builder()
            .name("Result Set Output Writer")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .description("The controller service to use for writing the results to a flowfile")
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .autoTerminateDefault(true)
            .name("original")
            .description("The incoming FlowFile that triggered the query is routed here once every resulting "
                    + "FlowFile has been transferred to success, or immediately if the query returned no rows. "
                    + "On a failed query the incoming FlowFile goes to failure or retry instead, never here.")
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            CONNECTION_PROVIDER_SERVICE,
            OUTPUT_WRITER,
            CQL_SELECT_QUERY,
            FETCH_SIZE,
            QUERY_TIMEOUT,
            MAX_ROWS_PER_FLOW_FILE,
            OUTPUT_BATCH_SIZE
    );

    public static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS, REL_ORIGINAL, REL_FAILURE, REL_RETRY);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        final Matcher matcher = QUERY_PARAMETER_PATTERN.matcher(propertyDescriptorName);

        if (!matcher.matches()) {
            throw new IllegalArgumentException(String.format(
                    "'%s' is not a valid query parameter name; positional parameters are named cql.arg.1, cql.arg.2, and so on",
                    propertyDescriptorName));
        }

        return new PropertyDescriptor.Builder()
                .dynamic(true)
                .name(propertyDescriptorName)
                .description(String.format("The value bound to bind marker %s of the CQL select query.", matcher.group("position")))
                .required(false)
                .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                .addValidator(StandardValidators.NON_EMPTY_EL_VALIDATOR)
                .build();
    }

    /**
     * Rejects a gap or a non-1 start in the positional parameter numbering. The parameters are bound by
     * position, so {@code cql.arg.1} plus {@code cql.arg.3} is ambiguous rather than merely unusual - it would
     * otherwise silently bind the second marker with the third parameter's value.
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<Integer> positions = getParameterPositions(context.getProperties().keySet());

        for (int index = 0; index < positions.size(); index++) {
            final int expected = index + 1;

            if (positions.get(index) != expected) {
                return List.of(new ValidationResult.Builder()
                        .subject("Query parameters")
                        .valid(false)
                        .explanation(String.format(
                                "positional query parameters must be numbered consecutively starting at 1, but cql.arg.%d is missing",
                                expected))
                        .build());
            }
        }

        return List.of();
    }

    private static List<Integer> getParameterPositions(final Collection<PropertyDescriptor> descriptors) {
        return descriptors.stream()
                .filter(PropertyDescriptor::isDynamic)
                .map(descriptor -> QUERY_PARAMETER_PATTERN.matcher(descriptor.getName()))
                .filter(Matcher::matches)
                .map(matcher -> Integer.valueOf(matcher.group("position")))
                .sorted()
                .toList();
    }

    /**
     * Collects the {@code cql.arg.<position>} dynamic properties into the positional order the bind markers
     * expect, evaluating each against {@code flowFile}'s attributes. Numbering is already known to be
     * consecutive from 1 by {@link #customValidate(ValidationContext)}, so sorting by position is enough.
     */
    private List<Object> getQueryParameters(final ProcessContext context, final FlowFile flowFile) {
        final SortedMap<Integer, Object> parametersByPosition = new TreeMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }

            final Matcher matcher = QUERY_PARAMETER_PATTERN.matcher(descriptor.getName());

            if (matcher.matches()) {
                parametersByPosition.put(Integer.valueOf(matcher.group("position")),
                        context.getProperty(descriptor).evaluateAttributeExpressions(flowFile).getValue());
            }
        }

        return new ArrayList<>(parametersByPosition.values());
    }

    @OnScheduled
    @Override
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile fileToProcess = null;

        if (context.hasIncomingConnection()) {
            fileToProcess = session.get();

            // If we have no FlowFile, and all incoming connections are self-loops then we can continue on.
            // However, if we have no FlowFile and we have connections coming from other Processors, then
            // we know that we should run only if we have a FlowFile.
            if (fileToProcess == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final ComponentLog logger = getLogger();
        final String selectQuery = context.getProperty(CQL_SELECT_QUERY).evaluateAttributeExpressions(fileToProcess).getValue();
        final long maxRowsPerFlowFile = context.getProperty(MAX_ROWS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final long outputBatchSize = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final PropertyValue fetchSizeProperty = context.getProperty(FETCH_SIZE).evaluateAttributeExpressions(fileToProcess);
        final Integer fetchSizeOverride = fetchSizeProperty.isSet() ? fetchSizeProperty.asInteger() : null;

        final PropertyValue queryTimeoutProperty = context.getProperty(QUERY_TIMEOUT).evaluateAttributeExpressions(fileToProcess);
        final Duration queryTimeoutOverride = queryTimeoutProperty.isSet() ? queryTimeoutProperty.asDuration() : null;

        final QueryOverrides queryOverrides = new QueryOverrides(fetchSizeOverride, queryTimeoutOverride);
        final List<Object> queryParameters = getQueryParameters(context, fileToProcess);

        final StopWatch stopWatch = new StopWatch(true);

        final RecordSetWriterFactory writerFactory = context.getProperty(OUTPUT_WRITER).asControllerService(RecordSetWriterFactory.class);
        final CQLExecutionService cqlExecutionService = context.getProperty(CONNECTION_PROVIDER_SERVICE)
                .asControllerService(CQLExecutionService.class);

        final ExecuteCQLQueryCallback callback = new ExecuteCQLQueryCallback(fileToProcess, writerFactory, session,
                getLogger(), maxRowsPerFlowFile, outputBatchSize);

        try {
            stopWatch.start();

            cqlExecutionService.query(selectQuery, queryParameters, callback, queryOverrides);

            if (callback.isEmpty() && fileToProcess != null) {
                session.transfer(fileToProcess, REL_ORIGINAL);
            }

            stopWatch.stop();

            getLogger().debug("The query took {} seconds.", stopWatch.getDuration(TimeUnit.SECONDS));
        } catch (final QueryFailureException qee) {
            //The logger is called in the client service
            if (context.hasIncomingConnection()) {
                if (fileToProcess == null || callback.hasSentOriginal()) {
                    fileToProcess = session.create();
                }
                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_RETRY);
            } else {
                context.yield();
            }
        } catch (final ProcessException e) {
            if (context.hasIncomingConnection()) {
                logger.error(String.format("Unable to execute CQL select query %s for %s routing to failure",
                        selectQuery, fileToProcess), e);
                if (fileToProcess == null || callback.hasSentOriginal()) {
                    fileToProcess = session.create();
                }

                fileToProcess = session.penalize(fileToProcess);
                session.transfer(fileToProcess, REL_FAILURE);

            } else {
                logger.error(String.format("Unable to execute CQL select query %s",
                        selectQuery), e);
                context.yield();
            }
        }
        session.commitAsync();
    }
}
