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

package org.apache.nifi.processors.airtable;

import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_COUNT;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_ID;
import static org.apache.nifi.flowfile.attributes.FragmentAttributes.FRAGMENT_INDEX;
import static org.apache.nifi.processors.airtable.service.AirtableRestService.API_V0_BASE_URL;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSettings;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.airtable.record.AirtableRecordSet;
import org.apache.nifi.processors.airtable.record.AirtableJsonTreeRowRecordReaderFactory;
import org.apache.nifi.processors.airtable.service.AirtableGetRecordsParameters;
import org.apache.nifi.processors.airtable.service.AirtableRestService;
import org.apache.nifi.processors.airtable.service.AirtableRestService.RateLimitExceeded;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.JsonRecordReaderFactory;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

@PrimaryNodeOnly
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@TriggerSerially
@TriggerWhenEmpty
@Tags({"airtable", "query", "database"})
@CapabilityDescription("Query records from an Airtable table. Records are incrementally retrieved based on the last modified time of the records."
        + " Records can also be further filtered by setting the 'Custom Filter' property which supports the formulas provided by the Airtable API."
        + " Schema can be provided by setting up a JsonTreeReader controller service properly. This processor is intended to be run on the Primary Node only.")
@Stateful(scopes = Scope.CLUSTER, description = "The last successful query's time is stored in order to enable incremental loading."
        + " The initial query returns all the records in the table and each subsequent query filters the records by their last modified time."
        + " In other words, if a record is updated after the last successful query only the updated records will be returned in the next query."
        + " State is stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected,"
        + " the new node can pick up where the previous node left off, without duplicating the data.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the mime.type attribute to the MIME Type specified by the Record Writer."),
        @WritesAttribute(attribute = "record.count", description = "Sets the number of records in the FlowFile.")
})
@DefaultSettings(yieldDuration = "35 sec")
public class QueryAirtableTable extends AbstractProcessor {

    static final PropertyDescriptor API_URL = new PropertyDescriptor.Builder()
            .name("api-url")
            .displayName("API URL")
            .description("The URL for the Airtable REST API including the domain and the path to the API (e.g. https://api.airtable.com/v0).")
            .defaultValue(API_V0_BASE_URL)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(true)
            .build();

    static final PropertyDescriptor BASE_ID = new PropertyDescriptor.Builder()
            .name("base-id")
            .displayName("Base ID")
            .description("The ID of the Airtable base to be queried.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor TABLE_ID = new PropertyDescriptor.Builder()
            .name("table-id")
            .displayName("Table Name or ID")
            .description("The name or the ID of the Airtable table to be queried.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor API_TOKEN = new PropertyDescriptor.Builder()
            .name("api-token")
            .displayName("API Token")
            .description("The REST API token to use in queries. Should be generated on Airtable's account page.")
            .required(true)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor FIELDS = new PropertyDescriptor.Builder()
            .name("fields")
            .displayName("Fields")
            .description("Comma-separated list of fields to query from the table. Both the field's name and ID can be used.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CUSTOM_FILTER = new PropertyDescriptor.Builder()
            .name("custom-filter")
            .displayName("Custom Filter")
            .description("Filter records by Airtable's formulas.")
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor PAGE_SIZE = new PropertyDescriptor.Builder()
            .name("page-size")
            .displayName("Page Size")
            .description("Number of records to be fetched in a page. Should be between 0 and 100 inclusively.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.createLongValidator(0, 100, true))
            .build();

    static final PropertyDescriptor MAX_RECORDS_PER_FLOW_FILE = new PropertyDescriptor.Builder()
            .name("max-records-per-flow-file")
            .displayName("Max Records Per Flow File")
            .description("The maximum number of result records that will be included in a single FlowFile. This will allow you to break up very large"
                    + " result sets into multiple FlowFiles. If the value specified is zero, then all records are returned in a single FlowFile.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor OUTPUT_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("output-batch-size")
            .displayName("Output Batch Size")
            .description("The number of output FlowFiles to queue before committing the process session. When set to zero, the session will be committed when all records"
                    + " have been processed and the output FlowFiles are ready for transfer to the downstream relationship. For large result sets, this can cause a large burst of FlowFiles"
                    + " to be transferred at the end of processor execution. If this property is set, then when the specified number of FlowFiles are ready for transfer, then the session will"
                    + " be committed, thus releasing the FlowFiles to the downstream relationship.")
            .defaultValue("0")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_NEGATIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor METADATA_STRATEGY = new PropertyDescriptor.Builder()
            .name("metadata-strategy")
            .displayName("Metadata Strategy")
            .description("Strategy to use for fetching record schema. Currently only 'Use JSON Record Reader' is supported."
                    + " When Airtable Metadata API becomes more stable it will be possible to fetch the record schema through it.")
            .required(true)
            .defaultValue(MetadataStrategy.USE_JSON_RECORD_READER.name())
            .allowableValues(MetadataStrategy.class)
            .build();

    static final PropertyDescriptor SCHEMA_READER = new PropertyDescriptor.Builder()
            .name("schema-reader")
            .displayName("Schema Reader")
            .description("JsonTreeReader service to use for fetching the schema of records returned by the Airtable REST API")
            .dependsOn(METADATA_STRATEGY, MetadataStrategy.USE_JSON_RECORD_READER.name())
            .identifiesControllerService(JsonRecordReaderFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor RECORD_WRITER = new PropertyDescriptor.Builder()
            .name("record-writer")
            .displayName("Record Writer")
            .description("Service used for writing records returned by the Airtable REST API")
            .identifiesControllerService(RecordSetWriterFactory.class)
            .required(true)
            .build();

    static final PropertyDescriptor WEB_CLIENT_SERVICE_PROVIDER = new PropertyDescriptor.Builder()
            .name("web-client-service-provider")
            .displayName("Web Client Service Provider")
            .description("Web Client Service Provider to use for Airtable REST API requests")
            .identifiesControllerService(WebClientServiceProvider.class)
            .required(true)
            .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("For FlowFiles created as a result of a successful query.")
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            API_URL,
            BASE_ID,
            TABLE_ID,
            API_TOKEN,
            FIELDS,
            CUSTOM_FILTER,
            PAGE_SIZE,
            MAX_RECORDS_PER_FLOW_FILE,
            OUTPUT_BATCH_SIZE,
            METADATA_STRATEGY,
            SCHEMA_READER,
            RECORD_WRITER,
            WEB_CLIENT_SERVICE_PROVIDER
    ));

    private static final Set<Relationship> RELATIONSHIPS = Collections.singleton(REL_SUCCESS);

    private static final String LAST_RECORD_FETCH_TIME = "last_record_fetch_time";
    private static final int QUERY_LAG_SECONDS = 1;

    private volatile AirtableRestService airtableRestService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        final String apiUrl = context.getProperty(API_URL).evaluateAttributeExpressions().getValue();
        final String apiToken = context.getProperty(API_TOKEN).evaluateAttributeExpressions().getValue();
        final String baseId = context.getProperty(BASE_ID).evaluateAttributeExpressions().getValue();
        final String tableId = context.getProperty(TABLE_ID).evaluateAttributeExpressions().getValue();
        final WebClientServiceProvider webClientServiceProvider = context.getProperty(WEB_CLIENT_SERVICE_PROVIDER).asControllerService(WebClientServiceProvider.class);
        airtableRestService = new AirtableRestService(webClientServiceProvider.getWebClientService(), apiUrl, apiToken, baseId, tableId);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final JsonRecordReaderFactory schemaRecordReaderFactory = context.getProperty(SCHEMA_READER).asControllerService(JsonRecordReaderFactory.class);
        final RecordSetWriterFactory writerFactory = context.getProperty(RECORD_WRITER).asControllerService(RecordSetWriterFactory.class);
        final Integer maxRecordsPerFlowFile = context.getProperty(MAX_RECORDS_PER_FLOW_FILE).evaluateAttributeExpressions().asInteger();
        final Integer outputBatchSize = context.getProperty(OUTPUT_BATCH_SIZE).evaluateAttributeExpressions().asInteger();

        final StateMap state;
        try {
            state = context.getStateManager().getState(Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to get cluster state", e);
        }

        final String lastRecordFetchDateTime = state.get(LAST_RECORD_FETCH_TIME);
        final String currentRecordFetchDateTime = OffsetDateTime.now().minusSeconds(QUERY_LAG_SECONDS).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        final AirtableGetRecordsParameters getRecordsParameters = buildGetRecordsParameters(context, lastRecordFetchDateTime, currentRecordFetchDateTime);
        final byte[] recordsJson;
        try {
            recordsJson = airtableRestService.getRecords(getRecordsParameters);
        } catch (RateLimitExceeded e) {
            context.yield();
            throw new ProcessException(e);
        }

        final FlowFile flowFile = session.create();
        final Map<String, String> originalAttributes = flowFile.getAttributes();

        final RecordSchema recordSchema;
        final RecordSchema writerSchema;
        try {
            final ByteArrayInputStream recordsStream = new ByteArrayInputStream(recordsJson);
            recordSchema = schemaRecordReaderFactory.createRecordReader(flowFile, recordsStream, getLogger()).getSchema();
            writerSchema = writerFactory.getSchema(originalAttributes, recordSchema);
        } catch (MalformedRecordException | IOException | SchemaNotFoundException e) {
            throw new ProcessException("Couldn't get record schema", e);
        }

        session.remove(flowFile);

        final List<FlowFile> flowFiles = new ArrayList<>();
        int totalRecordCount = 0;
        final AirtableJsonTreeRowRecordReaderFactory recordReaderFactory = new AirtableJsonTreeRowRecordReaderFactory(getLogger(), recordSchema);
        try (final AirtableRecordSet airtableRecordSet = new AirtableRecordSet(recordsJson, recordReaderFactory, airtableRestService, getRecordsParameters)) {
            while (true) {
                final AtomicInteger recordCountHolder = new AtomicInteger();
                final Map<String, String> attributes = new HashMap<>();
                FlowFile flowFileToAdd = session.create();
                flowFileToAdd = session.write(flowFileToAdd, out -> {
                    try (final RecordSetWriter writer = writerFactory.createWriter(getLogger(), writerSchema, out, originalAttributes)) {
                        final RecordSet recordSet = (maxRecordsPerFlowFile > 0 ? airtableRecordSet.limit(maxRecordsPerFlowFile) : airtableRecordSet);
                        final WriteResult writeResult = writer.write(recordSet);

                        attributes.put("record.count", String.valueOf(writeResult.getRecordCount()));
                        attributes.put(CoreAttributes.MIME_TYPE.key(), writer.getMimeType());
                        attributes.putAll(writeResult.getAttributes());

                        recordCountHolder.set(writeResult.getRecordCount());
                    } catch (final IOException e) {
                        final Throwable cause = e.getCause();
                        if (cause instanceof RateLimitExceeded) {
                            context.yield();
                            throw new ProcessException("Couldn't read records from input", cause);
                        }
                        throw new ProcessException(e);
                    } catch (final SchemaNotFoundException e) {
                        throw new ProcessException("Couldn't read records from input", e);
                    }
                });

                flowFileToAdd = session.putAllAttributes(flowFileToAdd, attributes);

                final int recordCount = recordCountHolder.get();
                if (recordCount > 0) {
                    totalRecordCount += recordCount;
                    flowFiles.add(flowFileToAdd);
                    if (outputBatchSize > 0 && flowFiles.size() == outputBatchSize) {
                        transferFlowFiles(flowFiles, session, totalRecordCount);
                        flowFiles.clear();
                    }
                    continue;
                }

                session.remove(flowFileToAdd);

                if (maxRecordsPerFlowFile > 0 && outputBatchSize == 0) {
                    fragmentFlowFiles(session, flowFiles);
                }

                transferFlowFiles(flowFiles, session, totalRecordCount);
                break;
            }
        } catch (final IOException e) {
            throw new ProcessException("Couldn't read records from input", e);
        }

        if (totalRecordCount == 0) {
            return;
        }

        final Map<String, String> newState = new HashMap<>(state.toMap());
        newState.put(LAST_RECORD_FETCH_TIME, currentRecordFetchDateTime);
        try {
            context.getStateManager().setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException("Failed to update cluster state", e);
        }
    }

    private AirtableGetRecordsParameters buildGetRecordsParameters(final ProcessContext context,
            final String lastRecordFetchTime,
            final String nowDateTimeString) {
        final String fieldsProperty = context.getProperty(FIELDS).evaluateAttributeExpressions().getValue();
        final String customFilter = context.getProperty(CUSTOM_FILTER).evaluateAttributeExpressions().getValue();
        final Integer pageSize = context.getProperty(PAGE_SIZE).evaluateAttributeExpressions().asInteger();

        final AirtableGetRecordsParameters.Builder getRecordsParametersBuilder = new AirtableGetRecordsParameters.Builder();
        if (lastRecordFetchTime != null) {
            getRecordsParametersBuilder
                    .modifiedAfter(lastRecordFetchTime)
                    .modifiedBefore(nowDateTimeString);
        }
        if (fieldsProperty != null) {
            getRecordsParametersBuilder.fields(Arrays.stream(fieldsProperty.split(",")).map(String::trim).collect(Collectors.toList()));
        }
        getRecordsParametersBuilder.filterByFormula(customFilter);
        if (pageSize != null && pageSize > 0) {
            getRecordsParametersBuilder.pageSize(pageSize);
        }

        return getRecordsParametersBuilder.build();
    }

    private void fragmentFlowFiles(final ProcessSession session, final List<FlowFile> flowFiles) {
        final String fragmentIdentifier = UUID.randomUUID().toString();
        for (int i = 0; i < flowFiles.size(); i++) {
            final Map<String, String> fragmentAttributes = new HashMap<>();
            fragmentAttributes.put(FRAGMENT_ID.key(), fragmentIdentifier);
            fragmentAttributes.put(FRAGMENT_INDEX.key(), String.valueOf(i));
            fragmentAttributes.put(FRAGMENT_COUNT.key(), String.valueOf(flowFiles.size()));

            flowFiles.set(i, session.putAllAttributes(flowFiles.get(i), fragmentAttributes));
        }
    }

    private void transferFlowFiles(final List<FlowFile> flowFiles, final ProcessSession session, final int totalRecordCount) {
        session.transfer(flowFiles, REL_SUCCESS);
        session.adjustCounter("Records Processed", totalRecordCount, false);
        final String flowFilesAsString = flowFiles.stream().map(FlowFile::toString).collect(Collectors.joining(", ", "[", "]"));
        getLogger().info("Successfully written {} records for flow files {}", totalRecordCount, flowFilesAsString);
    }
}
