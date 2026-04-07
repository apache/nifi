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

package org.apache.nifi.processors.elasticsearch;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.elasticsearch.ElasticSearchClientService;
import org.apache.nifi.elasticsearch.ElasticsearchException;
import org.apache.nifi.elasticsearch.ElasticsearchRequestOptions;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.migration.RelationshipConfiguration;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({"ndjson", "json", "json array", "elasticsearch", "elasticsearch7", "elasticsearch8", "elasticsearch9", "put", "index"})
@CapabilityDescription("An Elasticsearch put processor that uses the official Elastic REST client libraries to write JSON data. " +
        "Supports three input formats selectable via the \"Input Content Format\" property: " +
        "NDJSON (one JSON object per line), JSON Array (a top-level array of objects, streamed for memory efficiency), " +
        "and Single JSON (the entire FlowFile is one document). " +
        "FlowFiles are accumulated up to the configured Max Batch Size and flushed to Elasticsearch in _bulk API requests. " +
        "Large files that exceed the batch size are automatically split into multiple _bulk requests.")
@WritesAttributes({
        @WritesAttribute(attribute = "elasticsearch.put.error",
                description = "The error message if there is an issue parsing the FlowFile, sending the parsed document to Elasticsearch or parsing the Elasticsearch response"),
        @WritesAttribute(attribute = "elasticsearch.bulk.error", description = "The _bulk response if there was an error during processing the document within Elasticsearch.")
})
@SeeAlso(PutElasticsearchRecord.class)
@DynamicProperties({
        @DynamicProperty(
                name = "The name of the Bulk request header",
                value = "The value of the Bulk request header",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Prefix: " + AbstractPutElasticsearch.BULK_HEADER_PREFIX +
                        " - adds the specified property name/value as a Bulk request header in the Elasticsearch Bulk API body used for processing. " +
                        "If the value is null or blank, the Bulk header will be omitted for the document operation. " +
                        "These parameters will override any matching parameters in the _bulk request body."),
        @DynamicProperty(
                name = "The name of the HTTP request header",
                value = "A Record Path expression to retrieve the HTTP request header value",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Prefix: " + ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER +
                        " - adds the specified property name/value as a HTTP request header in the Elasticsearch request. " +
                        "If the Record Path expression results in a null or blank value, the HTTP request header will be omitted. " +
                        "If FlowFiles are batched, only the first FlowFile in the batch is used to evaluate property values."),
        @DynamicProperty(
                name = "The name of a URL query parameter to add",
                value = "The value of the URL query parameter",
                expressionLanguageScope = ExpressionLanguageScope.FLOWFILE_ATTRIBUTES,
                description = "Adds the specified property name/value as a query parameter in the Elasticsearch URL used for processing. " +
                        "These parameters will override any matching parameters in the _bulk request body. " +
                        "If FlowFiles are batched, only the first FlowFile in the batch is used to evaluate property values.")
})
@SystemResourceConsideration(
        resource = SystemResource.MEMORY,
        description = "At most one batch's worth of data will be buffered in memory at a time. " +
                "The maximum memory usage is bounded by the Max Batch Size property.")
public class PutElasticsearchJson extends AbstractPutElasticsearch {

    static final PropertyDescriptor SCRIPT = new PropertyDescriptor.Builder()
            .name("Script")
            .description("The script for the document update/upsert. Only applies to Update/Upsert operations. " +
                    "Must be parsable as JSON Object. If left blank, the FlowFile content will be used for document update/upsert")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor SCRIPTED_UPSERT = new PropertyDescriptor.Builder()
            .name("Scripted Upsert")
            .description("Whether to add the scripted_upsert flag to the Upsert operation. " +
                    "If true, forces Elasticsearch to execute the Script whether or not the document exists, defaults to false. " +
                    "If the upsert document will be empty, ensure null suppression is disabled so that an empty \"upsert\" doc " +
                    "is included in the request — otherwise Elasticsearch will not create a new document for the script to " +
                    "execute against, resulting in a \"not_found\" error. " +
                    "For Single JSON mode, set the " + CLIENT_SERVICE.getDisplayName() + " controller service's " +
                    ElasticSearchClientService.SUPPRESS_NULLS.getDisplayName() + " to " + ElasticSearchClientService.NEVER_SUPPRESS.getDisplayName() + ". " +
                    "For NDJSON and JSON Array modes, set the Suppress Nulls property on this processor to Never Suppress.")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .defaultValue("false")
            .build();

    static final PropertyDescriptor DYNAMIC_TEMPLATES = new PropertyDescriptor.Builder()
            .name("Dynamic Templates")
            .description("The dynamic_templates for the document. Must be parsable as a JSON Object. Requires Elasticsearch 7+")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
        .name("Character Set")
        .description("Specifies the character set of the document data.")
        .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(StandardCharsets.UTF_8.name())
        .required(true)
        .build();

    static final PropertyDescriptor MAX_BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Max Batch Size")
            .description("""
                    The maximum amount of data to send in a single Elasticsearch _bulk API request. \
                    For NDJSON and JSON Array modes, FlowFiles are accumulated until this threshold is reached, then flushed. \
                    For Single JSON mode, this acts as a size-based safety limit: if the accumulated FlowFiles exceed this size \
                    before Max FlowFiles Per Batch is reached, the request is flushed early. \
                    Elasticsearch recommends 5-15 MB per bulk request for optimal performance. \
                    To disable the size-based limit, check "Set Empty String".\
                    """)
            .defaultValue("10 MB")
            .addValidator((subject, input, context) -> StringUtils.isBlank(input)
                    ? new ValidationResult.Builder().subject(subject).valid(true).build()
                    : StandardValidators.DATA_SIZE_VALIDATOR.validate(subject, input, context))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .required(false)
            .build();

    static final PropertyDescriptor INPUT_FORMAT = new PropertyDescriptor.Builder()
            .name("Input Content Format")
            .description("""
                    The format of the JSON content in each FlowFile. \
                    NDJSON: one JSON object per line (newline-delimited). \
                    JSON Array: a top-level JSON array of objects, streamed element-by-element for memory efficiency. \
                    Single JSON: the entire FlowFile is a single JSON document.\
                    """)
            .allowableValues(InputFormat.class)
            .defaultValue(InputFormat.SINGLE_JSON.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .fromPropertyDescriptor(AbstractPutElasticsearch.BATCH_SIZE)
            .name("Max FlowFiles Per Batch")
            .displayName("Max FlowFiles Per Batch")
            .description("""
                    The maximum number of FlowFiles to include in a single Elasticsearch _bulk API request. \
                    If the accumulated FlowFiles exceed Max Batch Size before this count is reached, the request will be flushed early.\
                    """)
            .dependsOn(INPUT_FORMAT, InputFormat.SINGLE_JSON)
            .build();

    static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
            .name("Suppress Nulls")
            .description("""
                    When set to Always Suppress, null and empty values are removed from documents before they are sent to Elasticsearch.
                    This setting applies to NDJSON and JSON Array formats for Index and Create operations only. \
                    For Single JSON, configure null suppression on the controller service instead.
                    Performance note: for JSON Array the impact is negligible since documents are already being parsed. \
                    For NDJSON, each line must be parsed and re-serialized when suppression is enabled, \
                    which adds overhead compared to the default behaviour of passing lines through as raw bytes.\
                    """)
            .allowableValues(ElasticSearchClientService.NEVER_SUPPRESS, ElasticSearchClientService.ALWAYS_SUPPRESS)
            .defaultValue(ElasticSearchClientService.NEVER_SUPPRESS)
            .required(true)
            .dependsOn(INPUT_FORMAT, InputFormat.NDJSON, InputFormat.JSON_ARRAY)
            .build();

    static final PropertyDescriptor ID_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("Identifier Attribute")
            .description("""
                    The name of the FlowFile attribute containing the identifier for the document. \
                    If the Index Operation is "index", this property may be left empty or evaluate to an empty value, \
                    in which case the document's identifier will be auto-generated by Elasticsearch. \
                    For all other Index Operations, the attribute must evaluate to a non-empty value.\
                    """)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .dependsOn(INPUT_FORMAT, InputFormat.SINGLE_JSON)
            .build();

    static final PropertyDescriptor IDENTIFIER_FIELD = new PropertyDescriptor.Builder()
            .name("Identifier Field")
            .description("""
                    The name of the field within each document to use as the Elasticsearch document ID. \
                    If the field is not present in a document or this property is left blank, no document ID is set \
                    and Elasticsearch will auto-generate one.\
                    """)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .dependsOn(INPUT_FORMAT, InputFormat.NDJSON, InputFormat.JSON_ARRAY)
            .build();

    static final Relationship REL_BULK_REQUEST = new Relationship.Builder()
            .name("bulk_request")
            .description("When \"Output Bulk Request\" is enabled, the raw Elasticsearch _bulk API request body is written " +
                    "to this relationship as a FlowFile for inspection or debugging.")
            .build();

    static final PropertyDescriptor OUTPUT_BULK_REQUEST = new PropertyDescriptor.Builder()
            .name("Output Bulk Request")
            .description("If enabled, each Elasticsearch _bulk request body is written as a FlowFile to the \"" +
                    REL_BULK_REQUEST.getName() + "\" relationship. Useful for debugging. " +
                    "Each FlowFile contains the full NDJSON body exactly as sent to Elasticsearch.")
            .allowableValues("true", "false")
            .defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    static final List<PropertyDescriptor> DESCRIPTORS = List.of(
            INDEX_OP,
            INDEX,
            TYPE,
            SCRIPT,
            SCRIPTED_UPSERT,
            DYNAMIC_TEMPLATES,
            INPUT_FORMAT,
            BATCH_SIZE,
            MAX_BATCH_SIZE,
            SUPPRESS_NULLS,
            ID_ATTRIBUTE,
            IDENTIFIER_FIELD,
            CHARSET,
            MAX_JSON_FIELD_STRING_LENGTH,
            CLIENT_SERVICE,
            LOG_ERROR_RESPONSES,
            OUTPUT_ERROR_RESPONSES,
            OUTPUT_BULK_REQUEST,
            NOT_FOUND_IS_SUCCESSFUL
    );

    static final Set<Relationship> BASE_RELATIONSHIPS =
            Set.of(REL_ORIGINAL, REL_FAILURE, REL_RETRY, REL_SUCCESSFUL, REL_ERRORS);

    private static final int READER_BUFFER_SIZE = 65536;

    private final AtomicBoolean bulkRequestOutputEnabled = new AtomicBoolean(false);
    private boolean outputBulkRequest;
    private ObjectReader mapReader;
    private ObjectWriter suppressingWriter;

    @Override
    Set<Relationship> getBaseRelationships() {
        return BASE_RELATIONSHIPS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> rels = new HashSet<>(super.getRelationships());
        if (bulkRequestOutputEnabled.get()) {
            rels.add(REL_BULK_REQUEST);
        }
        return rels;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        super.onPropertyModified(descriptor, oldValue, newValue);
        if (OUTPUT_BULK_REQUEST.equals(descriptor)) {
            bulkRequestOutputEnabled.set(Boolean.parseBoolean(newValue));
        }
    }

    @Override
    public void migrateProperties(final PropertyConfiguration config) {
        super.migrateProperties(config);

        // Migrate legacy property names from PutElasticsearchJson
        config.removeProperty("put-es-json-error-documents");
        config.renameProperty("put-es-json-id-attr", ID_ATTRIBUTE.getName());
        config.renameProperty("put-es-json-script", SCRIPT.getName());
        config.renameProperty("put-es-json-scripted-upsert", SCRIPTED_UPSERT.getName());
        config.renameProperty("put-es-json-dynamic_templates", DYNAMIC_TEMPLATES.getName());
        config.renameProperty("put-es-json-charset", CHARSET.getName());
        config.renameProperty("put-es-json-not_found-is-error", AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL.getName());

        // If INPUT_FORMAT was not explicitly set, this flow was migrated from PutElasticsearchJson — default to Single JSON.
        // Set this before migrating BATCH_SIZE so its dependsOn condition is satisfied when NiFi processes the property.
        if (!config.hasProperty(INPUT_FORMAT.getName())) {
            config.setProperty(INPUT_FORMAT.getName(), InputFormat.SINGLE_JSON.getValue());
        }

        // Migrate "Batch Size" (from PutElasticsearchJson) to the new name used in this processor.
        // INPUT_FORMAT is set first above so its dependsOn condition is satisfied when NiFi processes BATCH_SIZE.
        config.renameProperty(AbstractPutElasticsearch.BATCH_SIZE.getName(), BATCH_SIZE.getName());

        // MAX_BATCH_SIZE is a new property — existing configurations had no byte-based limit.
        // Preserve that behavior on upgrade by leaving the property blank (unbounded).
        // The custom validator on MAX_BATCH_SIZE accepts blank values, so this will not be
        // overwritten by the default of 10 MB. New processor instances get the 10 MB default.
        if (!config.hasProperty(MAX_BATCH_SIZE.getName())) {
            config.setProperty(MAX_BATCH_SIZE.getName(), "");
        }
    }

    @Override
    public void migrateRelationships(final RelationshipConfiguration config) {
        super.migrateRelationships(config);

        // PutElasticsearchJson used "success" before it was renamed to "original"
        config.renameRelationship("success", AbstractPutElasticsearch.REL_ORIGINAL.getName());
    }

    @Override
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        super.onScheduled(context);
        this.notFoundIsSuccessful = context.getProperty(NOT_FOUND_IS_SUCCESSFUL).asBoolean();
        this.outputBulkRequest = context.getProperty(OUTPUT_BULK_REQUEST).asBoolean();
        this.bulkRequestOutputEnabled.set(this.outputBulkRequest);
        this.mapReader = mapper.readerFor(new TypeReference<Map<String, Object>>() { });
        final InputFormat inputFormat = InputFormat.fromValue(context.getProperty(INPUT_FORMAT).getValue());
        if (inputFormat != InputFormat.SINGLE_JSON
                && ElasticSearchClientService.ALWAYS_SUPPRESS.getValue().equals(context.getProperty(SUPPRESS_NULLS).getValue())) {
            final ObjectMapper suppressingMapper = mapper.copy()
                    .setDefaultPropertyInclusion(JsonInclude.Include.NON_EMPTY);
            this.suppressingWriter = suppressingMapper.writer();
        } else {
            this.suppressingWriter = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }
        final String maxBatchSizeStr = context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions().getValue();
        final long maxBatchBytes = StringUtils.isNotBlank(maxBatchSizeStr)
                ? context.getProperty(MAX_BATCH_SIZE).evaluateAttributeExpressions().asDataSize(DataUnit.B).longValue()
                : Long.MAX_VALUE;
        final InputFormat inputFormat = InputFormat.fromValue(context.getProperty(INPUT_FORMAT).getValue());
        final String idAttribute = inputFormat == InputFormat.SINGLE_JSON
                ? context.getProperty(ID_ATTRIBUTE).getValue()
                : null;
        final String documentIdField = inputFormat != InputFormat.SINGLE_JSON
                ? context.getProperty(IDENTIFIER_FIELD).evaluateAttributeExpressions().getValue()
                : null;
        final int batchSize = InputFormat.SINGLE_JSON == inputFormat
                ? context.getProperty(BATCH_SIZE).evaluateAttributeExpressions().asInteger()
                : Integer.MAX_VALUE;
        int flowFilesProcessed = 0;

        // Tracks all FlowFiles that were successfully parsed and submitted (even partially)
        final Set<FlowFile> allProcessedFlowFiles = new LinkedHashSet<>();
        // Tracks FlowFiles that had at least one Elasticsearch document error
        final Set<FlowFile> errorFlowFiles = new LinkedHashSet<>();
        // Deferred bulk-error attributes: applied after each FlowFile's InputStream is closed
        final Map<FlowFile, List<Map<String, Object>>> pendingBulkErrors = new HashMap<>();
        // Failed local record indices per FlowFile — O(error count) memory; used at finalization
        // to re-read FlowFiles once and reconstruct error/success content without buffering all bytes
        final Map<FlowFile, Set<Integer>> pendingErrorRecordIndices = new LinkedHashMap<>();

        // Current chunk accumulation — operationFlowFiles and operationRecordIndices are parallel to operations (same index)
        final List<FlowFile> operationFlowFiles = new ArrayList<>();
        final List<IndexOperationRequest> operations = new ArrayList<>();
        final List<Integer> operationRecordIndices = new ArrayList<>();
        long totalBytesAccumulated = 0;
        long chunkBytes = 0;

        while (flowFile != null) {
            final String indexOp = context.getProperty(INDEX_OP).evaluateAttributeExpressions(flowFile).getValue();
            final String index = context.getProperty(INDEX).evaluateAttributeExpressions(flowFile).getValue();
            final String type = context.getProperty(TYPE).evaluateAttributeExpressions(flowFile).getValue();
            final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(flowFile).getValue();
            final String flowFileIdAttribute = StringUtils.isNotBlank(idAttribute) ? flowFile.getAttribute(idAttribute) : null;

            final Map<String, Object> scriptMap = getMapFromAttribute(SCRIPT, context, flowFile);
            final boolean scriptedUpsert = context.getProperty(SCRIPTED_UPSERT).evaluateAttributeExpressions(flowFile).asBoolean();
            final Map<String, Object> dynamicTemplatesMap = getMapFromAttribute(DYNAMIC_TEMPLATES, context, flowFile);
            final Map<String, String> dynamicProperties = getRequestParametersFromDynamicProperties(context, flowFile);
            final Map<String, String> bulkHeaderFields = getBulkHeaderParameters(dynamicProperties);

            boolean parseError = false;
            try {
                final IndexOperationRequest.Operation o = IndexOperationRequest.Operation.forValue(indexOp);
                if (InputFormat.NDJSON == inputFormat) {
                    // NDJSON: each non-blank line is one JSON document.
                    // Index/Create operations pass raw UTF-8 bytes directly to avoid Map allocation.
                    // Update/Delete/Upsert parse into a Map so the bulk serializer can wrap the payload.
                    int localRecordIndex = 0;
                    try (final BufferedReader reader = new BufferedReader(
                            new InputStreamReader(session.read(flowFile), charset), READER_BUFFER_SIZE)) {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            final String trimmedLine = line.trim();
                            if (trimmedLine.isEmpty()) {
                                continue;
                            }
                            final IndexOperationRequest opRequest;
                            final long docBytes;
                            if (o == IndexOperationRequest.Operation.Index || o == IndexOperationRequest.Operation.Create) {
                                final String id = extractId(trimmedLine, documentIdField, flowFileIdAttribute);
                                final byte[] rawJsonBytes;
                                if (suppressingWriter != null) {
                                    // Parse to Map so NON_NULL/NON_EMPTY inclusion filters apply during serialization.
                                    // JsonNode tree serialization bypasses JsonInclude filters.
                                    rawJsonBytes = suppressingWriter.writeValueAsBytes(mapReader.readValue(trimmedLine));
                                } else {
                                    rawJsonBytes = trimmedLine.getBytes(StandardCharsets.UTF_8);
                                }
                                opRequest = IndexOperationRequest.builder()
                                        .index(index)
                                        .type(type)
                                        .id(id)
                                        .rawJsonBytes(rawJsonBytes)
                                        .operation(o)
                                        .script(scriptMap)
                                        .scriptedUpsert(scriptedUpsert)
                                        .dynamicTemplates(dynamicTemplatesMap)
                                        .headerFields(bulkHeaderFields)
                                        .build();
                                docBytes = rawJsonBytes.length;
                            } else {
                                final Map<String, Object> contentMap = mapReader.readValue(trimmedLine);
                                final String id = resolveId(contentMap, documentIdField, flowFileIdAttribute);
                                opRequest = IndexOperationRequest.builder()
                                        .index(index)
                                        .type(type)
                                        .id(id)
                                        .fields(contentMap)
                                        .operation(o)
                                        .script(scriptMap)
                                        .scriptedUpsert(scriptedUpsert)
                                        .dynamicTemplates(dynamicTemplatesMap)
                                        .headerFields(bulkHeaderFields)
                                        .build();
                                docBytes = trimmedLine.length();
                            }
                            operations.add(opRequest);
                            operationFlowFiles.add(flowFile);
                            operationRecordIndices.add(localRecordIndex++);
                            chunkBytes += docBytes;
                            totalBytesAccumulated += docBytes;
                            if (chunkBytes >= maxBatchBytes) {
                                flushChunk(operations, operationFlowFiles, operationRecordIndices, errorFlowFiles, flowFile, pendingBulkErrors, pendingErrorRecordIndices, context, session);
                                operations.clear();
                                operationFlowFiles.clear();
                                operationRecordIndices.clear();
                                chunkBytes = 0;
                            }
                        }
                    }
                } else if (InputFormat.JSON_ARRAY == inputFormat) {
                    // JSON Array: the FlowFile may contain one or more top-level JSON arrays.
                    // Arrays are read sequentially; elements within each array are streamed one at a
                    // time via JsonParser to avoid loading the entire content into memory.
                    // Each element is re-serialized to compact bytes for Index/Create,
                    // or parsed into a Map for Update/Delete/Upsert.
                    int localRecordIndex = 0;
                    try (final InputStreamReader isr = new InputStreamReader(session.read(flowFile), charset);
                         final JsonParser parser = mapper.getFactory().createParser(isr)) {
                        JsonToken outerToken;
                        while ((outerToken = parser.nextToken()) != null) {
                            if (outerToken != JsonToken.START_ARRAY) {
                                throw new IOException("Expected a JSON array but found: " + outerToken);
                            }
                            JsonToken token;
                            while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                                if (token == null) {
                                    throw new IOException("Malformed JSON Array: reached end of stream before the closing ']'. " +
                                            "Verify the FlowFile content is a complete, valid JSON array.");
                                }
                                final long startOffset = parser.currentTokenLocation().getCharOffset();
                                final IndexOperationRequest opRequest;
                                if (o == IndexOperationRequest.Operation.Index || o == IndexOperationRequest.Operation.Create) {
                                    final long docBytes;
                                    final byte[] rawJsonBytes;
                                    final String id;
                                    if (suppressingWriter != null) {
                                        // Parse directly to Map so NON_NULL/NON_EMPTY inclusion filters apply during
                                        // serialization. JsonNode tree serialization bypasses JsonInclude filters,
                                        // and convertValue(node, Map) adds an extra serialization cycle.
                                        final Map<String, Object> contentMap = mapReader.readValue(parser);
                                        docBytes = Math.max(1, parser.currentLocation().getCharOffset() - startOffset);
                                        rawJsonBytes = suppressingWriter.writeValueAsBytes(contentMap);
                                        id = resolveId(contentMap, documentIdField, flowFileIdAttribute);
                                    } else {
                                        final JsonNode node = mapper.readTree(parser);
                                        docBytes = Math.max(1, parser.currentLocation().getCharOffset() - startOffset);
                                        rawJsonBytes = mapper.writeValueAsBytes(node);
                                        id = extractId(node, documentIdField, flowFileIdAttribute);
                                    }
                                    opRequest = IndexOperationRequest.builder()
                                        .index(index)
                                        .type(type)
                                        .id(id)
                                        .rawJsonBytes(rawJsonBytes)
                                        .operation(o)
                                        .script(scriptMap)
                                        .scriptedUpsert(scriptedUpsert)
                                        .dynamicTemplates(dynamicTemplatesMap)
                                        .headerFields(bulkHeaderFields)
                                        .build();
                                    chunkBytes += docBytes;
                                    totalBytesAccumulated += docBytes;
                                } else {
                                    final Map<String, Object> contentMap = mapReader.readValue(parser);
                                    final long docBytes = Math.max(1, parser.currentLocation().getCharOffset() - startOffset);
                                    final String id = resolveId(contentMap, documentIdField, flowFileIdAttribute);
                                    opRequest = IndexOperationRequest.builder()
                                        .index(index)
                                        .type(type)
                                        .id(id)
                                        .fields(contentMap)
                                        .operation(o)
                                        .script(scriptMap)
                                        .scriptedUpsert(scriptedUpsert)
                                        .dynamicTemplates(dynamicTemplatesMap)
                                        .headerFields(bulkHeaderFields)
                                        .build();
                                    chunkBytes += docBytes;
                                    totalBytesAccumulated += docBytes;
                                }
                                operations.add(opRequest);
                                operationFlowFiles.add(flowFile);
                                operationRecordIndices.add(localRecordIndex++);
                                if (chunkBytes >= maxBatchBytes) {
                                    flushChunk(operations, operationFlowFiles, operationRecordIndices, errorFlowFiles, flowFile, pendingBulkErrors, pendingErrorRecordIndices, context, session);
                                    operations.clear();
                                    operationFlowFiles.clear();
                                    operationRecordIndices.clear();
                                    chunkBytes = 0;
                                }
                            }
                        }
                    }
                } else {
                    // Single JSON: the entire FlowFile is one document parsed into a Map.
                    // The client service serializes the Map, preserving null-suppression settings.
                    try (final InputStream in = session.read(flowFile)) {
                        final Map<String, Object> contentMap = mapReader.readValue(in);
                        final String id = StringUtils.isNotBlank(flowFileIdAttribute) ? flowFileIdAttribute : null;
                        final IndexOperationRequest opRequest = IndexOperationRequest.builder()
                                .index(index)
                                .type(type)
                                .id(id)
                                .fields(contentMap)
                                .operation(o)
                                .script(scriptMap)
                                .scriptedUpsert(scriptedUpsert)
                                .dynamicTemplates(dynamicTemplatesMap)
                                .headerFields(bulkHeaderFields)
                                .build();
                        operations.add(opRequest);
                        operationFlowFiles.add(flowFile);
                        operationRecordIndices.add(0);
                        final long docBytes = flowFile.getSize();
                        chunkBytes += docBytes;
                        totalBytesAccumulated += docBytes;
                        if (chunkBytes >= maxBatchBytes) {
                            flushChunk(operations, operationFlowFiles, operationRecordIndices, errorFlowFiles, flowFile, pendingBulkErrors, pendingErrorRecordIndices, context, session);
                            operations.clear();
                            operationFlowFiles.clear();
                            operationRecordIndices.clear();
                            chunkBytes = 0;
                        }
                    }
                }
            } catch (final ElasticsearchException ese) {
                final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                        ese.isElastic() ? "Routing to retry." : "Routing to failure.");
                getLogger().error(msg, ese);
                final Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
                // Route only the failing in-flight chunk to retry/failure. FlowFiles already
                // successfully indexed by prior _bulk requests are routed normally to avoid
                // duplicate indexing if those FlowFiles are re-processed on retry.
                final Set<FlowFile> inFlight = new LinkedHashSet<>(operationFlowFiles);
                transferFlowFilesOnException(ese, rel, session, true, inFlight.toArray(new FlowFile[0]));
                final Set<FlowFile> alreadyIndexed = new LinkedHashSet<>(allProcessedFlowFiles);
                alreadyIndexed.removeAll(inFlight);
                if (!alreadyIndexed.isEmpty()) {
                    handleFinalResponse(context, session, errorFlowFiles, alreadyIndexed, pendingErrorRecordIndices, inputFormat);
                }
                return;
            } catch (final IOException ioe) {
                getLogger().error("Could not read FlowFile content as valid {}.", inputFormat, ioe);
                removeFlowFileFromChunk(flowFile, operations, operationFlowFiles, operationRecordIndices);
                flowFile = session.putAttribute(flowFile, "elasticsearch.put.error", ioe.getMessage());
                session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                parseError = true;
            } catch (final Exception ex) {
                getLogger().error("Failed processing records.", ex);
                removeFlowFileFromChunk(flowFile, operations, operationFlowFiles, operationRecordIndices);
                flowFile = session.putAttribute(flowFile, "elasticsearch.put.error", ex.getMessage());
                session.penalize(flowFile);
                session.transfer(flowFile, REL_FAILURE);
                parseError = true;
            }

            if (!parseError) {
                allProcessedFlowFiles.add(flowFile);
                // InputStream is now closed — safe to apply any deferred bulk-error attributes
                applyPendingBulkErrors(flowFile, pendingBulkErrors, session);
            }

            flowFilesProcessed++;

            // For Single JSON, stop when the count-based batch limit is reached; otherwise use size limit
            if (InputFormat.SINGLE_JSON == inputFormat ? flowFilesProcessed >= batchSize : totalBytesAccumulated >= maxBatchBytes) {
                break;
            }

            flowFile = session.get();
        }

        // Flush any remaining operations (all InputStreams are closed at this point)
        if (!operations.isEmpty()) {
            try {
                flushChunk(operations, operationFlowFiles, operationRecordIndices, errorFlowFiles, null, pendingBulkErrors, pendingErrorRecordIndices, context, session);
            } catch (final ElasticsearchException ese) {
                final String msg = String.format("Encountered a server-side problem with Elasticsearch. %s",
                        ese.isElastic() ? "Routing to retry." : "Routing to failure.");
                getLogger().error(msg, ese);
                final Relationship rel = ese.isElastic() ? REL_RETRY : REL_FAILURE;
                final Set<FlowFile> inFlight = new LinkedHashSet<>(operationFlowFiles);
                transferFlowFilesOnException(ese, rel, session, true, inFlight.toArray(new FlowFile[0]));
                final Set<FlowFile> alreadyIndexed = new LinkedHashSet<>(allProcessedFlowFiles);
                alreadyIndexed.removeAll(inFlight);
                if (!alreadyIndexed.isEmpty()) {
                    handleFinalResponse(context, session, errorFlowFiles, alreadyIndexed, pendingErrorRecordIndices, inputFormat);
                }
                return;
            } catch (final Exception ex) {
                getLogger().error("Could not index documents.", ex);
                final Set<FlowFile> inFlight = new LinkedHashSet<>(operationFlowFiles);
                transferFlowFilesOnException(ex, REL_FAILURE, session, false, inFlight.toArray(new FlowFile[0]));
                final Set<FlowFile> alreadyIndexed = new LinkedHashSet<>(allProcessedFlowFiles);
                alreadyIndexed.removeAll(inFlight);
                if (!alreadyIndexed.isEmpty()) {
                    handleFinalResponse(context, session, errorFlowFiles, alreadyIndexed, pendingErrorRecordIndices, inputFormat);
                }
                context.yield();
                return;
            }
        }

        if (allProcessedFlowFiles.isEmpty()) {
            getLogger().warn("No records successfully parsed for sending to Elasticsearch");
        } else {
            handleFinalResponse(context, session, errorFlowFiles, allProcessedFlowFiles, pendingErrorRecordIndices, inputFormat);
        }
    }

    /**
     * Removes all entries belonging to {@code target} from the three parallel lists, so that a
     * FlowFile that failed mid-read does not leave stale references that would cause
     * {@link org.apache.nifi.processor.exception.FlowFileHandlingException} on the next flush.
     */
    private void removeFlowFileFromChunk(final FlowFile target,
                                          final List<IndexOperationRequest> operations,
                                          final List<FlowFile> operationFlowFiles,
                                          final List<Integer> operationRecordIndices) {
        for (int i = operations.size() - 1; i >= 0; i--) {
            if (operationFlowFiles.get(i) == target) {
                operations.remove(i);
                operationFlowFiles.remove(i);
                operationRecordIndices.remove(i);
            }
        }
    }

    /**
     * Applies any bulk-error attributes that were deferred for {@code flowFile} while its
     * InputStream was open.  Must only be called after the InputStream has been closed.
     */
    private void applyPendingBulkErrors(final FlowFile flowFile,
                                         final Map<FlowFile, List<Map<String, Object>>> pendingBulkErrors,
                                         final ProcessSession session) {
        final List<Map<String, Object>> errorList = pendingBulkErrors.remove(flowFile);
        if (errorList == null) {
            return;
        }
        try {
            final Object errorObj = errorList.size() == 1 ? errorList.get(0) : errorList;
            session.putAttribute(flowFile, "elasticsearch.bulk.error", mapper.writeValueAsString(errorObj));
        } catch (final JsonProcessingException e) {
            session.putAttribute(flowFile, "elasticsearch.bulk.error",
                    String.format("{\"error\": {\"type\": \"elasticsearch_response_parse_error\", \"reason\": \"%s\"}}",
                            e.getMessage().replace("\"", "\\\"")));
        }
    }

    /**
     * Sends the accumulated batch of operations to Elasticsearch via the _bulk API and records
     * any per-document errors. If {@code openStreamFlowFile} is non-null, error attributes for
     * that FlowFile are deferred into {@code pendingBulkErrors} so they are applied after its
     * InputStream is closed (calling {@code putAttribute} on an open FlowFile throws
     * {@link org.apache.nifi.processor.exception.FlowFileHandlingException}).
     * <p>
     * Only the local record index of each failed operation is stored (O(error count) memory).
     * Error/success content is reconstructed by re-reading the FlowFile in
     * {@link #handleFinalResponse}.
     */
    private void flushChunk(final List<IndexOperationRequest> operations, final List<FlowFile> operationFlowFiles,
                             final List<Integer> operationRecordIndices,
                             final Set<FlowFile> errorFlowFiles, final FlowFile openStreamFlowFile,
                             final Map<FlowFile, List<Map<String, Object>>> pendingBulkErrors,
                             final Map<FlowFile, Set<Integer>> pendingErrorRecordIndices,
                             final ProcessContext context, final ProcessSession session) throws IOException {
        if (outputBulkRequest) {
            outputBulkRequestFlowFile(operations, operationFlowFiles, session);
        }

        final FlowFile firstFlowFile = operationFlowFiles.get(0);
        final Map<String, String> dynamicProperties = getRequestParametersFromDynamicProperties(context, firstFlowFile);
        final IndexOperationResponse response = clientService.get().bulk(operations,
                new ElasticsearchRequestOptions(getRequestURLParameters(dynamicProperties), getRequestHeadersFromDynamicProperties(context, firstFlowFile)));

        final Map<Integer, Map<String, Object>> errors = findElasticsearchResponseErrors(response);

        // Attach per-FlowFile error attributes; defer putAttribute for the FlowFile whose
        // InputStream is currently open (putAttribute would throw FlowFileHandlingException).
        // Record only the local record index of each failure — content is reconstructed at finalization.
        final Map<FlowFile, List<Map<String, Object>>> flowFileErrors = new HashMap<>();
        errors.forEach((index, error) -> {
            final FlowFile flowFile = operationFlowFiles.get(index);
            errorFlowFiles.add(flowFile);
            flowFileErrors.computeIfAbsent(flowFile, k -> new ArrayList<>()).add(error);
            pendingErrorRecordIndices.computeIfAbsent(flowFile, k -> new HashSet<>()).add(operationRecordIndices.get(index));
        });
        flowFileErrors.forEach((flowFile, errorList) -> {
            if (flowFile == openStreamFlowFile) {
                // Defer: InputStream still open — apply after the try-with-resources closes
                pendingBulkErrors.computeIfAbsent(flowFile, k -> new ArrayList<>()).addAll(errorList);
            } else {
                try {
                    final Object errorObj = errorList.size() == 1 ? errorList.get(0) : errorList;
                    session.putAttribute(flowFile, "elasticsearch.bulk.error", mapper.writeValueAsString(errorObj));
                } catch (final JsonProcessingException e) {
                    session.putAttribute(flowFile, "elasticsearch.bulk.error",
                            String.format("{\"error\": {\"type\": \"elasticsearch_response_parse_error\", \"reason\": \"%s\"}}",
                                    e.getMessage().replace("\"", "\\\"")));
                }
            }
        });

        if (!errors.isEmpty()) {
            handleElasticsearchDocumentErrors(errors, session, firstFlowFile);
        }
    }

    /**
     * Serializes the pending batch into an Elasticsearch _bulk NDJSON body and writes it to a
     * new FlowFile on the {@code bulk_request} relationship for inspection or debugging.
     * Each operation produces an action metadata line followed by a document line
     * (Delete operations have no document line). Failures here are logged and swallowed so
     * that a debug-output failure does not abort the actual indexing.
     */
    private void outputBulkRequestFlowFile(final List<IndexOperationRequest> operations,
                                            final List<FlowFile> operationFlowFiles,
                                            final ProcessSession session) {
        final FlowFile parent = operationFlowFiles.get(0);
        FlowFile bulkRequestFF = session.create(parent);
        try (final OutputStream out = session.write(bulkRequestFF)) {
            for (final IndexOperationRequest op : operations) {
                // Action metadata line
                final Map<String, Object> actionBody = new LinkedHashMap<>();
                if (StringUtils.isNotBlank(op.getIndex())) {
                    actionBody.put("_index", op.getIndex());
                }
                if (StringUtils.isNotBlank(op.getType())) {
                    actionBody.put("_type", op.getType());
                }
                if (StringUtils.isNotBlank(op.getId())) {
                    actionBody.put("_id", op.getId());
                }
                if (op.getDynamicTemplates() != null && !op.getDynamicTemplates().isEmpty()) {
                    actionBody.put("dynamic_templates", op.getDynamicTemplates());
                }
                if (op.getHeaderFields() != null) {
                    actionBody.putAll(op.getHeaderFields());
                }

                // Upsert maps to "update" in the ES bulk API
                final String actionName = op.getOperation() == IndexOperationRequest.Operation.Upsert
                        ? "update" : op.getOperation().getValue();
                final Map<String, Object> actionLine = Map.of(actionName, actionBody);
                out.write(mapper.writeValueAsBytes(actionLine));
                out.write('\n');

                // Document line (delete has no document)
                if (op.getOperation() != IndexOperationRequest.Operation.Delete) {
                    if (op.getRawJsonBytes() != null) {
                        // Index/Create with rawJson path — write directly, no Map round-trip
                        out.write(op.getRawJsonBytes());
                    } else {
                        final Map<String, Object> docLine = new LinkedHashMap<>();
                        if (op.getOperation() == IndexOperationRequest.Operation.Update
                                || op.getOperation() == IndexOperationRequest.Operation.Upsert) {
                            if (op.getScript() != null && !op.getScript().isEmpty()) {
                                docLine.put("script", op.getScript());
                                if (op.isScriptedUpsert()) {
                                    docLine.put("scripted_upsert", true);
                                }
                                if (op.getFields() != null && !op.getFields().isEmpty()) {
                                    docLine.put("upsert", op.getFields());
                                }
                            } else {
                                docLine.put("doc", op.getFields());
                                if (op.getOperation() == IndexOperationRequest.Operation.Upsert) {
                                    docLine.put("doc_as_upsert", true);
                                }
                            }
                        } else {
                            docLine.putAll(op.getFields());
                        }
                        out.write(mapper.writeValueAsBytes(docLine));
                    }
                    out.write('\n');
                }
            }
        } catch (final IOException e) {
            getLogger().warn("Failed to write bulk request FlowFile for inspection.", e);
            session.remove(bulkRequestFF);
            return;
        }
        bulkRequestFF = session.putAttribute(bulkRequestFF, "elasticsearch.bulk.operation.count", String.valueOf(operations.size()));
        session.transfer(bulkRequestFF, REL_BULK_REQUEST);
    }

    /**
     * Extracts the document ID from a raw JSON string using a streaming parser.
     * Stops as soon as the target field is found, avoiding a full tree parse.
     * Used for Index/Create operations to avoid the Map allocation overhead.
     */
    private String extractId(final String rawJson, final String idAttribute, final String flowFileIdAttribute) throws IOException {
        if (StringUtils.isBlank(idAttribute)) {
            return StringUtils.isNotBlank(flowFileIdAttribute) ? flowFileIdAttribute : null;
        }
        try (final JsonParser p = mapper.getFactory().createParser(rawJson)) {
            while (p.nextToken() != null) {
                if (idAttribute.equals(p.currentName()) && p.nextToken() != null && !p.currentToken().isStructStart()) {
                    final String value = p.getText();
                    return StringUtils.isNotBlank(value) ? value
                            : (StringUtils.isNotBlank(flowFileIdAttribute) ? flowFileIdAttribute : null);
                }
            }
        }
        return StringUtils.isNotBlank(flowFileIdAttribute) ? flowFileIdAttribute : null;
    }

    /**
     * Extracts the document ID from a pre-parsed JsonNode.
     * Used for JSON Array Index/Create operations where the node is already available.
     */
    private String extractId(final JsonNode node, final String idAttribute, final String flowFileIdAttribute) {
        if (StringUtils.isBlank(idAttribute)) {
            return StringUtils.isNotBlank(flowFileIdAttribute) ? flowFileIdAttribute : null;
        }
        final JsonNode idNode = node.get(idAttribute);
        if (idNode != null && !idNode.isNull()) {
            return idNode.asText();
        }
        return StringUtils.isNotBlank(flowFileIdAttribute) ? flowFileIdAttribute : null;
    }

    /**
     * Resolves the document ID for Update/Delete/Upsert operations from the already-parsed content Map.
     * Falls back to the FlowFile attribute value when the field is absent from the document.
     */
    private String resolveId(final Map<String, Object> contentMap, final String idAttribute, final String flowFileIdAttribute) {
        if (StringUtils.isBlank(idAttribute)) {
            return null;
        }
        final Object idObj = contentMap.get(idAttribute);
        if (idObj != null) {
            return idObj.toString();
        }
        return StringUtils.isNotBlank(flowFileIdAttribute) ? flowFileIdAttribute : null;
    }

    /**
     * Reads a processor property as a JSON Object string and deserializes it into a Map.
     * Returns an empty Map when the property is blank. Throws {@link ProcessException} if the
     * value is not valid JSON or not a JSON Object.
     */
    private Map<String, Object> getMapFromAttribute(final PropertyDescriptor propertyDescriptor, final ProcessContext context, final FlowFile input) {
        final String propertyValue = context.getProperty(propertyDescriptor).evaluateAttributeExpressions(input).getValue();
        try {
            return StringUtils.isNotBlank(propertyValue) ? mapper.readValue(propertyValue, new TypeReference<Map<String, Object>>() { }) : Collections.emptyMap();
        } catch (final JsonProcessingException jpe) {
            throw new ProcessException(propertyDescriptor.getDisplayName() + " must be a String parsable into a JSON Object", jpe);
        }
    }

    /**
     * Routes all successfully processed FlowFiles to their final relationships after all
     * _bulk requests for the trigger have completed.
     * <ul>
     *   <li>FlowFiles with no errors are cloned to {@code REL_SUCCESSFUL} without re-reading.</li>
     *   <li>FlowFiles with errors are re-read exactly once to split records by failed indices:
     *       failed records go to a clone on {@code REL_ERRORS}; successful records (if any) go
     *       to a clone on {@code REL_SUCCESSFUL}. For SINGLE_JSON the FlowFile is always a single
     *       document so a direct clone is used instead of re-reading.</li>
     *   <li>All FlowFiles go to {@code REL_ORIGINAL}.</li>
     * </ul>
     */
    private void handleFinalResponse(final ProcessContext context, final ProcessSession session,
                                     final Set<FlowFile> errorFlowFiles, final Set<FlowFile> allFlowFiles,
                                     final Map<FlowFile, Set<Integer>> pendingErrorRecordIndices,
                                     final InputFormat inputFormat) {
        final List<FlowFile> copiedErrors = new ArrayList<>();
        final List<FlowFile> successfulDocuments = new ArrayList<>();

        for (final FlowFile ff : allFlowFiles) {
            if (!errorFlowFiles.contains(ff)) {
                // All records succeeded: clone the original without re-reading
                successfulDocuments.add(session.clone(ff));
            } else if (inputFormat == InputFormat.SINGLE_JSON) {
                // One document per FlowFile — it failed entirely; clone directly
                copiedErrors.add(session.clone(ff));
            } else {
                // NDJSON or JSON Array: re-read once and split records into error/success by index
                final Set<Integer> failedIndices = pendingErrorRecordIndices.getOrDefault(ff, Collections.emptySet());
                final String charset = context.getProperty(CHARSET).evaluateAttributeExpressions(ff).getValue();
                final ByteArrayOutputStream errorBaos = new ByteArrayOutputStream();
                final ByteArrayOutputStream successBaos = new ByteArrayOutputStream();
                session.read(ff, in -> splitRecords(in, failedIndices, charset, inputFormat, errorBaos, successBaos));

                if (errorBaos.size() > 0) {
                    final byte[] errorBytes = errorBaos.toByteArray();
                    FlowFile errorFf = session.clone(ff);
                    errorFf = session.write(errorFf, out -> out.write(errorBytes));
                    copiedErrors.add(errorFf);
                }
                if (successBaos.size() > 0) {
                    final byte[] successBytes = successBaos.toByteArray();
                    FlowFile successFf = session.clone(ff);
                    successFf = session.write(successFf, out -> out.write(successBytes));
                    successFf = session.removeAttribute(successFf, "elasticsearch.bulk.error");
                    successfulDocuments.add(successFf);
                }
            }
        }

        session.transfer(copiedErrors, REL_ERRORS);
        copiedErrors.forEach(e ->
                session.getProvenanceReporter().send(
                        e,
                        clientService.get().getTransitUrl(
                                context.getProperty(INDEX).evaluateAttributeExpressions(e).getValue(),
                                context.getProperty(TYPE).evaluateAttributeExpressions(e).getValue()
                        ),
                        "Elasticsearch _bulk operation error"
                )
        );

        session.transfer(successfulDocuments, REL_SUCCESSFUL);
        successfulDocuments.forEach(s ->
                session.getProvenanceReporter().send(
                        s,
                        clientService.get().getTransitUrl(
                                context.getProperty(INDEX).evaluateAttributeExpressions(s).getValue(),
                                context.getProperty(TYPE).evaluateAttributeExpressions(s).getValue()
                        )
                )
        );

        session.transfer(allFlowFiles, REL_ORIGINAL);
    }

    /**
     * Reads records from {@code in} according to {@code inputFormat} and writes each record
     * (as compact single-line JSON followed by {@code '\n'}) to either {@code errorOut} or
     * {@code successOut} depending on whether its zero-based index is in {@code failedIndices}.
     * <p>
     * The {@code InputStream} is owned by the NiFi session and must not be closed here;
     * {@code BufferedReader} and {@code JsonParser} wrappers are intentionally not closed so
     * that they do not propagate a close to the underlying stream.
     */
    private void splitRecords(final InputStream in, final Set<Integer> failedIndices, final String charset,
                               final InputFormat inputFormat, final OutputStream errorOut,
                               final OutputStream successOut) throws IOException {
        // Track whether each output stream has received its first record so we can write '\n'
        // between records (not after the last one), avoiding a trailing blank line.
        boolean firstError = true;
        boolean firstSuccess = true;
        int idx = 0;
        if (inputFormat == InputFormat.NDJSON) {
            // BufferedReader not closed intentionally — 'in' is session-managed
            final BufferedReader reader = new BufferedReader(new InputStreamReader(in, charset), READER_BUFFER_SIZE);
            String line;
            while ((line = reader.readLine()) != null) {
                final String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                final byte[] bytes = trimmed.getBytes(StandardCharsets.UTF_8);
                if (failedIndices.contains(idx)) {
                    if (!firstError) {
                        errorOut.write('\n');
                    }
                    errorOut.write(bytes);
                    firstError = false;
                } else {
                    if (!firstSuccess) {
                        successOut.write('\n');
                    }
                    successOut.write(bytes);
                    firstSuccess = false;
                }
                idx++;
            }
        } else {
            // JSON Array: stream elements one at a time via JsonParser
            // JsonParser not closed intentionally — 'in' is session-managed
            final JsonParser parser = mapper.getFactory().createParser(new InputStreamReader(in, charset));
            JsonToken outerToken;
            while ((outerToken = parser.nextToken()) != null) {
                if (outerToken != JsonToken.START_ARRAY) {
                    continue;
                }
                JsonToken token;
                while ((token = parser.nextToken()) != JsonToken.END_ARRAY) {
                    if (token == null) {
                        break;
                    }
                    final JsonNode node = mapper.readTree(parser);
                    final byte[] bytes = mapper.writeValueAsBytes(node);
                    if (failedIndices.contains(idx)) {
                        if (!firstError) {
                            errorOut.write('\n');
                        }
                        errorOut.write(bytes);
                        firstError = false;
                    } else {
                        if (!firstSuccess) {
                            successOut.write('\n');
                        }
                        successOut.write(bytes);
                        firstSuccess = false;
                    }
                    idx++;
                }
            }
        }
    }
}
