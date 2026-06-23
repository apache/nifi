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

import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessContext;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.RelationshipMigrationResult;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PutElasticsearchJsonTest extends AbstractPutElasticsearchTest {
    private static final String TEST_DIR = "src/test/resources/PutElasticsearchJsonTest";
    private static final Path BATCH_WITH_ERROR = Paths.get(TEST_DIR, "batchWithError.json");
    private static String script;
    private static String dynamicTemplates;
    private static String flowFileContents;
    private static String sampleErrorResponse;
    private static Map<String, Object> expectedScript;
    private static Map<String, Object> expectedDynamicTemplate;

    @Override
    public Class<? extends AbstractPutElasticsearch> getTestProcessor() {
        return PutElasticsearchJson.class;
    }

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        sampleErrorResponse = JsonUtils.readString(Paths.get(TEST_COMMON_DIR, "sampleErrorResponse.json"));
        flowFileContents = JsonUtils.readString(Paths.get(TEST_DIR, "flowFileContents.json"));
        script = JsonUtils.readString(Paths.get(TEST_DIR, "script.json"));
        dynamicTemplates = JsonUtils.readString(Paths.get(TEST_COMMON_DIR, "dynamicTemplates.json"));

        expectedScript = new LinkedHashMap<>();
        expectedScript.put("_source", "some script");
        expectedScript.put("language", "painless");
        expectedDynamicTemplate = new LinkedHashMap<>();
        expectedDynamicTemplate.put("my_field", "keyword");
        final Map<String, Object> yourField = new LinkedHashMap<>();
        yourField.put("type", "text");
        yourField.put("keyword", Collections.singletonMap("type", "text"));
        expectedDynamicTemplate.put("your_field", yourField);
    }

    @Override
    @BeforeEach
    public void setup() throws Exception {
        super.setup();

        runner.setProperty(PutElasticsearchJson.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "1");

        runner.assertValid();
    }

    void basicTest(final int failure, final int retry, final int successful) {
        final Consumer<List<IndexOperationRequest>> consumer = (final List<IndexOperationRequest> items) -> {
            final long nullIdCount = items.stream().filter(item -> item.getId() == null).count();
            final long indexCount = items.stream().filter(item -> "test_index".equals(item.getIndex())).count();
            final long typeCount = items.stream().filter(item -> "test_type".equals(item.getType())).count();
            final long opCount = items.stream().filter(item -> IndexOperationRequest.Operation.Index.equals(item.getOperation())).count();
            final long emptyScriptCount = items.stream().filter(item -> item.getScript().isEmpty()).count();
            final long falseScriptedUpsertCount = items.stream().filter(item -> !item.isScriptedUpsert()).count();
            final long emptyDynamicTemplatesCount = items.stream().filter(item -> item.getDynamicTemplates().isEmpty()).count();
            final long emptyHeaderFields = items.stream().filter(item -> item.getHeaderFields().isEmpty()).count();

            assertEquals(1L, nullIdCount);
            assertEquals(1L, indexCount);
            assertEquals(1L, typeCount);
            assertEquals(1L, opCount);
            assertEquals(1L, emptyScriptCount);
            assertEquals(1L, falseScriptedUpsertCount);
            assertEquals(1L, emptyDynamicTemplatesCount);
            assertEquals(1L, emptyHeaderFields);
        };

        basicTest(failure, retry, successful, consumer);
    }

    void basicTest(final int failure, final int retry, final int successful, final Consumer<List<IndexOperationRequest>> consumer) {
        basicTest(failure, retry, successful, consumer, Collections.emptyMap());
    }

    void basicTest(final int failure, final int retry, final int successful, final Consumer<List<IndexOperationRequest>> consumer, final Map<String, String> attr) {
        clientService.setEvalConsumer(consumer);
        basicTest(failure, retry, successful, attr);
    }

    void basicTest(final int failure, final int retry, final int successful, final Map<String, String> attr) {
        if (attr != null && !attr.isEmpty()) {
            runner.enqueue(flowFileContents, attr);
        } else {
            runner.enqueue(flowFileContents);
        }

        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, failure);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, retry);
        // for the "basic test"s, all original FlowFiles should be successful
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, successful);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, successful);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);

        assertEquals(successful, runner.getProvenanceEvents().stream()
                .filter(e -> ProvenanceEventType.SEND == e.getEventType() && e.getDetails() == null)
                .count());
    }

    @Test
    void testMigrateProperties() {
        runner.removeProperty(AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL);
        runner.setProperty("put-es-json-not_found-is-error", "true");
        runner.setProperty("put-es-json-error-documents", "true");
        runner.assertValid();

        final PropertyMigrationResult result = runner.migrateProperties();

        runner.assertValid();
        assertEquals("true", runner.getProcessContext().getProperty(AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL).getValue());
        assertTrue(runner.getProcessContext().getProperties().keySet().stream().noneMatch(pd -> "put-es-json-not_found-is-error".equals(pd.getName())));
        assertTrue(runner.getProcessContext().getProperties().keySet().stream().noneMatch(pd -> "put-es-json-error-documents".equals(pd.getName())));

        assertTrue(1 < result.getPropertiesRenamed().size());
        assertEquals(AbstractPutElasticsearch.NOT_FOUND_IS_SUCCESSFUL.getName(), result.getPropertiesRenamed().get("put-es-json-not_found-is-error"));
        assertEquals(1, result.getPropertiesRemoved().size());
        assertTrue(result.getPropertiesRemoved().contains("put-es-json-error-documents"));
        assertEquals(0, result.getPropertiesUpdated().size());
    }

    @Test
    void testMigrateRelationships() {
        runner.addConnection("success");
        assertFalse(runner.getProcessContext().hasConnection(AbstractPutElasticsearch.REL_ORIGINAL));

        final RelationshipMigrationResult result = runner.migrateRelationships();

        assertTrue(runner.getProcessContext().hasConnection(AbstractPutElasticsearch.REL_ORIGINAL));
        assertTrue(((MockProcessContext) runner.getProcessContext()).getAllRelationships().stream().noneMatch(r -> "success".equals(r.getName())));

        assertEquals(1, result.getRenamedRelationships().size());
        assertEquals(AbstractPutElasticsearch.REL_ORIGINAL.getName(), result.getRenamedRelationships().get("success"));
        assertEquals(0, result.getPreviousRelationships().size());
    }

    @Test
    void simpleTest() {
        clientService.setEvalParametersConsumer((Map<String, String> params) -> assertTrue(params.isEmpty()));

        basicTest(0, 0, 1);
    }

    @Test
    void simpleTestWithDocIdAndRequestParametersAndBulkHeadersAndRequestHeaders() {
        runner.setProperty("refresh", "true");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "routing", "1");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "version", "${version}");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "empty", "${empty}");
        runner.setProperty(ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER + "Accept", "application/json");
        runner.setProperty("slices", "${slices}");
        runner.setProperty("another", "${blank}");
        runner.setEnvironmentVariableValue("slices", "auto");
        runner.setEnvironmentVariableValue("blank", " ");
        runner.setEnvironmentVariableValue("version", "external");
        runner.assertValid();

        clientService.setEvalParametersConsumer((final Map<String, String> params) -> {
            assertEquals(2, params.size());
            assertEquals("true", params.get("refresh"));
            assertEquals("auto", params.get("slices"));
        });

        clientService.setEvalHeadersConsumer((final Map<String, String> headers) -> {
            assertEquals(1, headers.size());
            assertEquals("application/json", headers.get("Accept"));
        });

        clientService.setEvalConsumer((final List<IndexOperationRequest> items) -> {
            final long idCount = items.stream().filter(item -> "123".equals(item.getId())).count();
            final long indexCount = items.stream().filter(item -> "test_index".equals(item.getIndex())).count();
            final long typeCount = items.stream().filter(item -> "test_type".equals(item.getType())).count();
            final long opCount = items.stream().filter(item -> IndexOperationRequest.Operation.Index.equals(item.getOperation())).count();
            final long headerFieldsCount = items.stream().filter(item -> !item.getHeaderFields().isEmpty()).count();
            assertEquals(1L, idCount);
            assertEquals(1L, indexCount);
            assertEquals(1L, typeCount);
            assertEquals(1L, opCount);
            assertEquals(1L, headerFieldsCount);

            final Map<String, String> headerFields = items.getFirst().getHeaderFields();
            assertEquals(2, headerFields.size());
            assertEquals("1", headerFields.get("routing"));
            assertEquals("external", headerFields.get("version"));
        });

        basicTest(0, 0, 1, Collections.singletonMap("doc_id", "123"));
    }

    @Test
    void simpleTestWithRequestParametersAndBulkHeadersFlowFileEL() {
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setEnvironmentVariableValue("blank", " ");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "routing", "1");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "version", "${version}");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "empty", "${empty}");
        runner.setProperty(ElasticsearchRestProcessor.DYNAMIC_PROPERTY_PREFIX_REQUEST_HEADER + "Accept", "${accept}");
        runner.assertValid();

        clientService.setEvalParametersConsumer((final Map<String, String> params) -> {
            assertEquals(2, params.size());
            assertEquals("true", params.get("refresh"));
            assertEquals("auto", params.get("slices"));
        });

        clientService.setEvalHeadersConsumer((final Map<String, String> headers) -> {
            assertEquals(1, headers.size());
            assertEquals("application/json", headers.get("Accept"));
        });

        clientService.setEvalConsumer((final List<IndexOperationRequest> items) -> {
            final long nullIdCount = items.stream().filter(item -> item.getId() == null).count();
            final long headerFieldsCount = items.stream().filter(item -> !item.getHeaderFields().isEmpty()).count();
            assertEquals(1L, nullIdCount);
            assertEquals(1L, headerFieldsCount);

            final Map<String, String> headerFields = items.getFirst().getHeaderFields();
            assertEquals(2, headerFields.size());
            assertEquals("1", headerFields.get("routing"));
            assertEquals("external", headerFields.get("version"));
        });

        final Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("slices", "auto");
        attributes.put("version", "external");
        attributes.put("accept", "application/json");
        attributes.put("blank", " ");
        attributes.put("doc_id", "");
        basicTest(0, 0, 1, attributes);
    }

    @Test
    void simpleTestWithScriptAndDynamicTemplates() {
        runner.setProperty(PutElasticsearchJson.SCRIPT, script);
        runner.setProperty(PutElasticsearchJson.DYNAMIC_TEMPLATES, dynamicTemplates);
        final Consumer<List<IndexOperationRequest>> consumer = (final List<IndexOperationRequest> items) -> {
            final long scriptCount = items.stream().filter(item -> item.getScript().equals(expectedScript)).count();
            final long falseScriptedUpsertCount = items.stream().filter(item -> !item.isScriptedUpsert()).count();
            final long dynamicTemplatesCount = items.stream().filter(item -> item.getDynamicTemplates().equals(expectedDynamicTemplate)).count();
            assertEquals(1L, scriptCount);
            assertEquals(1L, falseScriptedUpsertCount);
            assertEquals(1L, dynamicTemplatesCount);
        };
        basicTest(0, 0, 1, consumer);
    }

    @Test
    void simpleTestWithScriptedUpsert() {
        runner.setProperty(PutElasticsearchJson.SCRIPT, script);
        runner.setProperty(PutElasticsearchJson.DYNAMIC_TEMPLATES, dynamicTemplates);
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Upsert.getValue().toLowerCase());
        runner.setProperty(PutElasticsearchJson.SCRIPTED_UPSERT, "true");
        final Consumer<List<IndexOperationRequest>> consumer = (final List<IndexOperationRequest> items) -> {
            final long scriptCount = items.stream().filter(item -> item.getScript().equals(expectedScript)).count();
            final long trueScriptedUpsertCount = items.stream().filter(IndexOperationRequest::isScriptedUpsert).count();
            final long dynamicTemplatesCount = items.stream().filter(item -> item.getDynamicTemplates().equals(expectedDynamicTemplate)).count();

            assertEquals(1L, scriptCount);
            assertEquals(1L, trueScriptedUpsertCount);
            assertEquals(1L, dynamicTemplatesCount);
        };
        basicTest(0, 0, 1, consumer);
    }

    @Test
    void simpleTestWithScriptedUpsertEL() {
        runner.setProperty(PutElasticsearchJson.SCRIPT, script);
        runner.setProperty(PutElasticsearchJson.DYNAMIC_TEMPLATES, dynamicTemplates);
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Upsert.getValue().toLowerCase());
        runner.setProperty(PutElasticsearchJson.SCRIPTED_UPSERT, "${scripted}");
        final Consumer<List<IndexOperationRequest>> consumer = (final List<IndexOperationRequest> items) -> {
            final long scriptCount = items.stream().filter(item -> item.getScript().equals(expectedScript)).count();
            final long trueScriptedUpsertCount = items.stream().filter(IndexOperationRequest::isScriptedUpsert).count();
            final long dynamicTemplatesCount = items.stream().filter(item -> item.getDynamicTemplates().equals(expectedDynamicTemplate)).count();

            assertEquals(1L, scriptCount);
            assertEquals(1L, trueScriptedUpsertCount);
            assertEquals(1L, dynamicTemplatesCount);
        };
        basicTest(0, 0, 1, consumer, Map.of("scripted", "true"));
    }

    @Test
    void testNonJsonScript() {
        runner.setProperty(PutElasticsearchJson.SCRIPT, "not-json");
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Upsert.getValue().toLowerCase());
        runner.setProperty(PutElasticsearchJson.SCRIPTED_UPSERT, "true");

        runner.enqueue(flowFileContents);
        final AssertionError ae = assertThrows(AssertionError.class, () -> runner.run());
        assertInstanceOf(ProcessException.class, ae.getCause());
        assertEquals(PutElasticsearchJson.SCRIPT.getDisplayName() + " must be a String parsable into a JSON Object", ae.getCause().getMessage());
    }

    @Test
    void testFatalError() {
        clientService.setThrowFatalError(true);
        basicTest(1, 0, 0);
    }

    @Test
    void testRetryable() {
        clientService.setThrowRetryableError(true);
        basicTest(0, 1, 0);
    }

    @Test
    void testInvalidIndexOperation() {
        runner.setProperty(PutElasticsearchJson.INDEX_OP, "not-valid");
        runner.assertNotValid();
        final AssertionError ae = assertThrows(AssertionError.class, () -> runner.run());
        assertEquals(String.format("Processor has 1 validation failures:\n'%s' validated against 'not-valid' is invalid because %s must be Expression Language or one of %s\n",
                PutElasticsearchJson.INDEX_OP.getName(), PutElasticsearchJson.INDEX_OP.getDisplayName(), PutElasticsearchJson.ALLOWED_INDEX_OPERATIONS), ae.getMessage());

        runner.setProperty(PutElasticsearchJson.INDEX_OP, "\\${operation}");
        runner.assertValid();
        runner.enqueue(flowFileContents, Collections.singletonMap("operation", "not-valid2"));
        runner.run();
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);
    }

    @Test
    void testInputRequired() {
        runner.run();
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);
    }

    @Test
    void testBatchingAndErrorRelationshipNotFoundSuccessful() throws Exception {
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "true");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchJson.NOT_FOUND_IS_SUCCESSFUL, "true");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(sampleErrorResponse));
        final List<String> values = JsonUtils.readListOfMapsAsIndividualJson(Files.readString(BATCH_WITH_ERROR));
        values.forEach(val -> runner.enqueue(val));
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 7);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 4);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 3);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);

        MockFlowFile failedDoc = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst();
        assertTrue(failedDoc.getContent().contains("20abcd"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("mapper_parsing_exception"));

        failedDoc = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).get(1);
        assertTrue(failedDoc.getContent().contains("213,456.9"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("mapper_parsing_exception"));

        failedDoc = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).get(2);
        assertTrue(failedDoc.getContent().contains("unit test"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("some_other_exception"));

        assertEquals(3,  runner.getProvenanceEvents().stream().filter(
                e -> ProvenanceEventType.SEND == e.getEventType() && "Elasticsearch _bulk operation error".equals(e.getDetails())).count());
        assertEquals(4,  runner.getProvenanceEvents().stream().filter(
                e -> ProvenanceEventType.SEND == e.getEventType() && null == e.getDetails()).count());
    }

    @Test
    void testBatchingAndErrorRelationshipNotFoundNotSuccessful() throws Exception {
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "true");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchJson.NOT_FOUND_IS_SUCCESSFUL, "false");
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_RESPONSES, "true");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(sampleErrorResponse));
        final List<String> values = JsonUtils.readListOfMapsAsIndividualJson(Files.readString(BATCH_WITH_ERROR));
        values.forEach(val -> runner.enqueue(val));
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 7);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 3);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 4);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 1);

        MockFlowFile failedDoc = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst();
        assertTrue(failedDoc.getContent().contains("not_found"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("not_found"));

        failedDoc = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).get(1);
        assertTrue(failedDoc.getContent().contains("20abcd"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("number_format_exception"));

        failedDoc = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).get(2);
        assertTrue(failedDoc.getContent().contains("213,456.9"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("mapper_parsing_exception"));

        failedDoc = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).get(3);
        assertTrue(failedDoc.getContent().contains("unit test"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("some_other_exception"));

        final String errorResponses = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERROR_RESPONSES).getFirst().getContent();
        assertTrue(errorResponses.contains("not_found"));
        assertTrue(errorResponses.contains("For input string: 20abc"));
        assertTrue(errorResponses.contains("For input string: 213,456.9"));
        assertTrue(errorResponses.contains("For input string: unit test"));

        assertEquals(4, runner.getProvenanceEvents().stream().filter(e ->
                ProvenanceEventType.SEND == e.getEventType() && "Elasticsearch _bulk operation error".equals(e.getDetails())).count());
        assertEquals(3,  runner.getProvenanceEvents().stream().filter(
                e -> ProvenanceEventType.SEND == e.getEventType() && null == e.getDetails()).count());
    }

    @Test
    void testBatchingAndNoErrorOutput() throws Exception {
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "false");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_RESPONSES, "false");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(sampleErrorResponse));
        for (final String val : JsonUtils.readListOfMapsAsIndividualJson(JsonUtils.readString(Paths.get(TEST_DIR, "batchWithoutError.json")))) {
            runner.enqueue(val);
        }

        runner.assertValid();
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 7);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 4);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 3);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);
    }

    @Test
    void testLargeInputStringHandling() {
        runner.setProperty(ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH, "1KB");
        runner.assertValid();

        final String val = String.format("{\"large\": \"%s\"}", "a".repeat(10000));
        runner.enqueue(val);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);
        final String elasticsearchPutError = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_FAILURE).getFirst().getAttribute("elasticsearch.put.error");
        assertTrue(elasticsearchPutError.contains("exceeds the maximum allowed"));

        // increase Jackson's Max String Length reader settings
        runner.clearTransferState();
        runner.setProperty(ElasticsearchRestProcessor.MAX_JSON_FIELD_STRING_LENGTH, "10KB");
        runner.assertValid();

        runner.enqueue(val);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);
    }

    @Test
    void testInvalidInput() {
        runner.enqueue("not-json");
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_RETRY, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERROR_RESPONSES, 0);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILURE).getFirst();
        assertTrue(flowFile.getAttribute("elasticsearch.put.error").contains("not"));
    }

    // -------------------------------------------------------------------------
    // NDJSON format tests
    // -------------------------------------------------------------------------

    @Test
    void testNdjsonFormat() {
        // Switch to NDJSON mode (one JSON object per line)
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.assertValid();

        // Three documents in a single FlowFile
        final String ndjson = "{\"id\":\"1\",\"msg\":\"hello\"}\n"
                + "{\"id\":\"2\",\"msg\":\"world\"}\n"
                + "{\"id\":\"3\",\"msg\":\"foo\"}\n";

        // Capture the operations sent to Elasticsearch
        final int[] operationCount = {0};
        clientService.setEvalConsumer(items -> operationCount[0] += items.size());

        runner.enqueue(ndjson);
        runner.run();

        // All three documents indexed in one bulk call → one FlowFile through
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        assertEquals(3, operationCount[0], "Expected 3 index operations from NDJSON FlowFile");
    }

    @Test
    void testNdjsonFormatBlankLinesIgnored() {
        // Blank lines between NDJSON records should be silently skipped
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.assertValid();

        final String ndjsonWithBlanks = "\n{\"id\":\"1\",\"msg\":\"hello\"}\n\n{\"id\":\"2\",\"msg\":\"world\"}\n\n";

        final int[] operationCount = {0};
        clientService.setEvalConsumer(items -> operationCount[0] += items.size());

        runner.enqueue(ndjsonWithBlanks);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        assertEquals(2, operationCount[0], "Expected 2 operations — blank lines should be ignored");
    }

    @Test
    void testNdjsonFormatInvalidLineRoutesToFailure() {
        // A malformed JSON line should route the whole FlowFile to failure.
        // IDENTIFIER_FIELD forces JSON parsing per line (via streaming parser), which catches malformed input.
        // Without it, lines are passed as raw bytes and validation happens server-side.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.IDENTIFIER_FIELD, "id");
        runner.assertValid();

        runner.enqueue("{\"id\":\"1\"}\nnot-json\n{\"id\":\"3\"}");
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);

        final MockFlowFile failed = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_FAILURE).getFirst();
        assertNotNull(failed.getAttribute("elasticsearch.put.error"),
                "Failed FlowFile should carry elasticsearch.put.error attribute");
    }

    @Test
    void testNdjsonFormatSizeBatchFlush() {
        // Set MAX_BATCH_SIZE very small so each document forces its own flush,
        // resulting in multiple bulk calls for a single FlowFile.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.MAX_BATCH_SIZE, "1 B");
        runner.assertValid();

        final String ndjson = "{\"id\":\"1\"}\n{\"id\":\"2\"}\n{\"id\":\"3\"}\n";

        final int[] bulkCallCount = {0};
        clientService.setEvalConsumer(items -> bulkCallCount[0]++);

        runner.enqueue(ndjson);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        assertTrue(bulkCallCount[0] > 1, "Expected multiple bulk flushes when MAX_BATCH_SIZE is tiny");
    }

    @Test
    void testNdjsonFormatPartialFailureIsolatesSuccessAndErrorDocs() throws Exception {
        // When an NDJSON FlowFile contains records that partially fail, the successful and failed
        // documents should be routed to REL_SUCCESSFUL and REL_ERRORS respectively, each as a
        // new NDJSON clone containing only those documents.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.NOT_FOUND_IS_SUCCESSFUL, "true");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(sampleErrorResponse));

        // 7 records in one FlowFile; sampleErrorResponse has errors at indices 4, 5, 6
        final String ndjson = "{\"id\":\"1\",\"field2\":\"20\"}\n"
                + "{\"id\":\"2\",\"field2\":\"20\"}\n"
                + "{\"id\":\"3\",\"field2\":\"20\"}\n"
                + "{\"id\":\"4\",\"field2\":\"not_found\"}\n"
                + "{\"id\":\"5\",\"field2\":\"20abcd\"}\n"
                + "{\"id\":\"6\",\"field2\":\"213,456.9\"}\n"
                + "{\"id\":\"7\",\"field2\":\"unit test\"}\n";

        runner.enqueue(ndjson);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);

        // REL_SUCCESSFUL clone contains only the 4 successfully-indexed documents
        final MockFlowFile successFf = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_SUCCESSFUL).getFirst();
        final String successContent = successFf.getContent();
        assertTrue(successContent.contains("\"id\":\"1\""), "Successful clone should include record 1");
        assertTrue(successContent.contains("\"id\":\"2\""), "Successful clone should include record 2");
        assertTrue(successContent.contains("\"id\":\"3\""), "Successful clone should include record 3");
        assertTrue(successContent.contains("not_found"), "Successful clone should include not_found record (treated as success)");
        assertFalse(successContent.contains("20abcd"), "Successful clone should not include failed record 5");
        assertFalse(successContent.contains("213,456.9"), "Successful clone should not include failed record 6");
        assertFalse(successContent.contains("unit test"), "Successful clone should not include failed record 7");

        // REL_ERRORS clone contains only the 3 failed documents
        final MockFlowFile errorFf = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_ERRORS).getFirst();
        final String errorContent = errorFf.getContent();
        assertTrue(errorContent.contains("20abcd"), "Error clone should include failed record 5");
        assertTrue(errorContent.contains("213,456.9"), "Error clone should include failed record 6");
        assertTrue(errorContent.contains("unit test"), "Error clone should include failed record 7");
        assertFalse(errorContent.contains("\"id\":\"1\""), "Error clone should not include successful record 1");
        errorFf.assertAttributeExists("elasticsearch.bulk.error");
    }

    // -------------------------------------------------------------------------
    // JSON Array format tests
    // -------------------------------------------------------------------------

    @Test
    void testJsonArrayFormat() {
        // Switch to JSON Array mode (top-level array of objects)
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.assertValid();

        final String jsonArray = "[{\"id\":\"1\",\"msg\":\"hello\"},{\"id\":\"2\",\"msg\":\"world\"},{\"id\":\"3\",\"msg\":\"foo\"}]";

        final int[] operationCount = {0};
        clientService.setEvalConsumer(items -> operationCount[0] += items.size());

        runner.enqueue(jsonArray);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_ERRORS, 0);
        assertEquals(3, operationCount[0], "Expected 3 index operations from JSON Array FlowFile");
    }

    @Test
    void testJsonArrayFormatMultipleArraysInOneFlowFile() {
        // A FlowFile may contain multiple top-level JSON arrays; all should be indexed
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.assertValid();

        // Two arrays concatenated in one FlowFile
        final String twoArrays = "[{\"id\":\"1\"},{\"id\":\"2\"}][{\"id\":\"3\"},{\"id\":\"4\"}]";

        final int[] operationCount = {0};
        clientService.setEvalConsumer(items -> operationCount[0] += items.size());

        runner.enqueue(twoArrays);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        assertEquals(4, operationCount[0], "Expected 4 operations — 2 per array, 2 arrays");
    }

    @Test
    void testJsonArrayFormatMalformedRoutesToFailure() {
        // A truncated / unclosed array should route to failure with a clear error message
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.assertValid();

        // Missing closing ']'
        runner.enqueue("[{\"id\":\"1\"},{\"id\":\"2\"}");
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);

        final MockFlowFile failed = runner.getFlowFilesForRelationship(AbstractPutElasticsearch.REL_FAILURE).getFirst();
        assertNotNull(failed.getAttribute("elasticsearch.put.error"),
                "Failed FlowFile should carry elasticsearch.put.error attribute");
    }

    @Test
    void testJsonArrayFormatPrettyPrinted() {
        // Pretty-printed (multi-line) JSON array — newlines inside the array must not confuse the parser
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.assertValid();

        final String prettyArray =
                "[\n"
                + "  { \"id\": \"1\", \"msg\": \"hello\" },\n"
                + "  { \"id\": \"2\", \"msg\": \"world\" },\n"
                + "  { \"id\": \"3\", \"msg\": \"foo\" }\n"
                + "]\n";

        final int[] operationCount = {0};
        clientService.setEvalConsumer(items -> operationCount[0] += items.size());

        runner.enqueue(prettyArray);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        assertEquals(3, operationCount[0], "Expected 3 operations from pretty-printed JSON array");
    }

    @Test
    void testJsonArrayFormatMultiplePrettyPrintedArrays() {
        // Multiple pretty-printed arrays in one FlowFile (newlines between and within arrays)
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.assertValid();

        final String twoArraysPretty =
                "[\n"
                + "  { \"id\": \"1\" },\n"
                + "  { \"id\": \"2\" }\n"
                + "]\n"
                + "[\n"
                + "  { \"id\": \"3\" },\n"
                + "  { \"id\": \"4\" }\n"
                + "]\n";

        final int[] operationCount = {0};
        clientService.setEvalConsumer(items -> operationCount[0] += items.size());

        runner.enqueue(twoArraysPretty);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        assertEquals(4, operationCount[0], "Expected 4 operations from two pretty-printed arrays");
    }

    @Test
    void testJsonArrayFormatMixedCompactAndPretty() {
        // One compact array followed by one pretty-printed array in the same FlowFile
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.assertValid();

        final String mixed =
                "[{\"id\":\"1\"},{\"id\":\"2\"}]\n"
                + "[\n"
                + "  { \"id\": \"3\" },\n"
                + "  { \"id\": \"4\" }\n"
                + "]\n";

        final int[] operationCount = {0};
        clientService.setEvalConsumer(items -> operationCount[0] += items.size());

        runner.enqueue(mixed);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 0);
        assertEquals(4, operationCount[0], "Expected 4 operations from mixed compact + pretty arrays");
    }

    @Test
    void testJsonArrayFormatNotAnArrayRoutesToFailure() {
        // Content that starts with something other than '[' is not a JSON array
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.assertValid();

        runner.enqueue("{\"id\":\"1\"}");
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 0);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_FAILURE, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 0);
    }

    // -------------------------------------------------------------------------
    // OUTPUT_BULK_REQUEST relationship test
    // -------------------------------------------------------------------------

    @Test
    void testOutputBulkRequestEnabled() {
        // When Output Bulk Request is true, the raw NDJSON body should be written
        // to the bulk_request relationship in addition to normal processing.
        runner.setProperty(PutElasticsearchJson.OUTPUT_BULK_REQUEST, "true");
        runner.assertValid();

        runner.enqueue(flowFileContents);
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_ORIGINAL, 1);
        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        runner.assertTransferCount(PutElasticsearchJson.REL_BULK_REQUEST, 1);

        final MockFlowFile bulkRequest = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_BULK_REQUEST).getFirst();
        // The bulk request body must contain the action metadata line and the document line
        final String body = bulkRequest.getContent();
        assertTrue(body.contains("\"index\""), "Bulk request body should contain action metadata");
        assertTrue(body.contains("msg"), "Bulk request body should contain document field from flowFileContents");
    }

    @Test
    void testOutputBulkRequestDisabledByDefault() {
        // The bulk_request relationship should not appear unless explicitly enabled
        runner.assertValid();

        runner.enqueue(flowFileContents);
        runner.run();

        runner.assertTransferCount(PutElasticsearchJson.REL_BULK_REQUEST, 0);
    }

    // -------------------------------------------------------------------------
    // Index Field / Timestamp Field / Retain toggles (NIFI-15985)
    // -------------------------------------------------------------------------

    /** Registers a consumer that collects every operation sent to the _bulk API into the returned list. */
    private List<IndexOperationRequest> captureOperations() {
        final List<IndexOperationRequest> captured = new ArrayList<>();
        clientService.setEvalConsumer((final List<IndexOperationRequest> items) -> captured.addAll(items));
        return captured;
    }

    /** Returns the document body of an operation as a String, from raw JSON bytes or the fields Map. */
    private static String docContent(final IndexOperationRequest item) {
        if (item.getRawJsonBytes() != null) {
            return new String(item.getRawJsonBytes(), StandardCharsets.UTF_8);
        }
        return item.getFields() == null ? "" : item.getFields().toString();
    }

    @Test
    void testDocumentFieldNamesWithSlashAndBackslashPassThroughRaw() {
        // Only the configured Identifier/Index/Timestamp Field property values are parsed as nested
        // paths; field names in the document body are never interpreted. With no extraction configured,
        // the raw bytes -- including field names that contain "/" and "\" -- are passed through unchanged.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        final String doc = "{\"a/b\":\"x\",\"c\\\\d\":\"y\",\"msg\":\"hello\"}";
        runner.enqueue(doc + "\n");
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        assertEquals(doc, docContent(ops.getFirst()), "document body passed through byte-for-byte");
    }

    @Test
    void testUnrelatedSlashFieldRetainedWhenExtractingId() {
        // Even on the parse-and-re-serialize path (here forced by removing the extracted id field), a
        // document field whose name contains a "/" is not the configured path and is left untouched.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.IDENTIFIER_FIELD, "doc_id");
        runner.setProperty(PutElasticsearchJson.RETAIN_IDENTIFIER_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"doc_id\":\"1\",\"a/b\":\"x\",\"msg\":\"hello\"}\n");
        runner.run();

        assertEquals("1", ops.getFirst().getId());
        final String content = docContent(ops.getFirst());
        assertTrue(content.contains("\"a/b\""), "unrelated slash-named field retained");
        assertFalse(content.contains("doc_id"), "only the configured field is removed");
    }

    @Test
    void testIndexFieldNdjsonExtractionRetainedByDefault() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "target_index");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"target_index\":\"docs-2026\",\"msg\":\"hello\"}\n");
        runner.run();

        runner.assertTransferCount(AbstractPutElasticsearch.REL_SUCCESSFUL, 1);
        assertEquals(1, ops.size());
        assertEquals("docs-2026", ops.getFirst().getIndex());
        // Retain Index Field defaults to true → the field stays in the document body
        assertTrue(docContent(ops.getFirst()).contains("target_index"));
    }

    @Test
    void testIndexFieldNdjsonRemovedWhenRetainFalse() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "target_index");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"target_index\":\"docs-2026\",\"msg\":\"hello\"}\n");
        runner.run();

        assertEquals("docs-2026", ops.getFirst().getIndex());
        final String content = docContent(ops.getFirst());
        assertFalse(content.contains("target_index"), "Index Field should be stripped when Retain Index Field=false");
        assertTrue(content.contains("hello"), "Other fields should remain");
    }

    @Test
    void testIndexFieldJsonArrayExtractionAndStrip() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "target_index");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("[{\"target_index\":\"docs-a\",\"msg\":\"x\"},{\"target_index\":\"docs-b\",\"msg\":\"y\"}]");
        runner.run();

        assertEquals(2, ops.size());
        assertEquals("docs-a", ops.get(0).getIndex());
        assertEquals("docs-b", ops.get(1).getIndex());
        assertFalse(docContent(ops.get(0)).contains("target_index"));
    }

    @Test
    void testIndexFieldSingleJsonExtractionRetainedByDefault() {
        // SINGLE_JSON is the default Input Content Format
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "target_index");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"target_index\":\"docs-single\",\"msg\":\"x\"}");
        runner.run();

        assertEquals(1, ops.size());
        assertEquals("docs-single", ops.getFirst().getIndex());
        assertTrue(ops.getFirst().getFields().containsKey("target_index"), "field retained by default");
    }

    @Test
    void testIndexFieldFallsBackToIndexPropertyWhenAbsent() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "target_index");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"msg\":\"no index field here\"}\n");
        runner.run();

        assertEquals("test_index", ops.getFirst().getIndex(), "Should fall back to the Index property when the field is absent");
    }

    @Test
    void testTimestampFieldNdjsonCopiesToAtTimestampAndStrips() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.TIMESTAMP_FIELD, "event_time");
        runner.setProperty(PutElasticsearchJson.RETAIN_TIMESTAMP_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"event_time\":\"2026-01-02T03:04:05Z\",\"msg\":\"x\"}\n");
        runner.run();

        final String content = docContent(ops.getFirst());
        assertTrue(content.contains("@timestamp"), "@timestamp should be added");
        assertTrue(content.contains("2026-01-02T03:04:05Z"));
        assertFalse(content.contains("event_time"), "source timestamp field removed when retain=false");
    }

    @Test
    void testTimestampFieldJsonArrayStrip() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.setProperty(PutElasticsearchJson.TIMESTAMP_FIELD, "event_time");
        runner.setProperty(PutElasticsearchJson.RETAIN_TIMESTAMP_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("[{\"event_time\":\"2026-05-31T00:00:00Z\",\"msg\":\"x\"}]");
        runner.run();

        final String content = docContent(ops.getFirst());
        assertTrue(content.contains("@timestamp"));
        assertFalse(content.contains("event_time"));
    }

    @Test
    void testTimestampFieldSingleJsonRetainedByDefault() {
        runner.setProperty(PutElasticsearchJson.TIMESTAMP_FIELD, "event_time");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"event_time\":\"2026-01-02T03:04:05Z\",\"msg\":\"x\"}");
        runner.run();

        final Map<String, Object> fields = ops.getFirst().getFields();
        assertEquals("2026-01-02T03:04:05Z", fields.get("@timestamp"));
        assertTrue(fields.containsKey("event_time"), "source field retained by default");
    }

    @Test
    void testIdentifierFieldRetainedByDefaultNdjson() {
        // Backward compatibility: before this feature the Identifier Field was always kept in the document
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.IDENTIFIER_FIELD, "id");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"id\":\"abc\",\"msg\":\"x\"}\n");
        runner.run();

        assertEquals("abc", ops.getFirst().getId());
        assertTrue(docContent(ops.getFirst()).contains("\"id\""), "Identifier Field kept by default for backward compatibility");
    }

    @Test
    void testIdentifierFieldRemovedWhenRetainFalseNdjson() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.IDENTIFIER_FIELD, "id");
        runner.setProperty(PutElasticsearchJson.RETAIN_IDENTIFIER_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"id\":\"abc\",\"msg\":\"x\"}\n");
        runner.run();

        assertEquals("abc", ops.getFirst().getId());
        final String content = docContent(ops.getFirst());
        assertFalse(content.contains("\"id\""), "identifier field removed when retain=false");
        assertTrue(content.contains("msg"));
    }

    @Test
    void testIndexFieldNumericValueConsistentAcrossFormats() {
        // The Map (Single JSON), JsonNode (JSON Array), and streaming (NDJSON) paths must all
        // resolve a non-string index value to the same string.
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "idx");
        List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"idx\":12345,\"msg\":\"x\"}");
        runner.run();
        assertEquals("12345", ops.getFirst().getIndex(), "Single JSON Map path");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        ops = captureOperations();
        runner.enqueue("[{\"idx\":12345,\"msg\":\"x\"}]");
        runner.run();
        assertEquals("12345", ops.getFirst().getIndex(), "JSON Array JsonNode path");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        ops = captureOperations();
        runner.enqueue("{\"idx\":12345,\"msg\":\"x\"}\n");
        runner.run();
        assertEquals("12345", ops.getFirst().getIndex(), "NDJSON streaming path");
    }

    @Test
    void testIndexFieldNullValueFallsBackConsistently() {
        // A JSON null index value must fall back to the Index property on every path —
        // in particular the NDJSON streaming path must not emit the literal string "null".
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "idx");
        List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"idx\":null,\"msg\":\"x\"}");
        runner.run();
        assertEquals("test_index", ops.getFirst().getIndex(), "Single JSON null -> fallback");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        ops = captureOperations();
        runner.enqueue("[{\"idx\":null,\"msg\":\"x\"}]");
        runner.run();
        assertEquals("test_index", ops.getFirst().getIndex(), "JSON Array null -> fallback");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        ops = captureOperations();
        runner.enqueue("{\"idx\":null,\"msg\":\"x\"}\n");
        runner.run();
        assertEquals("test_index", ops.getFirst().getIndex(), "NDJSON null -> fallback (not literal \"null\")");
    }

    // -------------------------------------------------------------------------
    // Nested field paths (e.g. @metadata/index)
    // -------------------------------------------------------------------------

    @Test
    void testNestedIndexFieldNdjsonPrunesEmptyParent() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "@metadata/index");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"index\":\"docs-2026\"},\"msg\":\"hello\"}\n");
        runner.run();

        assertEquals("docs-2026", ops.getFirst().getIndex());
        final String content = docContent(ops.getFirst());
        assertFalse(content.contains("@metadata"), "now-empty parent should be pruned");
        assertTrue(content.contains("hello"));
    }

    @Test
    void testNestedIndexFieldJsonArrayPrunesEmptyParent() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "@metadata/index");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("[{\"@metadata\":{\"index\":\"docs-a\"},\"msg\":\"x\"}]");
        runner.run();

        assertEquals("docs-a", ops.getFirst().getIndex());
        assertFalse(docContent(ops.getFirst()).contains("@metadata"));
    }

    @Test
    void testNestedIndexFieldSingleJsonPrunesEmptyParent() {
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "@metadata/index");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"index\":\"docs-single\"},\"msg\":\"x\"}");
        runner.run();

        assertEquals("docs-single", ops.getFirst().getIndex());
        assertFalse(ops.getFirst().getFields().containsKey("@metadata"));
    }

    @Test
    void testNestedIndexFieldKeepsParentWithRemainingSiblings() {
        // @metadata still holds another field after the index leaf is removed, so it must NOT be pruned
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "@metadata/index");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"index\":\"docs-2026\",\"keep\":\"yes\"},\"msg\":\"hello\"}\n");
        runner.run();

        assertEquals("docs-2026", ops.getFirst().getIndex());
        final String content = docContent(ops.getFirst());
        assertTrue(content.contains("@metadata"), "parent kept because it still has siblings");
        assertTrue(content.contains("keep"));
        assertFalse(content.contains("\"index\""));
    }

    @Test
    void testNestedIndexFieldRetainedByDefault() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "@metadata/index");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"index\":\"docs-2026\"},\"msg\":\"hello\"}\n");
        runner.run();

        assertEquals("docs-2026", ops.getFirst().getIndex());
        assertTrue(docContent(ops.getFirst()).contains("@metadata"), "nested field retained by default");
    }

    @Test
    void testNestedIdentifierFieldNdjsonPrunesEmptyParent() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.IDENTIFIER_FIELD, "@metadata/id");
        runner.setProperty(PutElasticsearchJson.RETAIN_IDENTIFIER_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"id\":\"abc-123\"},\"msg\":\"x\"}\n");
        runner.run();

        assertEquals("abc-123", ops.getFirst().getId());
        assertFalse(docContent(ops.getFirst()).contains("@metadata"));
    }

    @Test
    void testNestedTimestampFieldNdjsonPrunesEmptyParent() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.TIMESTAMP_FIELD, "@metadata/event_time");
        runner.setProperty(PutElasticsearchJson.RETAIN_TIMESTAMP_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"event_time\":\"2026-01-02T03:04:05Z\"},\"msg\":\"x\"}\n");
        runner.run();

        final String content = docContent(ops.getFirst());
        assertTrue(content.contains("@timestamp"));
        assertTrue(content.contains("2026-01-02T03:04:05Z"));
        assertFalse(content.contains("@metadata"), "empty parent pruned after extracting nested timestamp");
    }

    @Test
    void testNestedTimestampFieldSingleJsonPrunesEmptyParent() {
        runner.setProperty(PutElasticsearchJson.TIMESTAMP_FIELD, "meta/ts");
        runner.setProperty(PutElasticsearchJson.RETAIN_TIMESTAMP_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"meta\":{\"ts\":\"2026-05-31T00:00:00Z\"},\"msg\":\"x\"}");
        runner.run();

        final Map<String, Object> fields = ops.getFirst().getFields();
        assertEquals("2026-05-31T00:00:00Z", fields.get("@timestamp"));
        assertFalse(fields.containsKey("meta"), "empty parent pruned");
    }

    @Test
    void testNestedIndexFieldMissingLeafFallsBack() {
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "@metadata/index");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"other\":\"x\"},\"msg\":\"y\"}\n");
        runner.run();

        assertEquals("test_index", ops.getFirst().getIndex(), "missing nested leaf -> fall back to Index property");
    }

    @Test
    void testNestedIndexFieldConsistentAcrossFormats() {
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "@metadata/index");
        List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"index\":\"docs-x\"},\"msg\":\"a\"}");
        runner.run();
        assertEquals("docs-x", ops.getFirst().getIndex(), "Single JSON nested path");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        ops = captureOperations();
        runner.enqueue("[{\"@metadata\":{\"index\":\"docs-x\"},\"msg\":\"a\"}]");
        runner.run();
        assertEquals("docs-x", ops.getFirst().getIndex(), "JSON Array nested path");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"index\":\"docs-x\"},\"msg\":\"a\"}\n");
        runner.run();
        assertEquals("docs-x", ops.getFirst().getIndex(), "NDJSON nested path");
    }

    @Test
    void testDeeplyNestedIndexFieldPrunesAllEmptyAncestors() {
        // A 3-level path (a/b/c): removing the leaf must recursively prune b and then a,
        // since each ancestor becomes empty in turn.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "a/b/c");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"a\":{\"b\":{\"c\":\"docs-deep\"}},\"msg\":\"hello\"}\n");
        runner.run();

        assertEquals("docs-deep", ops.getFirst().getIndex());
        final String content = docContent(ops.getFirst());
        assertFalse(content.contains("\"a\""), "all empty ancestor objects pruned recursively");
        assertFalse(content.contains("\"b\""));
        assertFalse(content.contains("\"c\""));
        assertTrue(content.contains("hello"));
    }

    @Test
    void testDeeplyNestedIndexFieldKeepsAncestorWithRemainingSibling() {
        // A 3-level path (a/b/c) where the middle object 'b' retains a sibling after the leaf is
        // removed: pruning must stop at 'b' and leave 'a' intact.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "a/b/c");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"a\":{\"b\":{\"c\":\"docs-deep\",\"keep\":\"yes\"}},\"msg\":\"hello\"}\n");
        runner.run();

        assertEquals("docs-deep", ops.getFirst().getIndex());
        final String content = docContent(ops.getFirst());
        assertTrue(content.contains("\"keep\""), "sibling stops the recursive pruning");
        assertTrue(content.contains("\"b\""));
        assertTrue(content.contains("\"a\""));
        assertFalse(content.contains("\"c\""));
    }

    @Test
    void testNestedIdentifierFieldUpdateOperationResolvesAndPrunes() {
        // Update/Delete/Upsert resolve the ID from the parsed content Map via resolveId, a different
        // code path than the Index/Create streaming/JsonNode extraction. Verify the nested path is
        // honored and the now-empty parent is pruned from the doc body.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Update.getValue().toLowerCase());
        runner.setProperty(PutElasticsearchJson.IDENTIFIER_FIELD, "@metadata/id");
        runner.setProperty(PutElasticsearchJson.RETAIN_IDENTIFIER_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"@metadata\":{\"id\":\"abc-123\"},\"msg\":\"x\"}\n");
        runner.run();

        assertEquals("abc-123", ops.getFirst().getId());
        assertFalse(ops.getFirst().getFields().containsKey("@metadata"), "empty parent pruned after extracting nested id");
        assertTrue(ops.getFirst().getFields().containsKey("msg"));
    }

    // -------------------------------------------------------------------------
    // Escaped path separators (e.g. a\/b addresses the flat field named "a/b")
    // -------------------------------------------------------------------------

    @Test
    void testEscapedSlashIndexFieldTreatedAsFlatNameAcrossFormats() {
        // "a\/b" must resolve the top-level field literally named "a/b" on every path,
        // including the NDJSON raw-bytes streaming scan.
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "a\\/b");
        List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"a/b\":\"docs-x\",\"msg\":\"m\"}");
        runner.run();
        assertEquals("docs-x", ops.getFirst().getIndex(), "Single JSON escaped-slash flat field");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.JSON_ARRAY.getValue());
        ops = captureOperations();
        runner.enqueue("[{\"a/b\":\"docs-x\",\"msg\":\"m\"}]");
        runner.run();
        assertEquals("docs-x", ops.getFirst().getIndex(), "JSON Array escaped-slash flat field");
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        ops = captureOperations();
        runner.enqueue("{\"a/b\":\"docs-x\",\"msg\":\"m\"}\n");
        runner.run();
        assertEquals("docs-x", ops.getFirst().getIndex(), "NDJSON escaped-slash flat field (raw streaming path)");
    }

    @Test
    void testEscapedSlashIndexFieldRemovedWhenRetainFalse() {
        // The escaped-slash field is flat, so removal must delete the top-level "a/b" key and must
        // NOT descend into a nonexistent "a" object.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "a\\/b");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"a/b\":\"docs-x\",\"keep\":\"y\"}\n");
        runner.run();

        assertEquals("docs-x", ops.getFirst().getIndex());
        final String content = docContent(ops.getFirst());
        assertFalse(content.contains("a/b"), "escaped-slash flat field removed");
        assertTrue(content.contains("keep"));
    }

    @Test
    void testNestedPathWithEscapedSlashSegment() {
        // "a\/b/c": the first segment is the literal field "a/b", then nested field "c".
        // Removing the leaf prunes the now-empty "a/b" object.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "a\\/b/c");
        runner.setProperty(PutElasticsearchJson.RETAIN_INDEX_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"a/b\":{\"c\":\"docs-x\"},\"msg\":\"m\"}\n");
        runner.run();

        assertEquals("docs-x", ops.getFirst().getIndex());
        final String content = docContent(ops.getFirst());
        assertFalse(content.contains("a/b"), "now-empty object named \"a/b\" pruned");
        assertTrue(content.contains("msg"));
    }

    @Test
    void testEscapedBackslashFieldName() {
        // "a\\b" addresses the flat field literally named "a\b" (single backslash).
        runner.setProperty(PutElasticsearchJson.INDEX_FIELD, "a\\\\b");
        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"a\\\\b\":\"docs-x\",\"msg\":\"m\"}");
        runner.run();
        assertEquals("docs-x", ops.getFirst().getIndex(), "\\\\ resolves to a single literal backslash in the field name");
    }

    @Test
    void testEscapedSlashIdentifierFieldUpdateOperation() {
        // Update resolves the ID from the parsed Map via resolveId; verify the escaped-slash flat
        // field name is honored and removed.
        runner.setProperty(PutElasticsearchJson.INPUT_FORMAT, InputFormat.NDJSON.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Update.getValue().toLowerCase());
        runner.setProperty(PutElasticsearchJson.IDENTIFIER_FIELD, "a\\/b");
        runner.setProperty(PutElasticsearchJson.RETAIN_IDENTIFIER_FIELD, "false");
        runner.assertValid();

        final List<IndexOperationRequest> ops = captureOperations();
        runner.enqueue("{\"a/b\":\"id-1\",\"msg\":\"m\"}\n");
        runner.run();

        assertEquals("id-1", ops.getFirst().getId());
        assertFalse(ops.getFirst().getFields().containsKey("a/b"), "escaped-slash flat id field removed");
        assertTrue(ops.getFirst().getFields().containsKey("msg"));
    }
}
