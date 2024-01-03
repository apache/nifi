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
import org.apache.nifi.processors.elasticsearch.mock.MockBulkLoadClientService;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PutElasticsearchJsonTest extends AbstractPutElasticsearchTest<PutElasticsearchJson> {
    private static final String TEST_DIR = "src/test/resources/PutElasticsearchJsonTest";
    private static final String TEST_COMMON_DIR = "src/test/resources/common";
    private static final Path BATCH_WITH_ERROR = Paths.get(TEST_DIR,"batchWithError.json");
    private static String script;
    private static String dynamicTemplates;
    private MockBulkLoadClientService clientService;
    private TestRunner runner;
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
        sampleErrorResponse = JsonUtils.readString(Paths.get(TEST_COMMON_DIR,"sampleErrorResponse.json"));
        flowFileContents = JsonUtils.readString(Paths.get(TEST_DIR, "flowFileContents.json"));
        script = JsonUtils.readString(Paths.get(TEST_DIR,"script.json"));
        dynamicTemplates = JsonUtils.readString(Paths.get(TEST_COMMON_DIR,"dynamicTemplates.json"));
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

    @BeforeEach
    public void setup() throws Exception {
        runner = TestRunners.newTestRunner(getTestProcessor());
        clientService = new MockBulkLoadClientService();
        clientService.setResponse(new IndexOperationResponse(1500));
        runner.addControllerService("clientService", clientService);
        runner.setProperty(PutElasticsearchJson.ID_ATTRIBUTE, "doc_id");
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Index.getValue());
        runner.setProperty(PutElasticsearchJson.INDEX, "test_index");
        runner.setProperty(PutElasticsearchJson.TYPE, "test_type");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "1");
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_DOCUMENTS, "false");
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "false");
        runner.setProperty(PutElasticsearchJson.CLIENT_SERVICE, "clientService");
        runner.setProperty(PutElasticsearchJson.NOT_FOUND_IS_SUCCESSFUL, "true");
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_RESPONSES, "false");
        runner.enableControllerService(clientService);

        runner.assertValid();
    }

    public void basicTest(final int failure, final int retry, final int success) {
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

        basicTest(failure, retry, success, consumer, null);
    }

    public void basicTest(final int failure, final int retry, final int success, final Consumer<List<IndexOperationRequest>> consumer, final Map<String, String> attr) {
        clientService.setEvalConsumer(consumer);
        basicTest(failure, retry, success, attr);
    }

    public void basicTest(final int failure, final int retry, final int success, final Map<String, String> attr) {
        if (attr != null) {
            runner.enqueue(flowFileContents, attr);
        } else {
            runner.enqueue(flowFileContents);
        }

        runner.run();

        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, failure);
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, retry);
        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, success);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        assertEquals(success, runner.getProvenanceEvents().stream()
                .filter(e -> ProvenanceEventType.SEND == e.getEventType() && e.getDetails() == null)
                .count());
    }

    @Test
    public void simpleTest() {
        clientService.setEvalParametersConsumer((Map<String, String> params) -> assertTrue(params.isEmpty()));

        basicTest(0, 0, 1);
    }

    @Test
    public void simpleTestWithDocIdAndRequestParametersAndBulkHeaders() {
        runner.setProperty("refresh", "true");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "routing", "1");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "version", "${version}");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "empty", "${empty}");
        runner.setProperty("slices", "${slices}");
        runner.setProperty("another", "${blank}");
        runner.setVariable("slices", "auto");
        runner.setVariable("blank", " ");
        runner.setVariable("version", "external");
        runner.assertValid();

        clientService.setEvalParametersConsumer((final Map<String, String> params) -> {
            assertEquals(2, params.size());
            assertEquals("true", params.get("refresh"));
            assertEquals("auto", params.get("slices"));
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

            final Map<String, String> headerFields = items.get(0).getHeaderFields();
            assertEquals(2, headerFields.size());
            assertEquals("1", headerFields.get("routing"));
            assertEquals("external", headerFields.get("version"));
        });

        basicTest(0, 0, 1, Collections.singletonMap("doc_id", "123"));
    }

    @Test
    public void simpleTestWithRequestParametersAndBulkHeadersFlowFileEL() {
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setVariable("blank", " ");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "routing", "1");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "version", "${version}");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "empty", "${empty}");
        runner.assertValid();

        clientService.setEvalParametersConsumer((final Map<String, String> params) -> {
            assertEquals(2, params.size());
            assertEquals("true", params.get("refresh"));
            assertEquals("auto", params.get("slices"));
        });

        clientService.setEvalConsumer((final List<IndexOperationRequest> items) -> {
            final long nullIdCount = items.stream().filter(item -> item.getId() == null).count();
            final long headerFieldsCount = items.stream().filter(item -> !item.getHeaderFields().isEmpty()).count();
            assertEquals(1L, nullIdCount);
            assertEquals(1L, headerFieldsCount);

            final Map<String, String> headerFields = items.get(0).getHeaderFields();
            assertEquals(2, headerFields.size());
            assertEquals("1", headerFields.get("routing"));
            assertEquals("external", headerFields.get("version"));
        });

        final Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("slices", "auto");
        attributes.put("version", "external");
        attributes.put("blank", " ");
        attributes.put("doc_id", "");
        basicTest(0, 0, 1, attributes);
    }

    @Test
    public void simpleTestWithScriptAndDynamicTemplates() {
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
        basicTest(0, 0, 1, consumer, null);
    }

    @Test
    public void simpleTestWithScriptedUpsert() {
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
        basicTest(0, 0, 1, consumer, null);
    }

    @Test
    public void testNonJsonScript() {
        runner.setProperty(PutElasticsearchJson.SCRIPT, "not-json");
        runner.setProperty(PutElasticsearchJson.INDEX_OP, IndexOperationRequest.Operation.Upsert.getValue().toLowerCase());
        runner.setProperty(PutElasticsearchJson.SCRIPTED_UPSERT, "true");

        runner.enqueue(flowFileContents);
        final AssertionError ae = assertThrows(AssertionError.class, () -> runner.run());
        assertInstanceOf(ProcessException.class, ae.getCause());
        assertEquals(PutElasticsearchJson.SCRIPT.getDisplayName() + " must be a String parsable into a JSON Object", ae.getCause().getMessage());
    }

    @Test
    public void testFatalError() {
        clientService.setThrowFatalError(true);
        basicTest(1, 0, 0);
    }

    @Test
    public void testRetriable() {
        clientService.setThrowRetriableError(true);
        basicTest(0, 1, 0);
    }

    @Test
    public void testInvalidIndexOperation() {
        runner.setProperty(PutElasticsearchJson.INDEX_OP, "not-valid");
        runner.assertNotValid();
        final AssertionError ae = assertThrows(AssertionError.class, () -> runner.run());
        assertEquals(String.format("Processor has 1 validation failures:\n'%s' validated against 'not-valid' is invalid because %s must be Expression Language or one of %s\n",
                PutElasticsearchJson.INDEX_OP.getName(), PutElasticsearchJson.INDEX_OP.getDisplayName(), PutElasticsearchJson.ALLOWED_INDEX_OPERATIONS), ae.getMessage());

        runner.setProperty(PutElasticsearchJson.INDEX_OP, "\\${operation}");
        runner.assertValid();
        runner.enqueue(flowFileContents, Collections.singletonMap("operation", "not-valid2"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
    }

    @Test
    public void testInputRequired() {
        runner.run();
        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
    }

    @Test
    public void testBatchingAndErrorRelationshipNotFoundSuccessful() throws Exception {
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_DOCUMENTS, "true");
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "true");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchJson.NOT_FOUND_IS_SUCCESSFUL, "true");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(sampleErrorResponse));
        final List<String> values = JsonUtils.readListOfMapsAsIndividualJson(JsonUtils.readString(BATCH_WITH_ERROR));
        values.forEach(val -> runner.enqueue(val));
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 4);
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 3);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        MockFlowFile failedDoc = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS).get(0);
        assertTrue(failedDoc.getContent().contains("20abcd"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("mapper_parsing_exception"));

        failedDoc = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS).get(1);
        assertTrue(failedDoc.getContent().contains("213,456.9"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("mapper_parsing_exception"));

        failedDoc = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS).get(2);
        assertTrue(failedDoc.getContent().contains("unit test"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("some_other_exception"));

        assertEquals(3,  runner.getProvenanceEvents().stream().filter(
                e -> ProvenanceEventType.SEND == e.getEventType() && "Elasticsearch _bulk operation error".equals(e.getDetails())).count());
    }

    @Test
    public void testBatchingAndErrorRelationshipNotFoundNotSuccessful() throws Exception {
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_DOCUMENTS, "true");
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "true");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100");
        runner.setProperty(PutElasticsearchJson.NOT_FOUND_IS_SUCCESSFUL, "false");
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_RESPONSES, "true");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(sampleErrorResponse));
        final List<String> values = JsonUtils.readListOfMapsAsIndividualJson(JsonUtils.readString(BATCH_WITH_ERROR));
        values.forEach(val -> runner.enqueue(val));
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 3);
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 4);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 1);

        MockFlowFile failedDoc = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS).get(0);
        assertTrue(failedDoc.getContent().contains("not_found"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("not_found"));

        failedDoc = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS).get(1);
        assertTrue(failedDoc.getContent().contains("20abcd"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("number_format_exception"));

        failedDoc = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS).get(2);
        assertTrue(failedDoc.getContent().contains("213,456.9"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("mapper_parsing_exception"));

        failedDoc = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILED_DOCUMENTS).get(3);
        assertTrue(failedDoc.getContent().contains("unit test"));
        failedDoc.assertAttributeExists("elasticsearch.bulk.error");
        failedDoc.assertAttributeNotExists("elasticsearch.put.error");
        assertTrue(failedDoc.getAttribute("elasticsearch.bulk.error").contains("some_other_exception"));

        final String errorResponses = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_ERROR_RESPONSES).get(0).getContent();
        assertTrue(errorResponses.contains("not_found"));
        assertTrue(errorResponses.contains("For input string: 20abc"));
        assertTrue(errorResponses.contains("For input string: 213,456.9"));
        assertTrue(errorResponses.contains("For input string: unit test"));

        assertEquals(4, runner.getProvenanceEvents().stream().filter( e ->
                ProvenanceEventType.SEND == e.getEventType() && "Elasticsearch _bulk operation error".equals(e.getDetails())).count());
    }

    @Test
    public void testBatchingAndNoErrorOutput() throws Exception {
        runner.setProperty(PutElasticsearchJson.OUTPUT_ERROR_DOCUMENTS, "false");
        runner.setProperty(PutElasticsearchJson.LOG_ERROR_RESPONSES, "false");
        runner.setProperty(PutElasticsearchJson.BATCH_SIZE, "100");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(sampleErrorResponse));
        for (final String val : JsonUtils.readListOfMapsAsIndividualJson(JsonUtils.readString(Paths.get(TEST_DIR, "batchWithoutError.json")))) {
            runner.enqueue(val);
        }

        runner.assertValid();
        runner.run();

        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 5);
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
    }

    @Test
    public void testInvalidInput() {
        runner.enqueue("not-json");
        runner.run();

        runner.assertTransferCount(PutElasticsearchJson.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearchJson.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchJson.REL_FAILED_DOCUMENTS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        final MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_FAILURE).get(0);
        assertTrue(flowFile.getAttribute("elasticsearch.put.error").contains("not"));
    }
}
