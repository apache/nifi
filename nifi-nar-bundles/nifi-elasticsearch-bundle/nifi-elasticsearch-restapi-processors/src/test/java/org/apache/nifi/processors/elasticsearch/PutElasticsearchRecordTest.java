/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.elasticsearch.IndexOperationRequest;
import org.apache.nifi.elasticsearch.IndexOperationResponse;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.elasticsearch.mock.MockBulkLoadClientService;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PutElasticsearchRecordTest extends AbstractPutElasticsearchTest<PutElasticsearchRecord> {
    private static final int DATE_YEAR = 2020;
    private static final int DATE_MONTH = 11;
    private static final int DATE_DAY = 27;
    private static final int TIME_HOUR = 12;
    private static final int TIME_MINUTE = 55;
    private static final int TIME_SECOND = 23;
    private static final LocalDateTime LOCAL_DATE_TIME = LocalDateTime.of(DATE_YEAR, DATE_MONTH, DATE_DAY, TIME_HOUR, TIME_MINUTE, TIME_SECOND);
    private static final LocalDate LOCAL_DATE = LocalDate.of(DATE_YEAR, DATE_MONTH, DATE_DAY);
    private static final LocalTime LOCAL_TIME = LocalTime.of(TIME_HOUR, TIME_MINUTE, TIME_SECOND);
    private static final String TEST_DIR = "src/test/resources/PutElasticsearchRecordTest";
    private static final String TEST_COMMON_DIR = "src/test/resources/common";
    private static String simpleSchema;
    private MockBulkLoadClientService clientService;
    private MockSchemaRegistry registry;
    private TestRunner runner;

    @Override
    public Class<? extends AbstractPutElasticsearch> getTestProcessor() {
        return PutElasticsearchRecord.class;
    }

    @BeforeAll
    public static void setUpBeforeClass() throws Exception {
        simpleSchema = Files.readString(Paths.get(TEST_DIR, "simpleSchema.json"));
    }

    @BeforeEach
    public void setup() throws Exception {
        clientService = new MockBulkLoadClientService();
        clientService.setResponse(new IndexOperationResponse(1500));
        registry = new MockSchemaRegistry();
        registry.addSchema("simple", AvroTypeUtil.createSchema(new Schema.Parser().parse(simpleSchema)));
        RecordReaderFactory reader = new JsonTreeReader();
        runner = TestRunners.newTestRunner(getTestProcessor());
        runner.addControllerService("registry", registry);
        runner.addControllerService("reader", reader);
        runner.addControllerService("clientService", clientService);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(PutElasticsearchRecord.RECORD_READER, "reader");
        runner.setProperty(PutElasticsearchRecord.INDEX_OP, IndexOperationRequest.Operation.Index.getValue());
        runner.setProperty(PutElasticsearchRecord.INDEX, "test_index");
        runner.setProperty(PutElasticsearchRecord.TYPE, "test_type");
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP, "test_timestamp");
        runner.setProperty(PutElasticsearchRecord.CLIENT_SERVICE, "clientService");
        runner.setProperty(PutElasticsearchRecord.NOT_FOUND_IS_SUCCESSFUL, "true");
        runner.enableControllerService(registry);
        runner.enableControllerService(reader);
        runner.enableControllerService(clientService);

        runner.assertValid();
    }

    public void basicTest(int failure, int retry, int success) {
        Consumer<List<IndexOperationRequest>> consumer = (List<IndexOperationRequest> items) -> {
            long timestampDefaultCount = items.stream().filter(item -> "test_timestamp".equals(item.getFields().get("@timestamp"))).count();
            long indexCount = items.stream().filter(item -> "test_index".equals(item.getIndex())).count();
            long typeCount = items.stream().filter(item -> "test_type".equals(item.getType())).count();
            long opCount = items.stream().filter(item ->  IndexOperationRequest.Operation.Index.equals(item.getOperation())).count();
            long emptyScriptCount = items.stream().filter(item -> item.getScript().isEmpty()).count();
            long falseScriptedUpsertCount = items.stream().filter(item -> !item.isScriptedUpsert()).count();
            long emptyDynamicTemplatesCount = items.stream().filter(item -> item.getDynamicTemplates().isEmpty()).count();
            long emptyHeaderFields = items.stream().filter(item -> item.getHeaderFields().isEmpty()).count();
            assertEquals(2, timestampDefaultCount);
            assertEquals(2, indexCount);
            assertEquals(2, typeCount);
            assertEquals(2, opCount);
            assertEquals(2, emptyScriptCount);
            assertEquals(2, falseScriptedUpsertCount);
            assertEquals(2, emptyDynamicTemplatesCount);
            assertEquals(2, emptyHeaderFields);
        };

        basicTest(failure, retry, success, consumer);
    }

    public void basicTest(int failure, int retry, int success, Consumer<List<IndexOperationRequest>> consumer) {
        clientService.setEvalConsumer(consumer);
        basicTest(failure, retry, success, Collections.singletonMap("schema.name", "simple"));
    }

    public void basicTest(int failure, int retry, int success, Map<String, String> attr) {
        try {
            runner.enqueue(Paths.get(TEST_DIR, "flowFileContentMaps.json"), attr);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        runner.run();

        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, failure);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, retry);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, success);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        if (success > 0) {
            runner.getFlowFilesForRelationship(PutElasticsearchRecord.REL_SUCCESS).forEach(ff -> {
                ff.assertAttributeEquals("elasticsearch.put.success.count", "0");
                ff.assertAttributeEquals("elasticsearch.put.error.count", "0");});

            assertEquals(success,
                    runner.getProvenanceEvents().stream().filter(
                            e -> ProvenanceEventType.SEND.equals(e.getEventType()) && "1 Elasticsearch _bulk operation batch(es) [0 error(s), 0 success(es)]".equals(e.getDetails()))
                            .count());
        }
    }

    @Test
    public void simpleTest() {
        clientService.setEvalParametersConsumer((Map<String, String> params) -> assertTrue(params.isEmpty()));
        basicTest(0, 0, 1);
    }

    @Test
    public void simpleTestCoercedDefaultTimestamp() {
        Consumer<List<IndexOperationRequest>> consumer = (List<IndexOperationRequest> items) ->
                assertEquals(2L, items.stream().filter(item -> Long.valueOf(100).equals(item.getFields().get("@timestamp"))).count());

        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP, "100");
        basicTest(0, 0, 1, consumer);
    }

    @Test
    public void simpleTestWithRequestParametersAndBulkHeaders() {
        runner.setProperty("refresh", "true");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "routing", "/routing");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "version", "${version}");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "empty", "${empty}");
        runner.setProperty("slices", "${slices}");
        runner.setProperty("another", "${blank}");
        runner.setVariable("slices", "auto");
        runner.setVariable("blank", " ");
        runner.setVariable("version", "/version");
        runner.assertValid();

        clientService.setEvalParametersConsumer( (Map<String, String> params) -> {
            assertEquals(2, params.size());
            assertEquals("true", params.get("refresh"));
            assertEquals("auto", params.get("slices"));
        });

        Consumer<List<IndexOperationRequest>> consumer = (List<IndexOperationRequest> items) -> {
            long headerFieldsCount = items.stream().filter(item -> !item.getHeaderFields().isEmpty()).count();
            long routingCount = items.stream().filter(item -> "1".equals(item.getHeaderFields().get("routing"))).count();
            long versionCount = items.stream().filter(item -> "external".equals(item.getHeaderFields().get("version"))).count();
            assertEquals(2, headerFieldsCount);
            assertEquals(1, routingCount);
            assertEquals(1, versionCount);
        };

        basicTest(0, 0, 1, consumer);
    }

    @Test
    public void simpleTestWithRequestParametersAndBulkHeadersFlowFileEL() {
        runner.setProperty("refresh", "true");
        runner.setProperty("slices", "${slices}");
        runner.setVariable("blank", " ");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "routing", "/routing");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "version", "${version}");
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "empty", "${empty}");
        runner.assertValid();

        clientService.setEvalParametersConsumer((Map<String, String> params) -> {
            assertEquals(2, params.size());
            assertEquals("true", params.get("refresh"));
            assertEquals("auto", params.get("slices"));
        });

        clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long headerFieldsCount = items.stream().filter(item -> !item.getHeaderFields().isEmpty()).count();
            long routingCount = items.stream().filter(item -> "1".equals(item.getHeaderFields().get("routing"))).count();
            long versionCount = items.stream().filter(item -> "external".equals(item.getHeaderFields().get("version"))).count();
            assertEquals(2, headerFieldsCount);
            assertEquals(1, routingCount);
            assertEquals(1, versionCount);
        });

        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("schema.name", "simple");
        attributes.put("version", "/version");
        attributes.put("slices", "auto");
        attributes.put("blank", " ");
        basicTest(0, 0, 1, attributes);
    }

    @Test
    public void simpleTestWithMockReader() throws Exception{
        MockRecordParser mockReader = new MockRecordParser();
        mockReader.addSchemaField("msg", RecordFieldType.STRING);
        mockReader.addSchemaField("from", RecordFieldType.STRING);
        mockReader.addRecord("foo", "bar");
        mockReader.addRecord("biz", "baz");

        runner.addControllerService("mockReader", mockReader);
        runner.setProperty(PutElasticsearchRecord.RECORD_READER, "mockReader");
        runner.enableControllerService(mockReader);

        basicTest(0, 0, 1);
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
    public void testRecordPathFeatures() throws Exception{
        String newSchema = Files.readString(Paths.get(TEST_DIR, "recordPathTestSchema.json"));
        Map<String, Object> script =
                JsonUtils.readMap(Files.readString(Paths.get(TEST_DIR, "script.json")));
        Map<String, Object> dynamicTemplates =
                JsonUtils.readMap(Files.readString(Paths.get(TEST_COMMON_DIR, "dynamicTemplates.json")));
        clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long a = items.stream().filter(item ->  "bulk_a".equals(item.getIndex())).count();
            long b = items.stream().filter(item ->  "bulk_b".equals(item.getIndex())).count();
            long index = items.stream().filter(item ->  IndexOperationRequest.Operation.Index.equals(item.getOperation())).count();
            long create = items.stream().filter(item ->  IndexOperationRequest.Operation.Create.equals(item.getOperation())).count();
            long msg = items.stream().filter(item -> "Hello".equals(item.getFields().get("msg"))).count();
            long empties = items.stream().filter(item -> ("".equals(item.getFields().get("msg")))).count();
            long nulls = items.stream().filter(item -> null == item.getFields().get("msg")).count();
            long timestamp = items.stream().filter(item ->
                    LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIMESTAMP.getDefaultFormat())).equals(item.getFields().get("@timestamp"))).count();
            long timestampDefault = items.stream().filter(item ->  "test_timestamp".equals(item.getFields().get("@timestamp"))).count();
            long ts = items.stream().filter(item ->  item.getFields().get("ts") != null).count();
            long id = items.stream().filter(item ->  item.getFields().get("id") != null).count();
            long emptyScript = items.stream().filter(item ->  item.getScript().isEmpty()).count();
            long falseScriptedUpsertCount = items.stream().filter(item -> !item.isScriptedUpsert()).count();
            long trueScriptedUpsertCount = items.stream().filter(IndexOperationRequest::isScriptedUpsert).count();
            long s = items.stream().filter(item ->  script.equals(item.getScript())).count();
            long emptyDynamicTemplates = items.stream().filter(item ->  item.getDynamicTemplates().isEmpty()).count();
            long dt = items.stream().filter(item ->  dynamicTemplates.equals(item.getDynamicTemplates())).count();
            items.forEach(item -> {
                assertNotNull(item.getId());
                assertTrue(item.getId().startsWith("rec-"));
                assertEquals("message", item.getType()); });

            assertEquals(3, a);
            assertEquals(3, b);
            assertEquals(5, index);
            assertEquals(1, create);
            assertEquals(4, msg);
            assertEquals(1, empties);
            assertEquals(1, nulls);
            assertEquals(1, timestamp);
            assertEquals(5, timestampDefault);
            assertEquals(0, ts);
            assertEquals(0, id);
            assertEquals(5, emptyScript);
            assertEquals(5, falseScriptedUpsertCount);
            assertEquals(1, trueScriptedUpsertCount);
            assertEquals(1, s);
            assertEquals(5, emptyDynamicTemplates);
            assertEquals(1, dt);
        });

        registry.addSchema("recordPathTest", AvroTypeUtil.createSchema(new Schema.Parser().parse(newSchema)));

        runner.removeProperty(PutElasticsearchRecord.INDEX_OP);
        runner.setProperty(PutElasticsearchRecord.INDEX_OP_RECORD_PATH, "/op");
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchRecord.INDEX_RECORD_PATH, "/index");
        runner.setProperty(PutElasticsearchRecord.TYPE_RECORD_PATH, "/type");
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/ts");
        runner.setProperty(PutElasticsearchRecord.SCRIPT_RECORD_PATH, "/script");
        runner.setProperty(PutElasticsearchRecord.SCRIPTED_UPSERT_RECORD_PATH, "/scripted_upsert");
        runner.setProperty(PutElasticsearchRecord.DYNAMIC_TEMPLATES_RECORD_PATH, "/dynamic_templates");
        runner.enqueue(Paths.get(TEST_DIR, "1_flowFileContents.json"), Collections.singletonMap("schema.name", "recordPathTest"));

        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

       clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long testTypeCount = items.stream().filter(item ->  "test_type".equals(item.getType())).count();
            long messageTypeCount = items.stream().filter(item ->  "message".equals(item.getType())).count();
            long testIndexCount = items.stream().filter(item ->  "test_index".equals(item.getIndex())).count();
            long bulkIndexCount = items.stream().filter(item ->  item.getIndex().startsWith("bulk_")).count();
            long indexOperationCount = items.stream().filter(item ->  IndexOperationRequest.Operation.Index.equals(item.getOperation())).count();
            long updateOperationCount = items.stream().filter(item ->  IndexOperationRequest.Operation.Update.equals(item.getOperation())).count();
            long timestampCount = items.stream().filter(item ->
                    LOCAL_DATE.format(DateTimeFormatter.ofPattern("dd/MM/yyyy")).equals(item.getFields().get("@timestamp"))).count();
            long dateCount = items.stream().filter(item ->  item.getFields().get("date") != null).count();
            long idCount = items.stream().filter(item ->  item.getFields().get("id") != null).count();
            long defaultCoercedTimestampCount = items.stream().filter(item -> Long.valueOf(100).equals(item.getFields().get("@timestamp"))).count();
            long emptyScriptCount = items.stream().filter(item ->  item.getScript().isEmpty()).count();
            long scriptCount = items.stream().filter(item ->  script.equals(item.getScript())).count();
            assertEquals(5, testTypeCount);
            assertEquals(1, messageTypeCount);
            assertEquals(5, testIndexCount);
            assertEquals(1, bulkIndexCount);
            assertEquals(5, indexOperationCount);
            assertEquals(1, updateOperationCount);
            assertEquals(1, timestampCount);
            assertEquals(5, defaultCoercedTimestampCount);
            assertEquals(1, dateCount);
            assertEquals(6, idCount);
            assertEquals(5, emptyScriptCount);
            assertEquals(1, scriptCount);
        });

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "${operation}");
        runner.setProperty(PutElasticsearchRecord.RETAIN_ID_FIELD, "true");
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP, "100");
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/date");
        runner.setProperty(PutElasticsearchRecord.DATE_FORMAT, "dd/MM/yyyy");
        runner.setProperty(PutElasticsearchRecord.RETAIN_AT_TIMESTAMP_FIELD, "true");
        runner.setProperty(PutElasticsearchRecord.SCRIPT_RECORD_PATH, "/script_record");
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("schema.name", "recordPathTest");
        attributes.put("operation", "index");
        runner.enqueue(Paths.get(TEST_DIR, "2_flowFileContents.json"), attributes);
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

        clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long nullTypeCount = items.stream().filter(item -> item.getType() == null).count();
            long messageTypeCount = items.stream().filter(item -> "message".equals(item.getType())).count();
            long nullIdCount = items.stream().filter(item -> item.getId() == null).count();
            long recIdCount = items.stream().filter(item -> StringUtils.startsWith(item.getId(), "rec-")).count();
            long timestampCount = items.stream().filter(item ->
                    LOCAL_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIME.getDefaultFormat())).equals(item.getFields().get("@timestamp"))).count();
            assertEquals(5, nullTypeCount, "null type");
            assertEquals(1, messageTypeCount, "message type");
            assertEquals(2, nullIdCount, "null id");
            assertEquals(4, recIdCount, "rec- id");
            assertEquals(1, timestampCount, "@timestamp");
        });

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "index");
        runner.removeProperty(PutElasticsearchRecord.AT_TIMESTAMP);
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/time");
        runner.removeProperty(PutElasticsearchRecord.TYPE);
        runner.enqueue(Paths.get(TEST_DIR, "3_flowFileContents.json"), Collections.singletonMap("schema.name", "recordPathTest"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

         clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long index = items.stream().filter(item ->  IndexOperationRequest.Operation.Index.equals(item.getOperation())).count();
            long create = items.stream().filter(item ->  IndexOperationRequest.Operation.Create.equals(item.getOperation())).count();
            long update = items.stream().filter(item ->  IndexOperationRequest.Operation.Update.equals(item.getOperation())).count();
            long upsert = items.stream().filter(item ->  IndexOperationRequest.Operation.Upsert.equals(item.getOperation())).count();
            long delete = items.stream().filter(item ->  IndexOperationRequest.Operation.Delete.equals(item.getOperation())).count();
            long timestampCount = items.stream().filter(item -> Long.valueOf(101).equals(item.getFields().get("@timestamp"))).count();
            long noTimestampCount = items.stream().filter(item -> !item.getFields().containsKey("@timestamp") ).count();
            assertEquals(1, index);
            assertEquals(2, create);
            assertEquals(1, update);
            assertEquals(1, upsert);
            assertEquals(1, delete);
            assertEquals(1, timestampCount);
            assertEquals(5, noTimestampCount);
        });

        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/code");
        runner.enqueue(Paths.get(TEST_DIR, "4_flowFileContents.json"), Collections.singletonMap("schema.name", "recordPathTest"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

        clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long timestampCount = items.stream().filter(item ->  "Hello".equals(item.getFields().get("@timestamp"))).count();
            assertEquals(1, timestampCount);
        });

        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/msg");
        runner.enqueue(Paths.get(TEST_DIR, "5_flowFileContents.json"), Collections.singletonMap("schema.name", "recordPathTest"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

       clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long nullIdCount = items.stream().filter(item ->  item.getId() == null).count();
            long noTimestampCount = items.stream().filter(item -> !item.getFields().containsKey("@timestamp")).count();
            assertEquals(1, nullIdCount);
            assertEquals(1, noTimestampCount);
        });


        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "${id_not_exist}");
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "${not_exist}");
        runner.enqueue(Paths.get(TEST_DIR, "6_flowFileContents.json"), Collections.singletonMap("schema.name", "recordPathTest"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

        clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long nullIdCount = items.stream().filter(item ->  item.getId() == null).count();
            long noTimestampCount = items.stream().filter(item -> !item.getFields().containsKey("@timestamp")).count();
            assertEquals(1, nullIdCount);
            assertEquals(1, noTimestampCount);
        });

        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "${will_be_empty}");
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "${will_be_empty}");
        attributes = new LinkedHashMap<>();
        attributes.put("schema.name", "recordPathTest");
        attributes.put("will_be_empty", "/empty");
        runner.enqueue(Paths.get(TEST_DIR, "7_flowFileContents.json"), attributes);
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

        runner.enqueue(Paths.get(TEST_DIR, "8_flowFileContents.json"), Collections.singletonMap("schema.name", "recordPathTest"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
        MockFlowFile failure = runner.getFlowFilesForRelationship(PutElasticsearchRecord.REL_FAILURE).get(0);
        failure.assertAttributeEquals("elasticsearch.put.error", String.format("Field referenced by %s must be Map-type compatible or a String parsable into a JSON Object", "/dynamic_templates"));

        runner.clearTransferState();

        clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long idNotNull = items.stream().filter(item ->  item.getId() != null).count();
            long opIndex = items.stream().filter(item ->  IndexOperationRequest.Operation.Index.equals(item.getOperation())).count();
            long opCreate = items.stream().filter(item ->  IndexOperationRequest.Operation.Create.equals(item.getOperation())).count();
            long indexA = items.stream().filter(item ->  "bulk_a".equals(item.getIndex())).count();
            long indexC = items.stream().filter(item ->  "bulk_c".equals(item.getIndex())).count();
            long typeMessage = items.stream().filter(item ->  "message".equals(item.getType())).count();
            long typeBlah = items.stream().filter(item -> "blah".equals(item.getType())).count();
            assertEquals(4, idNotNull);
            assertEquals(3, opIndex);
            assertEquals(3, opCreate);
            assertEquals(4, indexA);
            assertEquals(2, indexC);
            assertEquals(4, typeMessage);
            assertEquals(2, typeBlah);
        });

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "index");
        runner.setProperty(PutElasticsearchRecord.INDEX_OP_RECORD_PATH, "/op");
        runner.setProperty(PutElasticsearchRecord.TYPE, "blah");
        runner.setProperty(PutElasticsearchRecord.TYPE_RECORD_PATH, "/type");
        runner.setProperty(PutElasticsearchRecord.INDEX, "bulk_c");
        runner.setProperty(PutElasticsearchRecord.INDEX_RECORD_PATH, "/index");
        runner.setProperty(PutElasticsearchRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchRecord.RETAIN_ID_FIELD, "false");
        runner.removeProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH);
        runner.enqueue(Paths.get(TEST_DIR, "9_flowFileContents.json"), Collections.singletonMap("schema.name", "recordPathTest"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
    }

    @Test
    public void testDateTimeFormatting() throws Exception{
        clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            long msg = items.stream().filter(item ->  (item.getFields().get("msg") != null)).count();
            long timestamp = items.stream().filter(item ->
                    LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIMESTAMP.getDefaultFormat())).equals(item.getFields().get("ts"))).count(); // "yyyy-MM-dd HH:mm:ss"
            long date = items.stream().filter(item ->
                    LOCAL_DATE.format(DateTimeFormatter.ofPattern(RecordFieldType.DATE.getDefaultFormat())).equals(item.getFields().get("date"))).count(); // "yyyy-MM-dd"
            long time = items.stream().filter(item ->
                    LOCAL_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIME.getDefaultFormat())).equals(item.getFields().get("time"))).count(); // "HH:mm:ss"
            long choiceTs = items.stream().filter(item ->
                    LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern(RecordFieldType.TIMESTAMP.getDefaultFormat())).equals(item.getFields().get("choice_ts"))).count();
            long choiceNotTs = items.stream().filter(item ->  "not-timestamp".equals(item.getFields().get("choice_ts"))).count();
            long atTimestampDefault = items.stream().filter(item ->  "test_timestamp".equals(item.getFields().get("@timestamp"))).count();
            long tsNull = items.stream().filter(item -> item.getFields().get("ts") == null).count();
            long dateNull = items.stream().filter(item ->  item.getFields().get("date") == null).count();
            long timeNull = items.stream().filter(item ->  item.getFields().get("time") == null).count();
            long choiceTsNull = items.stream().filter(item ->  item.getFields().get("choice_ts") == null).count();
            assertEquals(5, msg);
            assertEquals(2, timestamp);
            assertEquals(2, date);
            assertEquals(2, time);
            assertEquals(1, choiceTs);
            assertEquals(1, choiceNotTs);
            assertEquals(3, tsNull);
            assertEquals(3, dateNull);
            assertEquals(3, timeNull);
            assertEquals(3, choiceTsNull);
            assertEquals(5, atTimestampDefault);
        });

        String newSchema = Files.readString(Paths.get(TEST_DIR, "dateTimeFormattingTestSchema.json"));
        registry.addSchema("dateTimeFormattingTest", AvroTypeUtil.createSchema(new Schema.Parser().parse(newSchema)));
        Path flowFileContents = Paths.get(TEST_DIR, "10_flowFileContents.json");
        runner.enqueue(flowFileContents, Collections.singletonMap("schema.name", "dateTimeFormattingTest"));

        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.clearTransferState();

         clientService.setEvalConsumer((List<IndexOperationRequest> items) -> {
            String timestampOutput = LOCAL_DATE_TIME.format(DateTimeFormatter.ofPattern("yy MMM D H"));
            long msg = items.stream().filter(item -> (item.getFields().get("msg") != null)).count();
            long timestamp = items.stream().filter(item -> timestampOutput.equals(item.getFields().get("ts"))).count();
            long date = items.stream().filter(item ->
                    LOCAL_DATE.format(DateTimeFormatter.ofPattern("dd/MM/yyyy")).equals(item.getFields().get("date"))).count();
            long time = items.stream().filter(item ->
                            // converted to a Long because the output is completely numerical
                            Long.valueOf(LOCAL_TIME.format(DateTimeFormatter.ofPattern("HHmmss"))).equals(item.getFields().get("time"))
                    ).count();
            long choiceTs = items.stream().filter(item -> timestampOutput.equals(item.getFields().get("choice_ts"))).count();
            long choiceNotTs = items.stream().filter(item -> "not-timestamp".equals(item.getFields().get("choice_ts"))).count();
            long atTimestampDefault = items.stream().filter(item -> "test_timestamp".equals(item.getFields().get("@timestamp"))).count();
            long atTimestamp = items.stream().filter(item -> timestampOutput.equals(item.getFields().get("@timestamp"))).count();
            long tsNull = items.stream().filter(item -> item.getFields().get("ts") == null).count();
            long dateNull = items.stream().filter(item -> item.getFields().get("date") == null).count();
            long timeNull = items.stream().filter(item -> item.getFields().get("time") == null).count();
            long choiceTsNull = items.stream().filter(item -> item.getFields().get("choice_ts") == null).count();
            assertEquals(5, msg);
            assertEquals(2, timestamp);
            assertEquals(2, date);
            assertEquals(2, time);
            assertEquals(1, choiceTs);
            assertEquals(1, choiceNotTs);
            assertEquals(3, tsNull);
            assertEquals(3, dateNull);
            assertEquals(3, timeNull);
            assertEquals(3, choiceTsNull);
            assertEquals(2, atTimestamp);
            assertEquals(3, atTimestampDefault);
        });

        runner.setProperty(PutElasticsearchRecord.TIMESTAMP_FORMAT, "yy MMM D H");
        runner.setProperty(PutElasticsearchRecord.DATE_FORMAT, "dd/MM/yyyy");
        runner.setProperty(PutElasticsearchRecord.TIME_FORMAT, "HHmmss");
        runner.setProperty(PutElasticsearchRecord.AT_TIMESTAMP_RECORD_PATH, "/ts");
        runner.setProperty(PutElasticsearchRecord.RETAIN_AT_TIMESTAMP_FIELD, "true");

        runner.enqueue(flowFileContents, Collections.singletonMap("schema.name", "dateTimeFormattingTest"));

        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
    }

    @Test
    public void testInvalidIndexOperation() throws Exception{
        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "not-valid");
        runner.assertNotValid();
        final AssertionError ae = assertThrows(AssertionError.class, runner::run);
        assertEquals(String.format("Processor has 1 validation failures:\n'%s' validated against 'not-valid' is invalid because %s must be Expression Language or one of %s\n",
                PutElasticsearchRecord.INDEX_OP.getName(), PutElasticsearchRecord.INDEX_OP.getDisplayName(), PutElasticsearchRecord.ALLOWED_INDEX_OPERATIONS), ae.getMessage());

        runner.setProperty(PutElasticsearchRecord.INDEX_OP, "${operation}");
        runner.assertValid();
        runner.enqueue(Paths.get(TEST_DIR, "flowFileContentMaps.json"), Collections.singletonMap("operation", "not-valid2"));
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
    }

    @Test
    public void testInputRequired() {
        runner.run();
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);
    }

    @Test
    public void testErrorRelationship() throws Exception {
        JsonRecordSetWriter writer = new JsonRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.enableControllerService(writer);
        runner.setProperty(PutElasticsearchRecord.RESULT_RECORD_WRITER, "writer");
        runner.setProperty(PutElasticsearchRecord.NOT_FOUND_IS_SUCCESSFUL, "true");
        clientService.setResponse(IndexOperationResponse.fromJsonResponse(Files.readString(Paths.get(TEST_COMMON_DIR, "sampleErrorResponse.json"))));

        String newSchema = Files.readString(Paths.get(TEST_DIR, "errorTestSchema.json"));
        registry.addSchema("errorTest", AvroTypeUtil.createSchema(new Schema.Parser().parse(newSchema)));
        Path values = Paths.get(TEST_DIR, "values.json");
        runner.enqueue(values, Collections.singletonMap("schema.name", "errorTest"));
        runner.setProperty(PutElasticsearchRecord.LOG_ERROR_RESPONSES, "true");
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.getFlowFilesForRelationship(PutElasticsearchRecord.REL_FAILED_RECORDS).get(0).assertAttributeEquals(PutElasticsearchRecord.ATTR_RECORD_COUNT, "1");
        runner.getFlowFilesForRelationship(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS).get(0).assertAttributeEquals(PutElasticsearchRecord.ATTR_RECORD_COUNT, "4");

        assertEquals(1,
                runner.getProvenanceEvents().stream().filter(e -> ProvenanceEventType.SEND.equals(e.getEventType())
                        && "1 Elasticsearch _bulk operation batch(es) [1 error(s), 4 success(es)]".equals(e.getDetails())
                ).count());

        runner.clearTransferState();
        runner.clearProvenanceEvents();

        runner.setProperty(PutElasticsearchRecord.NOT_FOUND_IS_SUCCESSFUL, "false");
        runner.enqueue(values, Collections.singletonMap("schema.name", "errorTest"));
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 0);

        runner.getFlowFilesForRelationship(PutElasticsearchRecord.REL_FAILED_RECORDS).get(0).assertAttributeEquals(PutElasticsearchRecord.ATTR_RECORD_COUNT, "2");
        runner.getFlowFilesForRelationship(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS).get(0).assertAttributeEquals(PutElasticsearchRecord.ATTR_RECORD_COUNT, "3");

        assertEquals(1,
                runner.getProvenanceEvents().stream().filter(
                    e -> ProvenanceEventType.SEND.equals(e.getEventType()) && "1 Elasticsearch _bulk operation batch(es) [2 error(s), 3 success(es)]".equals(e.getDetails())
                ).count());

        runner.clearTransferState();
        runner.clearProvenanceEvents();

        // errors still counted/logged even if not outputting to the error relationship
        runner.removeProperty(PutElasticsearchRecord.RESULT_RECORD_WRITER);
        runner.setProperty(PutElasticsearchRecord.OUTPUT_ERROR_RESPONSES, "true");
        runner.enqueue(values, Collections.singletonMap("schema.name", "errorTest"));
        runner.assertValid();
        runner.run();

        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILURE, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_RETRY, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_FAILED_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_SUCCESSFUL_RECORDS, 0);
        runner.assertTransferCount(PutElasticsearchRecord.REL_ERROR_RESPONSES, 1);

        final String errorResponses = runner.getFlowFilesForRelationship(PutElasticsearchJson.REL_ERROR_RESPONSES).get(0).getContent();
        //assertThat(errorResponses, containsString("not_found"))
        assertTrue(errorResponses.contains("not_found"));
        //assertThat(errorResponses, containsString("For input string: 20abc"))
        assertTrue(errorResponses.contains("For input string: 20abc"));

        assertEquals(1,
                runner.getProvenanceEvents().stream().filter(
                        e -> ProvenanceEventType.SEND.equals(e.getEventType()) && "1 Elasticsearch _bulk operation batch(es) [2 error(s), 3 success(es)]".equals(e.getDetails())
                ).count());
    }

    @Test
    public void testInvalidBulkHeaderProperty() {
        runner.assertValid();
        runner.setProperty(AbstractPutElasticsearch.BULK_HEADER_PREFIX + "routing", "not-record-path");
        runner.assertNotValid();
    }
}
