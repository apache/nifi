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

import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.Call;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPutElasticsearchHttpRecord {
    private static final int DATE_YEAR = 2018;
    private static final int DATE_MONTH = 12;
    private static final int DATE_DAY = 20;
    private static final int TIME_HOUR = 12;
    private static final int TIME_MINUTE = 55;

    private static final String ISO_DATE = String.format("%d-%d-%d", DATE_YEAR, DATE_MONTH, DATE_DAY);
    private static final String EXPECTED_DATE = String.format("%d/%d/%d", DATE_DAY, DATE_MONTH, DATE_YEAR);
    private static final LocalDateTime LOCAL_DATE_TIME = LocalDateTime.of(DATE_YEAR, DATE_MONTH, DATE_DAY, TIME_HOUR, TIME_MINUTE);
    private static final LocalDate LOCAL_DATE = LocalDate.of(DATE_YEAR, DATE_MONTH, DATE_DAY);
    private static final LocalTime LOCAL_TIME = LocalTime.of(TIME_HOUR, TIME_MINUTE);

    private TestRunner runner;

    @AfterEach
    public void teardown() {
        runner = null;
    }

    @Test
    public void testPutElasticSearchOnTriggerIndex() throws IOException {
        PutElasticsearchHttpRecordTestProcessor processor = new PutElasticsearchHttpRecordTestProcessor(false);
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("h:m a");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("d/M/yyyy h:m a");
        processor.setRecordChecks(record -> {
            assertEquals(1, record.get("id"));
            assertEquals("reç1", record.get("name"));
            assertEquals(101, record.get("code"));
            assertEquals(EXPECTED_DATE, record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        }, record -> {
            assertEquals(2, record.get("id"));
            assertEquals("reç2", record.get("name"));
            assertEquals(102, record.get("code"));
            assertEquals(EXPECTED_DATE, record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        }, record -> {
            assertEquals(3, record.get("id"));
            assertEquals("reç3", record.get("name"));
            assertEquals(103, record.get("code"));
            assertEquals(EXPECTED_DATE, record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        }, record -> {
            assertEquals(4, record.get("id"));
            assertEquals("reç4", record.get("name"));
            assertEquals(104, record.get("code"));
            assertEquals(EXPECTED_DATE, record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        });
        runner = TestRunners.newTestRunner(processor); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.DATE_FORMAT, "d/M/yyyy");
        runner.setProperty(PutElasticsearchHttpRecord.TIME_FORMAT, "h:m a");
        runner.setProperty(PutElasticsearchHttpRecord.TIMESTAMP_FORMAT, "d/M/yyyy h:m a");

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
        out.assertAttributeEquals("record.count", "4");
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertNotNull(provEvents);
        assertEquals(1, provEvents.size());
        assertEquals(ProvenanceEventType.SEND, provEvents.get(0).getEventType());
    }

    @Test
    public void testPutElasticSearchOnTriggerCreate() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "create");

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
        out.assertAttributeEquals("record.count", "4");
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertNotNull(provEvents);
        assertEquals(1, provEvents.size());
        assertEquals(ProvenanceEventType.SEND, provEvents.get(0).getEventType());
    }

    @Test
    public void testPutElasticSearchOnTriggerIndex_withoutType() throws IOException {
        PutElasticsearchHttpRecordTestProcessor processor = new PutElasticsearchHttpRecordTestProcessor(false);
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("h:m a");
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("d/M/yyyy h:m a");
        processor.setRecordChecks(record -> {
            assertEquals(1, record.get("id"));
            assertEquals("reç1", record.get("name"));
            assertEquals(101, record.get("code"));
            assertEquals("20/12/2018", record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        }, record -> {
            assertEquals(2, record.get("id"));
            assertEquals("reç2", record.get("name"));
            assertEquals(102, record.get("code"));
            assertEquals(EXPECTED_DATE, record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        }, record -> {
            assertEquals(3, record.get("id"));
            assertEquals("reç3", record.get("name"));
            assertEquals(103, record.get("code"));
            assertEquals(EXPECTED_DATE, record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        }, record -> {
            assertEquals(4, record.get("id"));
            assertEquals("reç4", record.get("name"));
            assertEquals(104, record.get("code"));
            assertEquals(EXPECTED_DATE, record.get("date"));
            assertEquals(LOCAL_TIME.format(timeFormatter), record.get("time"));
            assertEquals(LOCAL_DATE_TIME.format(dateTimeFormatter), record.get("ts"));
        });
        runner = TestRunners.newTestRunner(processor); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.removeProperty(PutElasticsearchHttpRecord.TYPE);
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.DATE_FORMAT, "d/M/yyyy");
        runner.setProperty(PutElasticsearchHttpRecord.TIME_FORMAT, "h:m a");
        runner.setProperty(PutElasticsearchHttpRecord.TIMESTAMP_FORMAT, "d/M/yyyy h:m a");

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
        out.assertAttributeEquals("record.count", "4");
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertNotNull(provEvents);
        assertEquals(1, provEvents.size());
        assertEquals(ProvenanceEventType.SEND, provEvents.get(0).getEventType());
    }

    @Test
    public void testPutElasticSearchOnTriggerUpdate() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "Update");
        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerUpdate_withoutType() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.removeProperty(PutElasticsearchHttpRecord.TYPE);
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "Update");
        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerDelete() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "DELETE");
        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerDelete_withoutType() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.removeProperty(PutElasticsearchHttpRecord.TYPE);
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "DELETE");
        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerEL() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "${es.url}");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(AbstractElasticsearchHttpProcessor.CONNECT_TIMEOUT, "${connect.timeout}");
        runner.assertValid();

        runner.setVariable("es.url", "http://127.0.0.1:9200");
        runner.setVariable("connect.timeout", "5s");

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchOnTriggerBadIndexOp() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "${no.attr}");
        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_FAILURE).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
    }

    @Test
    public void testPutElasticSearchInvalidConfig() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.assertNotValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "");
        runner.assertNotValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "index");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "create");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "upsert");
        runner.assertNotValid();
    }

    @Test
    public void testPutElasticSearchOnTriggerWithFailures() throws IOException {
        PutElasticsearchHttpRecordTestProcessor processor = new PutElasticsearchHttpRecordTestProcessor(true);
        processor.setStatus(100, "Should fail");
        runner = TestRunners.newTestRunner(processor); // simulate failures
        generateTestData();

        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        runner.clearTransferState();

        processor.setStatus(500, "Should retry");
        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_RETRY, 1);
    }

    @Test
    public void testPutElasticSearchOnTriggerWithConnectException() throws IOException {
        PutElasticsearchHttpRecordTestProcessor processor = new PutElasticsearchHttpRecordTestProcessor(true);
        processor.setStatus(-1, "Connection Exception");
        runner = TestRunners.newTestRunner(processor); // simulate failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);
        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_FAILURE, 1);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithNoIdPath() throws Exception {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false));
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/none"); // Field does not exist

        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 0);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithNoIdField() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(true)); // simulate failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");

        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        MockFlowFile flowFile = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_FAILURE).get(0);
        flowFile.assertAttributeEquals("failure.count", "1");
    }

    @Test
    public void testPutElasticsearchOnTriggerWithIndexFromAttribute() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false));
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "${i}");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "${type}");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");

        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("i", "doc");
            put("type", "status");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        runner.clearTransferState();

        // Now try an empty attribute value, should fail
        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("type", "status");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_FAILURE).get(0);
        assertNotNull(out2);
    }

    @Test
    public void testPutElasticSearchOnTriggerWithInvalidIndexOp() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(false)); // no failures
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.assertNotValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "index");
        runner.assertValid();
        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "create");
        runner.assertValid();

        runner.setProperty(PutElasticsearchHttpRecord.INDEX_OP, "index_fail");
        runner.assertValid();

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_FAILURE).get(0);
        assertNotNull(out);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithNoAtTimestampPath() throws Exception {
        PutElasticsearchHttpRecordTestProcessor processor = new PutElasticsearchHttpRecordTestProcessor(false);
        runner = TestRunners.newTestRunner(processor);
        generateTestData(1);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");

        runner.removeProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP); // no default
        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP_RECORD_PATH, "/none"); // Field does not exist
        processor.setRecordChecks(record -> assertTimestamp(record, null)); // no @timestamp
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        runner.clearTransferState();

        // now add a default @timestamp
        final String timestamp = "2020-11-27T14:37:00.000Z";
        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP, timestamp);
        processor.setRecordChecks(record -> assertTimestamp(record, timestamp)); // @timestamp defaulted
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out2);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithAtTimestampFromAttribute() throws IOException {
        PutElasticsearchHttpRecordTestProcessor processor = new PutElasticsearchHttpRecordTestProcessor(false);
        runner = TestRunners.newTestRunner(processor);
        generateTestData(1);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "${i}");
        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP, "${timestamp}");

        final String timestamp = "2020-11-27T15:10:00.000Z";
        processor.setRecordChecks(record -> assertTimestamp(record, timestamp));
        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("i", "doc");
            put("timestamp", timestamp);
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        runner.clearTransferState();

        // Now try an empty attribute value, should be no timestamp
        processor.setRecordChecks(record -> assertTimestamp(record, null));
        runner.enqueue(new byte[0], new HashMap<String, String>() {{
            put("doc_id", "28039652144");
            put("i", "doc");
        }});
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out2 = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out2);
    }

    @Test
    public void testPutElasticsearchOnTriggerWithAtTimstampPath() throws Exception {
        PutElasticsearchHttpRecordTestProcessor processor = new PutElasticsearchHttpRecordTestProcessor(false);
        DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(RecordFieldType.TIME.getDefaultFormat());
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(RecordFieldType.TIMESTAMP.getDefaultFormat());
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern(RecordFieldType.DATE.getDefaultFormat());
        runner = TestRunners.newTestRunner(processor);
        generateTestData(1);
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");

        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP_RECORD_PATH, "/ts"); // TIMESTAMP
        processor.setRecordChecks(record -> assertTimestamp(record, LOCAL_DATE_TIME.format(dateTimeFormatter)));
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        assertNotNull(runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0));
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP_RECORD_PATH, "/date"); // DATE;
        processor.setRecordChecks(record -> assertTimestamp(record, LOCAL_DATE.format(dateFormatter)));
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        assertNotNull(runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0));
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP_RECORD_PATH, "/time"); // TIME
        processor.setRecordChecks(record -> assertTimestamp(record, LOCAL_TIME.format(timeFormatter)));
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        assertNotNull(runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0));
        runner.clearTransferState();

        // these INT/STRING values might not make sense from an Elasticsearch point of view,
        // but we want to prove we can handle them being selected from the Record
        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP_RECORD_PATH, "/code"); // INT
        processor.setRecordChecks(record -> assertTimestamp(record, 101));
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        assertNotNull(runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0));
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP_RECORD_PATH, "/name"); // STRING
        processor.setRecordChecks(record -> assertTimestamp(record, "reç1"));
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        assertNotNull(runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0));
        runner.clearTransferState();

        runner.setProperty(PutElasticsearchHttpRecord.AT_TIMESTAMP_RECORD_PATH, "/coerce"); // STRING coerced to LONG
        processor.setRecordChecks(record -> assertTimestamp(record, 1000));
        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        assertNotNull(runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0));
        runner.clearTransferState();
    }

    @Test
    public void testPutElasticSearchOnTriggerQueryParameter() throws IOException {
        PutElasticsearchHttpRecordTestProcessor p = new PutElasticsearchHttpRecordTestProcessor(false); // no failures
        p.setExpectedUrl("http://127.0.0.1:9200/_bulk?pipeline=my-pipeline");
        runner = TestRunners.newTestRunner(p);
        generateTestData();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");

        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");

        // Set dynamic property, to be added to the URL as a query parameter
        runner.setProperty("pipeline", "my-pipeline");

        runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "28039652140"));
        runner.run(1, true, true);

        runner.assertAllFlowFilesTransferred(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        assertNotNull(out);
        out.assertAttributeEquals("doc_id", "28039652140");
        List<ProvenanceEventRecord> provEvents = runner.getProvenanceEvents();
        assertNotNull(provEvents);
        assertEquals(1, provEvents.size());
        assertEquals(ProvenanceEventType.SEND, provEvents.get(0).getEventType());
    }

    @Test
    public void testPutElasticsearchOnTriggerFailureWithWriter() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(true)); // simulate failures
        generateTestData(1);
        generateWriter();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");

        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        MockFlowFile flowFileFailure = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_FAILURE).get(0);
        flowFileFailure.assertAttributeEquals("failure.count", "1");
    }

    @Test
    public void testPutElasticsearchOnTriggerFailureWithWriterMultipleRecords() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(2)); // simulate failures
        generateTestData();
        generateWriter();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");

        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        MockFlowFile flowFileSuccess = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        flowFileSuccess.assertAttributeEquals("record.count", "2");
        MockFlowFile flowFileFailure = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_FAILURE).get(0);
        flowFileFailure.assertAttributeEquals("record.count", "2");
        flowFileFailure.assertAttributeEquals("failure.count", "2");

        assertEquals(1, runner.getLogger().getErrorMessages().size());
    }

    @Test
    public void testPutElasticsearchOnTriggerFailureWithWriterMultipleRecordsLogging() throws IOException {
        runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecordTestProcessor(2)); // simulate failures
        generateTestData();
        generateWriter();
        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "http://127.0.0.1:9200");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.LOG_ALL_ERRORS, "true");

        runner.enqueue(new byte[0]);
        runner.run(1, true, true);

        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(PutElasticsearchHttpRecord.REL_FAILURE, 1);
        MockFlowFile flowFileSuccess = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_SUCCESS).get(0);
        flowFileSuccess.assertAttributeEquals("record.count", "2");
        MockFlowFile flowFileFailure = runner.getFlowFilesForRelationship(PutElasticsearchHttpRecord.REL_FAILURE).get(0);
        flowFileFailure.assertAttributeEquals("record.count", "2");
        flowFileFailure.assertAttributeEquals("failure.count", "2");

        assertEquals(2, runner.getLogger().getErrorMessages().size());
    }

    /**
     * A Test class that extends the processor in order to inject/mock behavior
     */
    private static class PutElasticsearchHttpRecordTestProcessor extends PutElasticsearchHttpRecord {
        int numResponseFailures;
        OkHttpClient client;
        int statusCode = 200;
        String statusMessage = "OK";
        String expectedUrl = null;
        Consumer<Map<String, Object>>[] recordChecks;

        PutElasticsearchHttpRecordTestProcessor(boolean responseHasFailures) {
            this.numResponseFailures = responseHasFailures ? 1 : 0;
        }

        PutElasticsearchHttpRecordTestProcessor(int numResponseFailures) {
            this.numResponseFailures = numResponseFailures;
        }

        void setStatus(int code, String message) {
            statusCode = code;
            statusMessage = message;
        }

        void setExpectedUrl(String url) {
            expectedUrl = url;
        }

        @SafeVarargs
        final void setRecordChecks(Consumer<Map<String, Object>>... checks) {
            recordChecks = checks;
        }

        @SuppressWarnings("unchecked")
        @Override
        protected void createElasticsearchClient(ProcessContext context) throws ProcessException {
            client = mock(OkHttpClient.class);

            when(client.newCall(any(Request.class))).thenAnswer(invocationOnMock -> {
                final Call call = mock(Call.class);
                if (statusCode != -1) {
                    Request realRequest = (Request) invocationOnMock.getArguments()[0];
                    assertTrue((expectedUrl == null) || (expectedUrl.equals(realRequest.url().toString())));
                    if (recordChecks != null) {
                        final ObjectMapper mapper = new ObjectMapper();
                        Buffer sink = new Buffer();
                        Objects.requireNonNull(realRequest.body()).writeTo(sink);
                        String line;
                        int recordIndex = 0;
                        boolean content = false;
                        while ((line = sink.readUtf8Line()) != null) {
                            if (content) {
                                content = false;
                                if (recordIndex < recordChecks.length) {
                                    recordChecks[recordIndex++].accept(mapper.readValue(line, Map.class));
                                }
                            } else {
                                content = true;
                            }
                        }
                    }
                    StringBuilder sb = new StringBuilder("{\"took\": 1, \"errors\": \"");
                    sb.append(numResponseFailures > 0);
                    sb.append("\", \"items\": [");
                    for (int i = 0; i < numResponseFailures; i ++) {
                        // This case is for a status code of 200 for the bulk response itself, but with an error (of 400) inside
                        sb.append("{\"index\":{\"_index\":\"doc\",\"_type\":\"status\",\"_id\":\"28039652140\",\"status\":\"400\",");
                        sb.append("\"error\":{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse [gender]\",");
                        sb.append("\"caused_by\":{\"type\":\"json_parse_exception\",\"reason\":\"Unexpected end-of-input in VALUE_STRING\\n at ");
                        sb.append("[Source: org.elasticsearch.common.io.stream.InputStreamStreamInput@1a2e3ac4; line: 1, column: 39]\"}}}},");
                    }
                    sb.append("{\"index\":{\"_index\":\"doc\",\"_type\":\"status\",\"_id\":\"28039652140\",\"status\":");
                    sb.append(statusCode);
                    sb.append(",\"_source\":{\"text\": \"This is a test document\"}}}");

                    sb.append("]}");
                    Response mockResponse = new Response.Builder()
                            .request(realRequest)
                            .protocol(Protocol.HTTP_1_1)
                            .code(statusCode)
                            .message(statusMessage)
                            .body(ResponseBody.create(sb.toString(), MediaType.parse("application/json")))
                            .build();

                    when(call.execute()).thenReturn(mockResponse);
                } else {
                    when(call.execute()).thenThrow(ConnectException.class);
                }
                return call;
            });
        }

        @Override
        protected OkHttpClient getClient() {
            return client;
        }
    }

    @Test
    public void testPutElasticSearchBadHostInEL() {
        final TestRunner runner = TestRunners.newTestRunner(new PutElasticsearchHttpRecord());

        runner.setProperty(AbstractElasticsearchHttpProcessor.ES_URL, "${es.url}");
        runner.setProperty(PutElasticsearchHttpRecord.INDEX, "doc");
        runner.setProperty(PutElasticsearchHttpRecord.TYPE, "status");
        runner.setProperty(PutElasticsearchHttpRecord.ID_RECORD_PATH, "/id");
        assertThrows(AssertionError.class, () -> {
            runner.assertValid();
            runner.enqueue(new byte[0], new HashMap<String, String>() {{
                put("doc_id", "1");
            }});

            runner.enqueue(new byte[0], Collections.singletonMap("doc_id", "1"));

            runner.run();
        });
    }

    private void generateTestData() throws IOException {
        generateTestData(4);
    }

    private void generateTestData(int numRecords) throws IOException {

        final MockRecordParser parser = new MockRecordParser();
        try {
            runner.addControllerService("parser", parser);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(PutElasticsearchHttpRecord.RECORD_READER, "parser");

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.INT);
        parser.addSchemaField("date", RecordFieldType.DATE);
        parser.addSchemaField("time", RecordFieldType.TIME);
        parser.addSchemaField("ts", RecordFieldType.TIMESTAMP);
        parser.addSchemaField("amount", RecordFieldType.DECIMAL);
        parser.addSchemaField("coerce", RecordFieldType.STRING);

        final Date date = Date.valueOf(ISO_DATE);
        final Timestamp timestamp = Timestamp.valueOf(LOCAL_DATE_TIME);
        final Time time = Time.valueOf(LOCAL_TIME);
        for(int i=1; i<=numRecords; i++) {
            parser.addRecord(i, "reç" + i, 100 + i, date, time, timestamp, new BigDecimal(Double.MAX_VALUE).multiply(BigDecimal.TEN), "1000");
        }
    }

    private void generateWriter() throws IOException {
        final MockRecordWriter writer = new MockRecordWriter();
        try {
            runner.addControllerService("writer", writer);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(writer);
        runner.setProperty(PutElasticsearchHttpRecord.RECORD_WRITER, "writer");
    }

    private void assertTimestamp(final Map<String, Object> record, final Object timestamp) {
        if (timestamp == null) {
            assertFalse(record.containsKey("@timestamp"));
        } else {
            assertEquals(timestamp, record.get("@timestamp"));
        }
    }
}
