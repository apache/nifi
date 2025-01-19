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
package org.apache.nifi.processors.geohash;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.MockSchemaRegistry;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GeohashRecordTest {

    private TestRunner runner;

    @BeforeEach
    public void setUp() throws InitializationException {
        ControllerService reader = new JsonTreeReader();
        ControllerService writer = new JsonRecordSetWriter();
        ControllerService registry = new MockSchemaRegistry();
        runner = TestRunners.newTestRunner(GeohashRecord.class);
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.addControllerService("registry", registry);

        try (InputStream is = getClass().getResourceAsStream("/record_schema.avsc")) {
            String raw = IOUtils.toString(is, StandardCharsets.UTF_8);
            RecordSchema parsed = AvroTypeUtil.createSchema(new Schema.Parser().parse(raw));
            ((MockSchemaRegistry) registry).addSchema("record", parsed);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");

        runner.setProperty(GeohashRecord.RECORD_READER, "reader");
        runner.setProperty(GeohashRecord.RECORD_WRITER, "writer");
        runner.enableControllerService(registry);
        runner.enableControllerService(reader);
        runner.enableControllerService(writer);

        runner.setProperty(GeohashRecord.LATITUDE_RECORD_PATH, "/latitude");
        runner.setProperty(GeohashRecord.LONGITUDE_RECORD_PATH, "/longitude");
        runner.setProperty(GeohashRecord.GEOHASH_RECORD_PATH, "/geohash");
        runner.setProperty(GeohashRecord.GEOHASH_FORMAT, GeohashRecord.GeohashFormat.BASE32.toString());
        runner.setProperty(GeohashRecord.GEOHASH_LEVEL, "12");
    }

    private void assertTransfers(String path, int failure, int success, int matched, int notMatched, int original) {
        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "record");
        runner.enqueue(getClass().getResourceAsStream(path), attrs);
        runner.run();

        runner.assertTransferCount(GeohashRecord.REL_FAILURE, failure);
        runner.assertTransferCount(GeohashRecord.REL_SUCCESS, success);
        runner.assertTransferCount(GeohashRecord.REL_MATCHED, matched);
        runner.assertTransferCount(GeohashRecord.REL_NOT_MATCHED, notMatched);
        runner.assertTransferCount(GeohashRecord.REL_ORIGINAL, original);
    }

    @Test
    public void testSkipUnEnrichedEncodeIllegalLatLon() throws Exception {
        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ProcessingMode.ENCODE.toString());
        runner.setProperty(GeohashRecord.ROUTING_STRATEGY, GeohashRecord.RoutingStrategy.SKIP.toString());
        runner.assertValid();

        assertTransfers("/encode-records-with-illegal-arguments.json", 0, 1, 0, 0, 1);

        MockFlowFile outSuccess = runner.getFlowFilesForRelationship(GeohashRecord.REL_SUCCESS).getFirst();
        byte[] raw = runner.getContentAsByteArray(outSuccess);
        String content = new String(raw);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> result = (List<Map<String, Object>>) mapper.readValue(content, List.class);

        assertNotNull(result);
        assertEquals(3, result.size());

        Map<String, Object> element = result.getFirst();
        String geohash = (String) element.get("geohash");
        assertNotNull(geohash);
    }

    @Test
    public void testSkipUnEnrichedEncodeParseFailure() {
        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ProcessingMode.ENCODE.toString());
        runner.setProperty(GeohashRecord.ROUTING_STRATEGY, GeohashRecord.RoutingStrategy.SKIP.toString());
        runner.assertValid();

        assertTransfers("/encode-records-with-incorrect-format.json", 1, 0, 0, 0, 0);
    }

    @Test
    public void testSplitEncodeIllegalLatLon() throws IOException {
        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ProcessingMode.ENCODE.toString());
        runner.setProperty(GeohashRecord.ROUTING_STRATEGY, GeohashRecord.RoutingStrategy.SPLIT.toString());
        runner.assertValid();

        assertTransfers("/encode-records-with-illegal-arguments.json", 0, 0, 1, 1, 1);

        final MockFlowFile outNotMatched = runner.getFlowFilesForRelationship(GeohashRecord.REL_NOT_MATCHED).getFirst();
        final MockFlowFile outMatched = runner.getFlowFilesForRelationship(GeohashRecord.REL_MATCHED).getFirst();

        byte[] rawNotMatched = runner.getContentAsByteArray(outNotMatched);
        byte[] rawMatched = runner.getContentAsByteArray(outMatched);
        String contentNotMatched = new String(rawNotMatched);
        String contentMatched = new String(rawMatched);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> resultNotMatched = (List<Map<String, Object>>) mapper.readValue(contentNotMatched, List.class);
        List<Map<String, Object>> resultMatched = (List<Map<String, Object>>) mapper.readValue(contentMatched, List.class);

        assertNotNull(resultNotMatched);
        assertNotNull(resultMatched);
        assertEquals(2, resultNotMatched.size());
        assertEquals(1, resultMatched.size());

        for (Map<String, Object> elementNotMatched : resultNotMatched) {
            String geohashNotMatched = (String) elementNotMatched.get("geohash");
            assertNull(geohashNotMatched);
        }

        Map<String, Object> elementMatched = resultMatched.getFirst();
        String geohashMatched = (String) elementMatched.get("geohash");
        assertNotNull(geohashMatched);
    }

    @Test
    public void testSplitRemoveEmptyFlowFiles() {
        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ProcessingMode.DECODE.toString());
        runner.setProperty(GeohashRecord.ROUTING_STRATEGY, GeohashRecord.RoutingStrategy.SPLIT.toString());
        runner.assertValid();
        assertTransfers("/decode-record.json", 0, 0, 1, 0, 1);
    }

    @Test
    public void testRequireAllEnrichedSendToSuccess() {
        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ProcessingMode.DECODE.toString());
        runner.setProperty(GeohashRecord.ROUTING_STRATEGY, GeohashRecord.RoutingStrategy.REQUIRE.toString());
        runner.assertValid();

        assertTransfers("/decode-record.json", 0, 1, 0, 0, 1);
    }

    @Test
    public void testRequireAllEnrichedSendToFailure() {
        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ProcessingMode.ENCODE.toString());
        runner.setProperty(GeohashRecord.ROUTING_STRATEGY, GeohashRecord.RoutingStrategy.REQUIRE.toString());
        runner.assertValid();

        assertTransfers("/encode-records-with-illegal-arguments.json", 1, 0, 0, 0, 0);
    }

}