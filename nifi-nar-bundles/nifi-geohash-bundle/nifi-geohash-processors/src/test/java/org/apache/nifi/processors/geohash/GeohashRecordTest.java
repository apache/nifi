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
import org.codehaus.jackson.map.ObjectMapper;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class GeohashRecordTest {

    private TestRunner runner = TestRunners.newTestRunner(GeohashRecord.class);

    @Before
    public void setUp() throws InitializationException {
        ControllerService reader = new JsonTreeReader();
        ControllerService writer = new JsonRecordSetWriter();
        ControllerService registry = new MockSchemaRegistry();
        runner.addControllerService("reader", reader);
        runner.addControllerService("writer", writer);
        runner.addControllerService("registry", registry);

        try (InputStream is = getClass().getResourceAsStream("/record_schema.avsc")) {
            String raw = IOUtils.toString(is, "UTF-8");
            RecordSchema parsed = AvroTypeUtil.createSchema(new Schema.Parser().parse(raw));
            ((MockSchemaRegistry) registry).addSchema("record", parsed);

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_NAME_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_REGISTRY, "registry");

        runner.setProperty(GeohashRecord.GEOHASH_RECORD_PATH, "/geohash");
        runner.setProperty(GeohashRecord.RECORD_READER, "reader");
        runner.setProperty(GeohashRecord.RECORD_WRITER, "writer");
        runner.enableControllerService(registry);
        runner.enableControllerService(reader);
        runner.enableControllerService(writer);

        runner.setProperty(GeohashRecord.LATITUDE_RECORD_PATH, "/latitude");
        runner.setProperty(GeohashRecord.LONGITUDE_RECORD_PATH, "/longitude");
    }
    @Test
    public void testDecodeSendToSuccess() throws Exception {

        runner.setProperty(GeohashRecord.MODE, GeohashRecord.DECODE_MODE);
        runner.setProperty(GeohashRecord.GEOHASH_FORMAT, GeohashRecord.BASE32);
        runner.assertValid();

        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "record");
        runner.enqueue(getClass().getResourceAsStream("/record_decode.json"), attrs);
        runner.run();

        runner.assertTransferCount(GeohashRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(GeohashRecord.REL_FAILURE, 0);

        MockFlowFile ff = runner.getFlowFilesForRelationship(GeohashRecord.REL_SUCCESS).get(0);
        byte[] raw = runner.getContentAsByteArray(ff);
        String content = new String(raw);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> result = (List<Map<String, Object>>)mapper.readValue(content, List.class);

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());

        Map<String, Object> element = result.get(0);
        Double latitude = (Double)element.get("latitude");
        Double longitude = (Double) element.get("longitude");

        Assert.assertNotNull(latitude);
        Assert.assertNotNull(longitude);
    }

    @Test
    public void testEncodeSendToSuccess() throws Exception {

        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ENCODE_MODE);
        runner.setProperty(GeohashRecord.GEOHASH_FORMAT, GeohashRecord.BASE32);
        runner.setProperty(GeohashRecord.GEOHASH_LEVEL, "12");
        runner.assertValid();

        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "record");
        runner.enqueue(getClass().getResourceAsStream("/record_encode.json"), attrs);
        runner.run();

        runner.assertTransferCount(GeohashRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(GeohashRecord.REL_FAILURE, 0);

        MockFlowFile ff = runner.getFlowFilesForRelationship(GeohashRecord.REL_SUCCESS).get(0);
        byte[] raw = runner.getContentAsByteArray(ff);
        String content = new String(raw);
        ObjectMapper mapper = new ObjectMapper();
        List<Map<String, Object>> result = (List<Map<String, Object>>)mapper.readValue(content, List.class);

        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.size());

        Map<String, Object> element = result.get(0);
        String geohash = (String)element.get("geohash");

        Assert.assertNotNull(geohash);

    }

    @Test
    public void testDecodeSendToFailure() {

        runner.setProperty(GeohashRecord.MODE, GeohashRecord.DECODE_MODE);
        runner.setProperty(GeohashRecord.GEOHASH_FORMAT, GeohashRecord.BASE32);
        runner.assertValid();

        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "record");
        runner.enqueue(getClass().getResourceAsStream("/record_encode.json"), attrs);
        runner.run();

        runner.assertTransferCount(GeohashRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(GeohashRecord.REL_FAILURE, 1);
    }
    @Test
    public void testEncodeSendToFailure() {

        runner.setProperty(GeohashRecord.MODE, GeohashRecord.ENCODE_MODE);
        runner.setProperty(GeohashRecord.GEOHASH_FORMAT, GeohashRecord.BASE32);
        runner.setProperty(GeohashRecord.GEOHASH_LEVEL, "12");
        runner.assertValid();

        Map<String, String> attrs = new HashMap<>();
        attrs.put("schema.name", "record");
        runner.enqueue(getClass().getResourceAsStream("/record_decode.json"), attrs);
        runner.run();

        runner.assertTransferCount(GeohashRecord.REL_SUCCESS, 0);
        runner.assertTransferCount(GeohashRecord.REL_FAILURE, 1);
    }

}
