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
package org.apache.nifi.processors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.processors.model.IoTDBSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.model.ValidationResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AbstractIoTDBUT {
    private static TestAbstractIoTDBProcessor processor;

    @BeforeAll
    public static void init() {
        processor = new TestAbstractIoTDBProcessor();
    }

    @Test
    public void testValidateSchemaAttribute() {
        // normal schema
        String schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";

        ValidationResult result = processor.validateSchemaAttribute(schemaAttribute);
        Assertions.assertTrue(result.getKey());
        Assertions.assertEquals(null, result.getValue());

        // schema with wrong field
        schemaAttribute =
                "{\n"
                        + "\t\"time\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        result = processor.validateSchemaAttribute(schemaAttribute);
        String exceptedMsg = "The JSON of schema must contain `timeType` and `fields`";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());

        // schema with wrong time type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"int\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        result = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg =
                "Unknown `timeType`: int, there are only two options `LONG` and `STRING` for this property";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());

        // schema without tsName
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        result = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "`tsName` or `dataType` has not been set";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());

        // schema without data type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        result = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "`tsName` or `dataType` has not been set";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());

        // tsName not start with root
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        ;
        result = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "The tsName `test_sg.test_d1.s1` is not start with 'root.'";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());

        // schema with wrong data type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";

        result = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg =
                "Unknown `dataType`: INT. The supported dataTypes are [FLOAT, INT64, INT32, TEXT, DOUBLE, BOOLEAN]";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());

        // schema with wrong key
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encode\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";

        result = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg = "Unknown property or properties: [encode]";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());

        // schema with wrong compression type
        schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\",\n"
                        + "\t\t\"compressionType\": \"ZIP\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\",\n"
                        + "\t\t\"compressionType\": \"GZIP\"\n"
                        + "\t}]\n"
                        + "}";

        result = processor.validateSchemaAttribute(schemaAttribute);
        exceptedMsg =
                "Unknown `compressionType`: ZIP, The supported compressionType are [LZO, PAA, SDT, UNCOMPRESSED, PLA, LZ4, GZIP, SNAPPY]";

        Assertions.assertEquals(false, result.getKey());
        Assertions.assertEquals(exceptedMsg, result.getValue());
    }

    @Test
    public void testParseSchema() {
        ArrayList<String> filedNames =
                new ArrayList<String>() {
                    {
                        add("root.sg1.d1.s1");
                        add("root.sg1.d1.s2");
                        add("root.sg1.d2.s1");
                    }
                };
        Map<String, List<String>> deviceMeasurementMap = processor.parseSchema(filedNames);
        HashMap<String, List<String>> exceptedMap =
                new HashMap<String, List<String>>() {
                    {
                        put(
                                "root.sg1.d1",
                                new ArrayList<String>() {
                                    {
                                        add("s1");
                                        add("s2");
                                    }
                                });
                        put(
                                "root.sg1.d2",
                                new ArrayList<String>() {
                                    {
                                        add("s1");
                                    }
                                });
                    }
                };
        Assertions.assertEquals(exceptedMap, deviceMeasurementMap);
    }

    @Test
    public void testGenerateTablet() throws JsonProcessingException {
        String schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [{\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s1\",\n"
                        + "\t\t\"dataType\": \"INT32\",\n"
                        + "\t\t\"encoding\": \"RLE\"\n"
                        + "\t}, {\n"
                        + "\t\t\"tsName\": \"root.test_sg.test_d1.s2\",\n"
                        + "\t\t\"dataType\": \"DOUBLE\",\n"
                        + "\t\t\"encoding\": \"PLAIN\"\n"
                        + "\t}]\n"
                        + "}";
        IoTDBSchema schema = new ObjectMapper().readValue(schemaAttribute, IoTDBSchema.class);
        HashMap<String, Tablet> tablets = processor.generateTablets(schema, 1);

        HashMap<String, Tablet> exceptedTablets = new HashMap<>();
        ArrayList<MeasurementSchema> schemas = new ArrayList<>();
        schemas.add(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.RLE));
        schemas.add(new MeasurementSchema("s2", TSDataType.DOUBLE, TSEncoding.PLAIN));
        exceptedTablets.put("root.test_sg.test_d1", new Tablet("root.test_sg.test_d1", schemas, 1));

        Assertions.assertEquals("root.test_sg.test_d1", tablets.keySet().toArray()[0]);
        Assertions.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getSchemas(),
                tablets.get("root.test_sg.test_d1").getSchemas());
        Assertions.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getMaxRowNumber(),
                tablets.get("root.test_sg.test_d1").getMaxRowNumber());
        Assertions.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getTimeBytesSize(),
                tablets.get("root.test_sg.test_d1").getTimeBytesSize());
        Assertions.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").getTotalValueOccupation(),
                tablets.get("root.test_sg.test_d1").getTotalValueOccupation());
        Assertions.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").deviceId,
                tablets.get("root.test_sg.test_d1").deviceId);
        Assertions.assertEquals(
                exceptedTablets.get("root.test_sg.test_d1").rowSize,
                tablets.get("root.test_sg.test_d1").rowSize);
    }

    @Test
    public void testParseTime() {
        String time = "2022/08/09 10:50:00.000";
        for (String format : processor.STRING_TIME_FORMAT) {
            try {
                DateTimeFormatter.ofPattern(format).parse(time);
                System.out.println(format);
            } catch (DateTimeParseException e) {

            }
        }
    }

    public static class TestAbstractIoTDBProcessor extends AbstractIoTDB {

        @Override
        public void onTrigger(ProcessContext processContext, ProcessSession processSession)
                throws ProcessException {
        }
    }
}
