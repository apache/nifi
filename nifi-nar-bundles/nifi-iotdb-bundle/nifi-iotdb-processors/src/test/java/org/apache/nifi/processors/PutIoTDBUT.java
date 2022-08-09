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
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

public class PutIoTDBUT {

    @Test
    public void testParseSchemaByAttribute() throws JsonProcessingException {
        String schemaAttribute =
                "{\n"
                        + "\t\"timeType\": \"LONG\",\n"
                        + "\t\"fields\": [\n"
                        + "\t\t{\"tsName\": \"root.sg.d1.s1\",\"dataType\": \"INT32\", \"encoding\": \"PLAIN\", \"compressionType\": \"SNAPPY\"},\n"
                        + "\t\t{\"tsName\": \"root.sg.d1.s2\",\"dataType\": \"BOOLEAN\", \"encoding\": \"PLAIN\", \"compressionType\": \"GZIP\"},\n"
                        + "\t\t{\"tsName\": \"root.sg.d1.s3\",\"dataType\": \"TEXT\", \"encoding\": \"DICTIONARY\"}\n"
                        + "\t]\n"
                        + "}";
        IoTDBSchema.TimeType exceptedTimeType = IoTDBSchema.TimeType.LONG;
        ArrayList<String> exceptedFieldNames =
                new ArrayList<String>() {
                    {
                        add("root.sg.d1.s1");
                        add("root.sg.d1.s2");
                        add("root.sg.d1.s3");
                    }
                };
        ArrayList<TSDataType> exceptedDataTypes =
                new ArrayList<TSDataType>() {
                    {
                        add(TSDataType.INT32);
                        add(TSDataType.BOOLEAN);
                        add(TSDataType.TEXT);
                    }
                };
        ArrayList<TSEncoding> exceptedEncodings =
                new ArrayList<TSEncoding>() {
                    {
                        add(TSEncoding.PLAIN);
                        add(TSEncoding.PLAIN);
                        add(TSEncoding.DICTIONARY);
                    }
                };

        ArrayList<CompressionType> exceptedCompressionTypes =
                new ArrayList<CompressionType>() {
                    {
                        add(CompressionType.SNAPPY);
                        add(CompressionType.GZIP);
                        add(null);
                    }
                };

        IoTDBSchema schema = new ObjectMapper().readValue(schemaAttribute, IoTDBSchema.class);
        Assert.assertEquals(exceptedTimeType, schema.getTimeType());
        Assert.assertEquals(exceptedFieldNames, schema.getFieldNames());
        Assert.assertEquals(exceptedDataTypes, schema.getDataTypes());
        Assert.assertEquals(exceptedEncodings, schema.getEncodingTypes());
        Assert.assertEquals(exceptedCompressionTypes, schema.getCompressionTypes());
    }
}
