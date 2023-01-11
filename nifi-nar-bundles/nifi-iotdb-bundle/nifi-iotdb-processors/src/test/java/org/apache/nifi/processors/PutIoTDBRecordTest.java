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
import org.apache.nifi.processors.model.DatabaseSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PutIoTDBRecordTest {

    @Test
    public void testParseSchemaByAttribute() throws JsonProcessingException {
        String schemaAttribute =
                "{"
                        + "\"fields\": ["
                        + "{\"tsName\": \"s1\",\"dataType\": \"INT32\", \"encoding\": \"PLAIN\", \"compressionType\": \"SNAPPY\"},"
                        + "{\"tsName\": \"s2\",\"dataType\": \"BOOLEAN\", \"encoding\": \"PLAIN\", \"compressionType\": \"GZIP\"},"
                        + "{\"tsName\": \"s3\",\"dataType\": \"TEXT\", \"encoding\": \"DICTIONARY\"}"
                        + "]"
                        + "}";
        List<String> exceptedFieldNames = Arrays.asList("root.sg.d1.s1","root.sg.d1.s2","root.sg.d1.s3");
        List<TSDataType> exceptedDataTypes = Arrays.asList(TSDataType.INT32, TSDataType.BOOLEAN, TSDataType.TEXT);
        List<TSEncoding> exceptedEncodings = Arrays.asList(TSEncoding.PLAIN, TSEncoding.PLAIN, TSEncoding.DICTIONARY);

        List<CompressionType> exceptedCompressionTypes =  Arrays.asList(CompressionType.SNAPPY, CompressionType.GZIP, null);

        DatabaseSchema schema = new ObjectMapper().readValue(schemaAttribute, DatabaseSchema.class);
        assertEquals(exceptedFieldNames, schema.getFieldNames("root.sg.d1."));
        assertArrayEquals(exceptedDataTypes.stream().sorted().toArray(), schema.getDataTypes().stream().sorted().toArray());
        assertArrayEquals(exceptedEncodings.stream().sorted().toArray(), schema.getEncodingTypes().stream().sorted().toArray());
        assertArrayEquals(exceptedCompressionTypes.stream().filter(Objects::nonNull).sorted().toArray(), schema.getCompressionTypes().stream().filter(Objects::nonNull).sorted().toArray());
    }
}
