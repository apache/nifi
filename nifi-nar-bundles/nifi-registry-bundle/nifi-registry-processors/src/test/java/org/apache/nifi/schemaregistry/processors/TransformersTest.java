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
package org.apache.nifi.schemaregistry.processors;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

public class TransformersTest {

    @Test
    public void validateCSVtoAvroPair() throws Exception {
        String data = "John Dow|13|blue";
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
                + "\"fields\": [ " + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"favorite_number\",  \"type\": \"int\"}, "
                + "{\"name\": \"favorite_color\", \"type\": \"string\"} " + "]" + "}";

        Schema schema = new Schema.Parser().parse(fooSchemaText);

        // CSV -> AVRO -> CSV
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        GenericRecord record = CSVUtils.read(in, '|', schema, '\"');
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroUtils.write(record, out);
        byte[] avro = out.toByteArray();

        in = new ByteArrayInputStream(avro);
        record = AvroUtils.read(in, schema);
        out = new ByteArrayOutputStream();
        CSVUtils.write(record, '|', out);
        byte[] csv = out.toByteArray();
        assertEquals(data, new String(csv, StandardCharsets.UTF_8));
    }

    @Test
    public void validateCSVtoJsonPair() throws Exception {
        String data = "John Dow|13|blue";
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
                + "\"fields\": [ " + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"favorite_number\",  \"type\": \"int\"}, "
                + "{\"name\": \"favorite_color\", \"type\": \"string\"} " + "]" + "}";

        Schema schema = new Schema.Parser().parse(fooSchemaText);

        // CSV -> JSON -> CSV
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        GenericRecord record = CSVUtils.read(in, '|', schema, '\"');
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonUtils.write(record, out);
        byte[] json = out.toByteArray();

        assertEquals("{\"name\":\"John Dow\",\"favorite_number\":13,\"favorite_color\":\"blue\"}", new String(json, StandardCharsets.UTF_8));

        in = new ByteArrayInputStream(json);
        record = JsonUtils.read(in, schema);
        out = new ByteArrayOutputStream();
        CSVUtils.write(record, '|', out);
        byte[] csv = out.toByteArray();
        assertEquals(data, new String(csv, StandardCharsets.UTF_8));
    }

    @Test
    public void validateJsonToAvroPair() throws Exception {
        String data = "{\"name\":\"John Dow\",\"favorite_number\":13,\"favorite_color\":\"blue\"}";
        String fooSchemaText = "{\"namespace\": \"example.avro\", " + "\"type\": \"record\", " + "\"name\": \"User\", "
                + "\"fields\": [ " + "{\"name\": \"name\", \"type\": \"string\"}, "
                + "{\"name\": \"favorite_number\",  \"type\": \"int\"}, "
                + "{\"name\": \"favorite_color\", \"type\": \"string\"} " + "]" + "}";

        Schema schema = new Schema.Parser().parse(fooSchemaText);

        // JSON -> AVRO -> JSON
        ByteArrayInputStream in = new ByteArrayInputStream(data.getBytes());
        GenericRecord record = JsonUtils.read(in, schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroUtils.write(record, out);
        byte[] avro = out.toByteArray();

        in = new ByteArrayInputStream(avro);
        record = AvroUtils.read(in, schema);
        out = new ByteArrayOutputStream();
        JsonUtils.write(record, out);
        byte[] csv = out.toByteArray();
        assertEquals(data, new String(csv, StandardCharsets.UTF_8));
    }
}
