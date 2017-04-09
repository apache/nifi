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
package org.apache.nifi.processors.avro;

import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class TestConvertKafkaAvroToJSON {

    private static final String TOPIC = "topic";

    Schema schema;
    KafkaAvroSerializer serializer;
    ConvertKafkaAvroToJSON convertKafkaAvroToJSON;

    @Before
    public void setUp() throws IOException, RestClientException {
        SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        convertKafkaAvroToJSON = new ConvertKafkaAvroToJSON() {
            @Override
            protected AvroConverter createAvroConverter() {
                return new AvroConverter(schemaRegistry);
            }
        };
        serializer = new KafkaAvroSerializer(schemaRegistry);
        schema = SchemaBuilder
                .record("User").fields()
                .requiredString("name")
                .optionalInt("favorite_number")
                .optionalString("favorite_color")
                .endRecord();
        schemaRegistry.register(TOPIC + "-value", schema);

    }

    @Test
    public void testAvroMessage() {
        final TestRunner runner = TestRunners.newTestRunner(convertKafkaAvroToJSON);
        runner.setProperty(ConvertKafkaAvroToJSON.SCHEMA_REGISTRY_URL, "http://fake-url");
        runner.setProperty(ConvertKafkaAvroToJSON.TOPIC, TOPIC);

        final GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        byte[] out1 = serializer.serialize(TOPIC, user1);
        runner.enqueue(out1);

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToJSON.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(ConvertAvroToJSON.REL_SUCCESS).get(0);
        out.assertContentEquals("{\"name\":\"Alyssa\",\"favorite_number\":256,\"favorite_color\":null}");
    }

    @Test
    public void testEmptyFlowFile() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(convertKafkaAvroToJSON);
        runner.setProperty(ConvertKafkaAvroToJSON.SCHEMA_REGISTRY_URL, "http://fake-url");
        runner.setProperty(ConvertKafkaAvroToJSON.TOPIC, TOPIC);

        runner.enqueue(new byte[]{});

        runner.run();

        runner.assertAllFlowFilesTransferred(ConvertAvroToJSON.REL_FAILURE, 1);
    }

    @Test
    public void testNonJsonHandledProperly() throws IOException {
        final TestRunner runner = TestRunners.newTestRunner(convertKafkaAvroToJSON);
        runner.setProperty(ConvertKafkaAvroToJSON.SCHEMA_REGISTRY_URL, "http://fake-url");
        runner.setProperty(ConvertKafkaAvroToJSON.TOPIC, TOPIC);
        runner.enqueue("hello".getBytes());
        runner.run();
        runner.assertAllFlowFilesTransferred(ConvertAvroToJSON.REL_FAILURE, 1);
    }
}
