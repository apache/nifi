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

package org.apache.nifi.controller.repository;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.controller.repository.schema.RepositoryRecordSchema;
import org.apache.nifi.repository.schema.NoOpFieldCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.nifi.controller.repository.RepositoryRecordType.SWAP_IN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SchemaRepositoryRecordSerdeTest {
    public static final String TEST_QUEUE_IDENTIFIER = "testQueueIdentifier";
    private StandardResourceClaimManager resourceClaimManager;
    private SchemaRepositoryRecordSerde schemaRepositoryRecordSerde;
    private FlowFileQueue flowFileQueue;
    private ByteArrayOutputStream byteArrayOutputStream;
    private DataOutputStream dataOutputStream;

    @BeforeEach
    public void setup() {
        resourceClaimManager = new StandardResourceClaimManager();
        schemaRepositoryRecordSerde = new SchemaRepositoryRecordSerde(resourceClaimManager, new NoOpFieldCache());
        flowFileQueue = createMockQueue(TEST_QUEUE_IDENTIFIER);
        byteArrayOutputStream = new ByteArrayOutputStream();
        dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    }

    @AfterEach
    public void teardown() {
        resourceClaimManager.purge();
    }

    @Test
    public void testV1CreateCantHandleLongAttributeName() throws IOException {
        RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1.writeTo(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put(stringBuilder.toString(), "testValue");
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(createCreateFlowFileRecord(attributes)), dataOutputStream,
                RepositoryRecordSchema.CREATE_OR_UPDATE_SCHEMA_V1, RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertNotEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testV1CreateCantHandleLongAttributeValue() throws IOException {
        RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1.writeTo(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put("testName", stringBuilder.toString());
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(createCreateFlowFileRecord(attributes)), dataOutputStream,
                RepositoryRecordSchema.CREATE_OR_UPDATE_SCHEMA_V1, RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertNotEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testV2CreateCanHandleLongAttributeName() throws IOException {
        schemaRepositoryRecordSerde.writeHeader(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put(stringBuilder.toString(), "testValue");
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(createCreateFlowFileRecord(attributes)), dataOutputStream);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testV2CreateCanHandleLongAttributeValue() throws IOException {
        schemaRepositoryRecordSerde.writeHeader(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put("testName", stringBuilder.toString());
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(createCreateFlowFileRecord(attributes)), dataOutputStream);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testRoundTripCreateV1ToV2() throws IOException {
        RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1.writeTo(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("testName", "testValue");
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(createCreateFlowFileRecord(attributes)), dataOutputStream,
                RepositoryRecordSchema.CREATE_OR_UPDATE_SCHEMA_V1, RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testV1SwapInCantHandleLongAttributeName() throws IOException {
        RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1.writeTo(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put(stringBuilder.toString(), "testValue");
        final StandardRepositoryRecord record = createCreateFlowFileRecord(attributes);
        record.setSwapLocation("fake");
        assertEquals(SWAP_IN, record.getType());
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(record), dataOutputStream, RepositoryRecordSchema.SWAP_IN_SCHEMA_V1,
            RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertNotEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testV1SwapInCantHandleLongAttributeValue() throws IOException {
        RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1.writeTo(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put("testName", stringBuilder.toString());
        final StandardRepositoryRecord record = createCreateFlowFileRecord(attributes);
        record.setSwapLocation("fake");
        assertEquals(SWAP_IN, record.getType());
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(record), dataOutputStream, RepositoryRecordSchema.SWAP_IN_SCHEMA_V1,
            RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertNotEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testV2SwapInCanHandleLongAttributeName() throws IOException {
        schemaRepositoryRecordSerde.writeHeader(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put(stringBuilder.toString(), "testValue");
        final StandardRepositoryRecord record = createCreateFlowFileRecord(attributes);
        record.setSwapLocation("fake");
        assertEquals(SWAP_IN, record.getType());
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(record), dataOutputStream);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testV2SwapInCanHandleLongAttributeValue() throws IOException {
        schemaRepositoryRecordSerde.writeHeader(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        final StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < 65536; i++) {
            stringBuilder.append('a');
        }
        attributes.put("testName", stringBuilder.toString());
        final StandardRepositoryRecord record = createCreateFlowFileRecord(attributes);
        record.setSwapLocation("fake");
        assertEquals(SWAP_IN, record.getType());
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(record), dataOutputStream);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
    }

    @Test
    public void testRoundTripSwapInV1ToV2() throws IOException {
        RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1.writeTo(dataOutputStream);
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("testName", "testValue");
        final StandardRepositoryRecord record = createCreateFlowFileRecord(attributes);
        record.setSwapLocation("fake");
        assertEquals(SWAP_IN, record.getType());
        schemaRepositoryRecordSerde.serializeRecord(new LiveSerializedRepositoryRecord(record), dataOutputStream, RepositoryRecordSchema.SWAP_IN_SCHEMA_V1,
            RepositoryRecordSchema.REPOSITORY_RECORD_SCHEMA_V1);

        final DataInputStream dataInputStream = createDataInputStream();
        schemaRepositoryRecordSerde.readHeader(dataInputStream);
        final SerializedRepositoryRecord repositoryRecord = schemaRepositoryRecordSerde.deserializeRecord(dataInputStream, 2);
        assertEquals(attributes, repositoryRecord.getFlowFileRecord().getAttributes());
        assertEquals(SWAP_IN, repositoryRecord.getType());
    }

    private DataInputStream createDataInputStream() throws IOException {
        dataOutputStream.flush();
        return new DataInputStream(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
    }

    private StandardRepositoryRecord createCreateFlowFileRecord(final Map<String, String> attributes) {
        final StandardRepositoryRecord standardRepositoryRecord = new StandardRepositoryRecord(flowFileQueue);
        final StandardFlowFileRecord.Builder flowFileRecordBuilder = new StandardFlowFileRecord.Builder();
        flowFileRecordBuilder.addAttributes(attributes);
        standardRepositoryRecord.setWorking(flowFileRecordBuilder.build(), false);
        return standardRepositoryRecord;
    }

    private FlowFileQueue createMockQueue(final String identifier) {
        final FlowFileQueue flowFileQueue = mock(FlowFileQueue.class);
        when(flowFileQueue.getIdentifier()).thenReturn(identifier);
        return flowFileQueue;
    }
}
