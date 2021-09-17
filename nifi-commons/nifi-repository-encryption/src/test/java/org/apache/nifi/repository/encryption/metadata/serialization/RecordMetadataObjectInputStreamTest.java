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
package org.apache.nifi.repository.encryption.metadata.serialization;

import org.apache.nifi.repository.encryption.configuration.EncryptionProtocol;
import org.apache.nifi.repository.encryption.configuration.RepositoryEncryptionMethod;
import org.apache.nifi.repository.encryption.metadata.RecordMetadata;
import org.apache.nifi.repository.encryption.metadata.RecordMetadataSerializer;
import org.apache.nifi.repository.encryption.metadata.record.EncryptionMetadata;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class RecordMetadataObjectInputStreamTest {
    private static final String KEY_ID = UUID.randomUUID().toString();

    private static final RepositoryEncryptionMethod METHOD = RepositoryEncryptionMethod.AES_GCM;

    private static final EncryptionProtocol PROTOCOL = EncryptionProtocol.VERSION_1;

    private static final byte[] INITIALIZATION_VECTOR = new byte[] {1, 2, 3, 4, 5, 6, 7, 8};

    private static final int LENGTH = 1024;

    @Test
    public void testGetRecordMetadata() throws IOException {
        final RecordMetadataSerializer serializer = new StandardRecordMetadataSerializer(PROTOCOL);
        final byte[] serialized = serializer.writeMetadata(KEY_ID, INITIALIZATION_VECTOR, LENGTH, METHOD);

        try (final RecordMetadataObjectInputStream objectInputStream = new RecordMetadataObjectInputStream(new ByteArrayInputStream(serialized))) {
            final RecordMetadata recordMetadata = objectInputStream.getRecordMetadata();
            assertEquals(KEY_ID, recordMetadata.getKeyId());
            assertEquals(LENGTH, recordMetadata.getLength());
            assertArrayEquals(INITIALIZATION_VECTOR, recordMetadata.getInitializationVector());
            assertEquals(METHOD.getAlgorithm(), recordMetadata.getAlgorithm());
            assertEquals(PROTOCOL.getVersion(), recordMetadata.getVersion());
        }
    }

    @Test
    public void testGetRecordMetadataCompatibleClass() throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        final EncryptionMetadata metadata = new EncryptionMetadata();
        metadata.algorithm = METHOD.getAlgorithm();
        metadata.cipherByteLength = LENGTH;
        metadata.ivBytes = INITIALIZATION_VECTOR;
        metadata.keyId = KEY_ID;
        metadata.version = PROTOCOL.getVersion();

        try (final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
            objectOutputStream.writeObject(metadata);
        }

        final byte[] serialized = outputStream.toByteArray();

        try (final RecordMetadataObjectInputStream objectInputStream = new RecordMetadataObjectInputStream(new ByteArrayInputStream(serialized))) {
            final RecordMetadata recordMetadata = objectInputStream.getRecordMetadata();
            assertEquals(KEY_ID, recordMetadata.getKeyId());
            assertEquals(LENGTH, recordMetadata.getLength());
            assertArrayEquals(INITIALIZATION_VECTOR, recordMetadata.getInitializationVector());
        }
    }
}
