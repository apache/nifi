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
import org.apache.nifi.repository.encryption.metadata.RecordMetadataSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Standard implementation of Record Metadata Serializer using ObjectOutputStream
 */
public class StandardRecordMetadataSerializer implements RecordMetadataSerializer {
    private final EncryptionProtocol encryptionProtocol;

    public StandardRecordMetadataSerializer(final EncryptionProtocol encryptionProtocol) {
        this.encryptionProtocol = Objects.requireNonNull(encryptionProtocol, "Encryption Protocol required");
    }

    /**
     * Write Metadata to byte array
     *
     * @param keyId Key Identifier used for encryption
     * @param initializationVector Initialization Vector
     * @param length Length of encrypted binary
     * @param repositoryEncryptionMethod Repository Encryption Method
     * @return Serialized byte array
     */
    @Override
    public byte[] writeMetadata(final String keyId, final byte[] initializationVector, final int length, final RepositoryEncryptionMethod repositoryEncryptionMethod) {
        Objects.requireNonNull(keyId, "Key Identifier required");
        Objects.requireNonNull(repositoryEncryptionMethod, "Repository Encryption Method required");

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (final ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream)) {
            final StandardRecordMetadata recordMetadata = new StandardRecordMetadata();
            recordMetadata.keyId = keyId;
            recordMetadata.ivBytes = initializationVector;
            recordMetadata.cipherByteLength = length;
            recordMetadata.algorithm = repositoryEncryptionMethod.getAlgorithm();
            recordMetadata.version = encryptionProtocol.getVersion();
            objectOutputStream.writeObject(recordMetadata);
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format("Write Metadata Key ID [%s] Failed", keyId), e);
        }
        return outputStream.toByteArray();
    }
}
