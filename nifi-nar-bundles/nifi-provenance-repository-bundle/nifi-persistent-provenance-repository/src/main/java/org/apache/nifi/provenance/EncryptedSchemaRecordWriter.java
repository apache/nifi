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
package org.apache.nifi.provenance;

import org.apache.nifi.provenance.toc.TocWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.util.concurrent.atomic.AtomicLong;

public class EncryptedSchemaRecordWriter extends EventIdFirstSchemaRecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSchemaRecordWriter.class);
    public static final String SERIALIZATION_NAME = "EncryptedSchemaRecordWriter";
    public static final int SERIALIZATION_VERSION = 1;

    private ProvenanceEventEncryptor provenanceEventEncryptor;
    private String keyId;

    public EncryptedSchemaRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed,
                                       final int uncompressedBlockSize, final IdentifierLookup idLookup,
                                       ProvenanceEventEncryptor provenanceEventEncryptor, int debugFrequency) throws IOException, EncryptionException {
        super(file, idGenerator, writer, compressed, uncompressedBlockSize, idLookup);
        this.provenanceEventEncryptor = provenanceEventEncryptor;

        try {
            this.keyId = getNextAvailableKeyId();
        } catch (KeyManagementException e) {
            logger.error("Encountered an error initializing the encrypted schema record writer because the provided encryptor has no valid keys available: ", e);
            throw new EncryptionException("No valid keys in the provenance event encryptor", e);
        }
    }

    @Override
    protected byte[] serializeEvent(final ProvenanceEventRecord event) throws IOException {
        final byte[] serialized = super.serializeEvent(event);
        final String eventId = event.getBestEventIdentifier();

        try {
            final byte[] cipherBytes = encrypt(serialized, eventId);
            return cipherBytes;
        } catch (EncryptionException e) {
            logger.error("Encountered an error: ", e);
            throw new IOException("Error encrypting the provenance record", e);
        }
    }

    private byte[] encrypt(byte[] serialized, String eventId) throws EncryptionException {
        final String keyId = getKeyId();
        try {
            return provenanceEventEncryptor.encrypt(serialized, eventId, keyId);
        } catch (Exception e) {
            logger.error("Encountered an error: ", e);
            throw new EncryptionException(e);
        }
    }

    private String getNextAvailableKeyId() throws KeyManagementException {
        return provenanceEventEncryptor.getNextKeyId();
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERSION;
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }

    public String getKeyId() {
        return keyId;
    }

    @Override
    public String toString() {
        return "EncryptedSchemaRecordWriter[keyId=" + keyId + ", encryptor=" + provenanceEventEncryptor + "]";
    }
}
