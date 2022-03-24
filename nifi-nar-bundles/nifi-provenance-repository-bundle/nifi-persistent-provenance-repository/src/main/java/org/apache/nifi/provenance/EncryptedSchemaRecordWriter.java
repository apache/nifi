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
import org.apache.nifi.repository.encryption.RepositoryEncryptor;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class EncryptedSchemaRecordWriter extends EventIdFirstSchemaRecordWriter {
    public static final String SERIALIZATION_NAME = "EncryptedSchemaRecordWriter";
    public static final int SERIALIZATION_VERSION = 1;

    private final RepositoryEncryptor<byte[], byte[]> repositoryEncryptor;
    private final String keyId;

    public EncryptedSchemaRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed,
                                       final int uncompressedBlockSize, final IdentifierLookup idLookup,
                                       final RepositoryEncryptor<byte[], byte[]> repositoryEncryptor, final String keyId) throws IOException {
        super(file, idGenerator, writer, compressed, uncompressedBlockSize, idLookup);
        this.repositoryEncryptor = repositoryEncryptor;
        this.keyId = keyId;
    }

    @Override
    protected byte[] serializeEvent(final ProvenanceEventRecord event) throws IOException {
        final byte[] serialized = super.serializeEvent(event);
        final String eventId = event.getBestEventIdentifier();

        return repositoryEncryptor.encrypt(serialized, eventId, keyId);
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERSION;
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }

    @Override
    public String toString() {
        return "EncryptedSchemaRecordWriter[keyId=" + keyId + ", encryptor=" + repositoryEncryptor + "]";
    }
}
