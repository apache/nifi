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

import java.io.IOException;
import java.util.List;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedWriteAheadProvenanceRepository extends WriteAheadProvenanceRepository {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedWriteAheadProvenanceRepository.class);

    private KeyProvider keyProvider;
    private ProvenanceEventEncryptor provenanceEventEncryptor;

    private String keyId;

    /**
     * This constructor exists solely for the use of the Java Service Loader mechanism and should not be used.
     */
    public EncryptedWriteAheadProvenanceRepository() {
        super();
    }

    public EncryptedWriteAheadProvenanceRepository(final NiFiProperties nifiProperties) {
        super(RepositoryConfiguration.create(nifiProperties));
    }

    public EncryptedWriteAheadProvenanceRepository(final RepositoryConfiguration config) {
        super(config);
    }

    public EncryptedWriteAheadProvenanceRepository(String keyId) {
        // TODO: Allow configuration/DI of keyProvider and provenanceEventEncryptor?
        this.keyId = keyId;
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) {
        // try {
            final String keyId = getCurrentKeyId();
            // if (keyProvider.keyExists(keyId)) {
            //     EncryptedProvenanceEventRecord encryptedEvent = provenanceEventEncryptor.encrypt(event, keyId);
            //
            // }
        // } catch (EncryptionException e) {
        //     logger.error("Encountered an exception encrypting the event " + event.getFlowFileUuid() + " before registering in the provenance repository", e);
        //     // TODO: Throw exception/recover?
        // }
    }

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> events) {

    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) {
        return null;
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) throws IOException {
        return null;
    }

    public String getCurrentKeyId() {
        return keyId;
    }
}
