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

import java.io.IOException;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.repository.schema.FieldCache;
import org.apache.nifi.security.kms.CryptoUtils;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.repository.config.FlowFileRepositoryEncryptionConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDe;

public class EncryptedRepositoryRecordSerdeFactory extends StandardRepositoryRecordSerdeFactory {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedRepositoryRecordSerdeFactory.class);

    private FlowFileRepositoryEncryptionConfiguration ffrec;

    public EncryptedRepositoryRecordSerdeFactory(final ResourceClaimManager claimManager, final NiFiProperties niFiProperties, final FieldCache fieldCache) throws EncryptionException {
        super(claimManager, fieldCache);

        // Retrieve encryption configuration
        FlowFileRepositoryEncryptionConfiguration ffrec = new FlowFileRepositoryEncryptionConfiguration(niFiProperties);

        // The configuration should be validated immediately rather than waiting until attempting to deserialize records (initial recovery at startup)
        if (!CryptoUtils.isValidRepositoryEncryptionConfiguration(ffrec)) {
            logger.error("The flowfile repository encryption configuration is not valid (see above). Shutting down...");
            throw new EncryptionException("The flowfile repository encryption configuration is not valid");
        }

        this.ffrec = ffrec;
    }

    @Override
    public SerDe<SerializedRepositoryRecord> createSerDe(final String encodingName) {
        // If no encoding is provided, use the encrypted as the default
        if (encodingName == null || EncryptedSchemaRepositoryRecordSerde.class.getName().equals(encodingName)) {
            // Delegate the creation of the wrapped serde to the standard factory
            final SerDe<SerializedRepositoryRecord> serde = super.createSerDe(null);

            // Retrieve encryption configuration
            try {
                return new EncryptedSchemaRepositoryRecordSerde(serde, ffrec);
            } catch (IOException e) {
                throw new IllegalArgumentException("Could not create Deserializer for Repository Records because the encoding " + encodingName + " requires NiFi properties which could not be loaded");
            }
        }

        // If not encrypted, delegate to the standard factory
        return super.createSerDe(encodingName);
    }
}
