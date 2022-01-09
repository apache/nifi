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

import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.repository.schema.FieldCache;
import org.apache.nifi.util.NiFiProperties;
import org.wali.SerDe;

public class EncryptedRepositoryRecordSerdeFactory extends StandardRepositoryRecordSerdeFactory {
    private final NiFiProperties niFiProperties;

    public EncryptedRepositoryRecordSerdeFactory(final ResourceClaimManager claimManager, final NiFiProperties niFiProperties, final FieldCache fieldCache) {
        super(claimManager, fieldCache);
        this.niFiProperties = niFiProperties;
    }

    @Override
    public SerDe<SerializedRepositoryRecord> createSerDe(final String encodingName) {
        // If no encoding is provided, use the encrypted as the default
        if (encodingName == null || EncryptedSchemaRepositoryRecordSerde.class.getName().equals(encodingName)) {
            // Delegate the creation of the wrapped serde to the standard factory
            final SerDe<SerializedRepositoryRecord> serde = super.createSerDe(null);
            return new EncryptedSchemaRepositoryRecordSerde(serde, niFiProperties);
        }

        // If not encrypted, delegate to the standard factory
        return super.createSerDe(encodingName);
    }
}
