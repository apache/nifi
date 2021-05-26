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
package org.apache.nifi.security.repository.config;

import java.util.Map;
import org.apache.nifi.security.repository.RepositoryType;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.util.NiFiProperties;

public class ProvenanceRepositoryEncryptionConfiguration extends RepositoryEncryptionConfiguration {
    /**
     * Constructor which accepts a {@link NiFiProperties} object and extracts the relevant
     * property values directly.
     *
     * @param niFiProperties the NiFi properties
     */
    public ProvenanceRepositoryEncryptionConfiguration(NiFiProperties niFiProperties) {
        this(niFiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS),
                niFiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION),
                niFiProperties.getProvenanceRepoEncryptionKeyId(),
                niFiProperties.getProvenanceRepoEncryptionKeys(),
                niFiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS),
                niFiProperties.getProperty(NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_PASSWORD)
        );
    }

    /**
     * Constructor which accepts explicit values for each configuration value.
     *
     * @param keyProviderImplementation the key provider implementation class
     * @param keyProviderLocation the key provider location
     * @param encryptionKeyId the active encryption key id
     * @param encryptionKeys the map of available keys
     * @param repositoryImplementation the repository implementation class
     * @param keyProviderPassword Key Provider Password used based on provider implementation
     */
    public ProvenanceRepositoryEncryptionConfiguration(final String keyProviderImplementation,
                                                       final String keyProviderLocation,
                                                       final String encryptionKeyId,
                                                       final Map<String, String> encryptionKeys,
                                                       final String repositoryImplementation,
                                                       final String keyProviderPassword) {
        this.keyProviderImplementation = keyProviderImplementation;
        this.keyProviderLocation = keyProviderLocation;
        this.encryptionKeyId = encryptionKeyId;
        this.encryptionKeys = encryptionKeys;
        this.repositoryImplementation = repositoryImplementation;
        this.repositoryType = RepositoryType.CONTENT;
        this.keyStoreType = KeyStoreUtils.getKeystoreTypeFromExtension(keyProviderLocation).getType();
        this.keyProviderPassword = keyProviderPassword;
    }
}
