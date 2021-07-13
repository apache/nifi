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
import org.apache.nifi.security.kms.FileBasedKeyProvider;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.kms.StaticKeyProvider;
import org.apache.nifi.security.repository.RepositoryType;
import org.apache.nifi.util.NiFiProperties;

/**
 * Abstract class which defines the method contracts for various repository encryption configuration
 * values. The implementing classes will act as data containers for the encryption configs when
 * initializing the repositories.
 */
public abstract class RepositoryEncryptionConfiguration {
    String keyProviderImplementation;
    String keyProviderLocation;
    String encryptionKeyId;
    Map<String, String> encryptionKeys;
    String repositoryImplementation;
    RepositoryType repositoryType;
    String keyStoreType;
    String keyProviderPassword;

    /**
     * Returns the class name of the {@link KeyProvider} implementation used.
     *
     * @return the class of the key provider
     */
    public String getKeyProviderImplementation() {
        return keyProviderImplementation;
    }

    /**
     * Returns the location of the key provider. For a
     * {@link StaticKeyProvider} this will be null; for all
     * others, it will be the location (file path/URL/etc.) to access the key definitions.
     *
     * @return the file, URL, etc. where the keys are defined
     */
    public String getKeyProviderLocation() {
        return keyProviderLocation;
    }

    /**
     * Returns the "active" encryption key id.
     *
     * @return the key id
     */
    public String getEncryptionKeyId() {
        return encryptionKeyId;
    }

    /**
     * Returns a map of all available encryption keys indexed by the key id if using
     * {@link StaticKeyProvider}. For
     * {@link FileBasedKeyProvider}, this method will return an
     * empty map because the keys must be loaded using the {@code root key} to decrypt them
     *
     * @return a map of key ids & keys
     * @see NiFiProperties#getContentRepositoryEncryptionKeys()
     */
    public Map<String, String> getEncryptionKeys() {
        return encryptionKeys;
    }

    /**
     * Returns the class name for the repository implementation.
     *
     * @return the repository class
     */
    public String getRepositoryImplementation() {
        return repositoryImplementation;
    }

    /**
     * Returns the {@link RepositoryType} enum identifying this repository. Useful for
     * programmatically determining the kind of repository being configured.
     *
     * @return the repository type
     */
    public RepositoryType getRepositoryType() {
        return repositoryType;
    }

    /**
     * Get Key Store Type for Key Store implementation
     *
     * @return Key Store Type
     */
    public String getKeyStoreType() {
        return keyStoreType;
    }

    /**
     * Get Key Provider Password
     *
     * @return Key Provider Password
     */
    public String getKeyProviderPassword() {
        return keyProviderPassword;
    }

    public static RepositoryEncryptionConfiguration fromNiFiProperties(NiFiProperties niFiProperties, RepositoryType repositoryType) {
        switch (repositoryType) {
            case CONTENT:
                return new ContentRepositoryEncryptionConfiguration(niFiProperties);
            case PROVENANCE:
                return new ProvenanceRepositoryEncryptionConfiguration(niFiProperties);
            case FLOWFILE:
                return new FlowFileRepositoryEncryptionConfiguration(niFiProperties);
            default:
                throw new IllegalArgumentException("The specified repository does not support encryption");
        }
    }
}
