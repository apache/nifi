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
package org.apache.nifi.repository.encryption.configuration.kms;

import org.apache.nifi.repository.encryption.configuration.EncryptedRepositoryType;

import java.util.Arrays;

import static org.apache.nifi.util.NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
import static org.apache.nifi.util.NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION;
import static org.apache.nifi.util.NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_PASSWORD;

/**
 * Enumeration of configuration property names for encrypted repositories supporting backward compatibility
 */
enum EncryptedRepositoryProperty {
    CONTENT(
            EncryptedRepositoryType.CONTENT,
            CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS,
            CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION,
            CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD
    ),

    FLOWFILE(
            EncryptedRepositoryType.FLOWFILE,
            FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS,
            FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION,
            FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_PASSWORD
    ),

    PROVENANCE(
            EncryptedRepositoryType.PROVENANCE,
            PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS,
            PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_LOCATION,
            PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_PASSWORD
    );

    private EncryptedRepositoryType encryptedRepositoryType;

    private String propertyType;

    private String implementationClass;

    private String location;

    private String password;

    EncryptedRepositoryProperty(final EncryptedRepositoryType encryptedRepositoryType,
                                final String implementationClass,
                                final String location,
                                final String password) {
        this.encryptedRepositoryType = encryptedRepositoryType;
        this.propertyType = encryptedRepositoryType.toString().toLowerCase();
        this.implementationClass = implementationClass;
        this.location = location;
        this.password = password;
    }

    /**
     * Get Encrypted Repository Type
     *
     * @return Encrypted Repository Type
     */
    public EncryptedRepositoryType getEncryptedRepositoryType() {
        return encryptedRepositoryType;
    }

    /**
     * Get Property Type for resolving property names in NiFi Properties
     *
     * @return Property Type
     */
    public String getPropertyType() {
        return propertyType;
    }

    /**
     * Get implementation class property name
     *
     * @return Implementation class property name
     */
    public String getImplementationClass() {
        return implementationClass;
    }

    /**
     * Get location property name
     *
     * @return Location property name
     */
    public String getLocation() {
        return location;
    }

    /**
     * Get password property name
     *
     * @return Password property name
     */
    public String getPassword() {
        return password;
    }

    /**
     * Get Encrypted Repository Property from Encrypted Repository Type
     *
     * @param encryptedRepositoryType Encryption Repository Type
     * @return Encrypted Repository Property
     * @throws IllegalArgumentException Thrown when matching Encrypted Repository Type not found
     */
    public static EncryptedRepositoryProperty fromEncryptedRepositoryType(final EncryptedRepositoryType encryptedRepositoryType) {
        return Arrays.stream(values())
                .filter(value -> value.encryptedRepositoryType == encryptedRepositoryType)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(encryptedRepositoryType.toString()));
    }
}
