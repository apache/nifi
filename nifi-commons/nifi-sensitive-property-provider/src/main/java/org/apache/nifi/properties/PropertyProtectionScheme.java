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
package org.apache.nifi.properties;

import java.util.Arrays;
import java.util.Objects;

/**
 * A scheme for protecting sensitive properties.  Each scheme is intended to be backed by an implementation of
 * SensitivePropertyProvider.
 */
public enum PropertyProtectionScheme {
    AES_GCM("aes/gcm/(128|192|256)", "aes/gcm/%s", "AES Sensitive Property Provider", true),
    HASHICORP_VAULT_TRANSIT("hashicorp/vault/transit/[a-zA-Z0-9_-]+", "hashicorp/vault/transit/%s", "HashiCorp Vault Transit Engine Sensitive Property Provider", false);

    PropertyProtectionScheme(final String identifierPattern, final String identifierFormat, final String name, final boolean requiresSecretKey) {
        this.identifierPattern = identifierPattern;
        this.identifierFormat = identifierFormat;
        this.name = name;
        this.requiresSecretKey = requiresSecretKey;
    }

    private final String identifierFormat;
    private final String identifierPattern;
    private final String name;
    private final boolean requiresSecretKey;

    /**
     * Returns a the identifier of the PropertyProtectionScheme.
     * @param args scheme-specific arguments used to fill in the formatted identifierPattern
     * @return The identifier of the PropertyProtectionScheme
     */
    public String getIdentifier(final String... args) {
        return String.format(identifierFormat, args);
    }

    /**
     * Returns whether this scheme requires a secret key.
     * @return True if this scheme requires a secret key
     */
    public boolean requiresSecretKey() {
        return requiresSecretKey;
    }

    /**
     * Returns the name of the PropertyProtectionScheme.
     * @return The name
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the PropertyProtectionScheme matching the provided name.
     * @param identifier The unique PropertyProtectionScheme identifier
     * @return The matching PropertyProtectionScheme
     * @throws IllegalArgumentException If the name was not recognized
     */
    public static PropertyProtectionScheme fromIdentifier(final String identifier) {
        Objects.requireNonNull(identifier, "Identifier must be specified");
        return Arrays.stream(PropertyProtectionScheme.values())
                .filter(scheme -> identifier.matches(scheme.identifierPattern))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unrecognized protection scheme :" + identifier));
    }
}
