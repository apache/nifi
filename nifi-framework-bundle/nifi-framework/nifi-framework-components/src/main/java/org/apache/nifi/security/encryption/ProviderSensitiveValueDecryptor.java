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
package org.apache.nifi.security.encryption;

import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedExtensionComponent;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.diff.SensitiveValueDecryptor;

import java.util.Objects;

/**
 * Decrypts sensitive values for flow comparison using a Property Encryption Provider. The context supplied for each
 * value is rebuilt from the owning component so that it matches the context supplied when the flow was serialized.
 */
public class ProviderSensitiveValueDecryptor implements SensitiveValueDecryptor {
    private final PropertyEncryptionProvider propertyEncryptionProvider;

    public ProviderSensitiveValueDecryptor(final PropertyEncryptionProvider propertyEncryptionProvider) {
        this.propertyEncryptionProvider = Objects.requireNonNull(propertyEncryptionProvider, "Property Encryption Provider required");
    }

    @Override
    public String decrypt(final VersionedComponent owner, final String valueName, final String encryptedValue) {
        return SensitivePropertyCodec.decrypt(propertyEncryptionProvider, encryptedValue, getContext(owner, valueName));
    }

    private SensitivePropertyContext getContext(final VersionedComponent owner, final String valueName) {
        if (owner instanceof final VersionedParameterContext parameterContext) {
            return SensitivePropertyContextFactory.forParameter(parameterContext.getName(), valueName);
        }

        final String componentType = owner instanceof final VersionedExtensionComponent extension ? extension.getType() : null;
        return SensitivePropertyContextFactory.forComponent(owner.getInstanceIdentifier(), componentType, valueName);
    }
}
