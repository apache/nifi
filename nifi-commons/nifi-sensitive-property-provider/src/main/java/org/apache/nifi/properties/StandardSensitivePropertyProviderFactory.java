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

import org.apache.nifi.properties.BootstrapProperties.BootstrapPropertyKey;
import org.apache.nifi.util.NiFiBootstrapUtils;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class StandardSensitivePropertyProviderFactory implements SensitivePropertyProviderFactory {
    private static final Logger logger = LoggerFactory.getLogger(StandardSensitivePropertyProviderFactory.class);

    private final Optional<String> keyHex;
    private final Supplier<BootstrapProperties> bootstrapPropertiesSupplier;
    private final Map<PropertyProtectionScheme, SensitivePropertyProvider> providerMap;
    private Map<String, Pattern> customPropertyContextMap;

    /**
     * Creates a StandardSensitivePropertyProviderFactory using the default bootstrap.conf location and
     * the keyHex extracted from this bootstrap.conf.
     */
    public static SensitivePropertyProviderFactory withDefaults() {
        return withKeyAndBootstrapSupplier(null, null);
    }

    /**
     * Creates a StandardSensitivePropertyProviderFactory using only the provided secret key hex.  The default
     * bootstrap.conf will be used for any providers that may require it, but the provided keyHex will be used instead
     * of the one from the default bootstrap.conf.
     * @param keyHex The secret key hex for encrypting properties
     * @return A StandardSensitivePropertyProviderFactory
     */
    public static SensitivePropertyProviderFactory withKey(final String keyHex) {
        return new StandardSensitivePropertyProviderFactory(keyHex, null);
    }

    /**
     * Creates a new StandardSensitivePropertyProviderFactory using a separate keyHex and provided bootstrap.conf.
     * The provided keyHex will be used instead of the one from the bootstrap.conf.
     * @param keyHex The secret key hex for encrypting properties
     * @param bootstrapPropertiesSupplier A supplier for the BootstrapProperties that represent bootstrap.conf.
     *                                    If the supplier returns null, the default bootstrap.conf will be used instead.
     * @return A StandardSensitivePropertyProviderFactory
     */
    public static SensitivePropertyProviderFactory withKeyAndBootstrapSupplier(final String keyHex,
                                                                               final Supplier<BootstrapProperties> bootstrapPropertiesSupplier) {
        return new StandardSensitivePropertyProviderFactory(keyHex, bootstrapPropertiesSupplier);
    }

    private StandardSensitivePropertyProviderFactory(final String keyHex, final Supplier<BootstrapProperties> bootstrapPropertiesSupplier) {
        this.keyHex = Optional.ofNullable(keyHex);
        this.bootstrapPropertiesSupplier = bootstrapPropertiesSupplier == null ? () -> null : bootstrapPropertiesSupplier;
        this.providerMap = new HashMap<>();
        this.customPropertyContextMap = null;
    }

    private void populateCustomPropertyContextMap() {
        final BootstrapProperties bootstrapProperties = getBootstrapProperties();
        customPropertyContextMap = new HashMap<>();
        final String contextMappingKeyPrefix = BootstrapPropertyKey.CONTEXT_MAPPING_PREFIX.getKey();
        bootstrapProperties.getPropertyKeys().stream()
                .filter(k -> k.contains(contextMappingKeyPrefix))
                .forEach(k -> {
                    customPropertyContextMap.put(StringUtils.substringAfter(k, contextMappingKeyPrefix), Pattern.compile(bootstrapProperties.getProperty(k)));
                });
    }

    private String getKeyHex() {
        return keyHex.orElseGet(() -> getBootstrapProperties().getProperty(BootstrapPropertyKey.SENSITIVE_KEY)
                .orElseThrow(() -> new SensitivePropertyProtectionException("Could not read root key from bootstrap.conf")));
    }

    /**
     * Returns the configured bootstrap properties, or the default bootstrap.conf properties if
     * not provided.
     * @return The bootstrap.conf properties
     */
    private BootstrapProperties getBootstrapProperties() {
        return Optional.ofNullable(bootstrapPropertiesSupplier.get()).orElseGet(() -> {
            try {
                return NiFiBootstrapUtils.loadBootstrapProperties();
            } catch (final IOException e) {
                logger.debug("Could not load bootstrap.conf from disk, so using empty bootstrap.conf", e);
                return BootstrapProperties.EMPTY;
            }
        });
    }

    @Override
    public SensitivePropertyProvider getProvider(final PropertyProtectionScheme protectionScheme) throws SensitivePropertyProtectionException {
        Objects.requireNonNull(protectionScheme, "Protection scheme is required");
        // Only look up the secret key, which can perform a disk read, if this provider actually requires one
        final String keyHex = protectionScheme.requiresSecretKey() ? getKeyHex() : null;
        switch (protectionScheme) {
            case AES_GCM:
                return providerMap.computeIfAbsent(protectionScheme, s -> new AESSensitivePropertyProvider(keyHex));
            case AWS_KMS:
                return providerMap.computeIfAbsent(protectionScheme, s -> new AWSKMSSensitivePropertyProvider(getBootstrapProperties()));
            case AZURE_KEYVAULT_KEY:
                return providerMap.computeIfAbsent(protectionScheme, s -> new AzureKeyVaultKeySensitivePropertyProvider(getBootstrapProperties()));
            case HASHICORP_VAULT_TRANSIT:
                return providerMap.computeIfAbsent(protectionScheme, s -> new HashiCorpVaultTransitSensitivePropertyProvider(getBootstrapProperties()));
            case HASHICORP_VAULT_KV:
                return providerMap.computeIfAbsent(protectionScheme, s -> new HashiCorpVaultKeyValueSensitivePropertyProvider(getBootstrapProperties()));
            default:
                throw new SensitivePropertyProtectionException("Unsupported protection scheme " + protectionScheme);
        }
    }

    @Override
    public Collection<SensitivePropertyProvider> getSupportedSensitivePropertyProviders() {
        return Arrays.stream(PropertyProtectionScheme.values())
                .map(this::getProvider)
                .filter(SensitivePropertyProvider::isSupported)
                .collect(Collectors.toList());
    }

    @Override
    public ProtectedPropertyContext getPropertyContext(final String groupIdentifier, final String propertyName) {
        if (customPropertyContextMap == null) {
            populateCustomPropertyContextMap();
        }
        final String contextName = customPropertyContextMap.entrySet().stream()
                .filter(entry -> entry.getValue().matcher(groupIdentifier).find())
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
        return ProtectedPropertyContext.contextFor(propertyName, contextName);
    }

}
