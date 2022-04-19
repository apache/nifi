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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;

/**
 * Class performing unprotection activities before returning a clean
 * implementation of {@link ApplicationProperties}.
 * This encapsulates the sensitive property access logic from external consumers
 * of {@code ApplicationProperties}.
 *
 * @param <T> The type of protected application properties
 * @param <U> The type of standard application properties that backs the protected application properties
 */
public class ApplicationPropertiesProtector<T extends ProtectedProperties<U>, U extends ApplicationProperties>
        implements SensitivePropertyProtector<T, U> {
    public static final String PROTECTED_KEY_SUFFIX = ".protected";

    private static final Logger logger = LoggerFactory.getLogger(ApplicationPropertiesProtector.class);

    private final T protectedProperties;

    private final Map<String, SensitivePropertyProvider> localProviderCache = new HashMap<>();

    /**
     * Creates an instance containing the provided {@link ProtectedProperties}.
     *
     * @param protectedProperties the ProtectedProperties to contain
     */
    public ApplicationPropertiesProtector(final T protectedProperties) {
        this.protectedProperties = protectedProperties;
        logger.debug("Loaded {} properties (including {} protection schemes) into {}", getPropertyKeysIncludingProtectionSchemes().size(),
                getProtectedPropertyKeys().size(), this.getClass().getName());
    }

    /**
     * Returns the sibling property key which specifies the protection scheme for this key.
     * <p>
     * Example:
     * <p>
     * nifi.sensitive.key=ABCXYZ
     * nifi.sensitive.key.protected=aes/gcm/256
     * <p>
     * nifi.sensitive.key -> nifi.sensitive.key.protected
     *
     * @param key the key identifying the sensitive property
     * @return the key identifying the protection scheme for the sensitive property
     */
    public static String getProtectionKey(final String key) {
        if (key == null || key.isEmpty()) {
            throw new IllegalArgumentException("Cannot find protection key for null key");
        }

        return key + PROTECTED_KEY_SUFFIX;
    }

    /**
     * Retrieves all known property keys.
     *
     * @return all known property keys
     */
    @Override
    public Set<String> getPropertyKeys() {
        Set<String> filteredKeys = getPropertyKeysIncludingProtectionSchemes();
        filteredKeys.removeIf(p -> p.endsWith(PROTECTED_KEY_SUFFIX));
        return filteredKeys;
    }

    @Override
    public int size() {
        return getPropertyKeys().size();
    }

    @Override
    public Set<String> getPropertyKeysIncludingProtectionSchemes() {
        return protectedProperties.getApplicationProperties().getPropertyKeys();
    }

    /**
     * Splits a single string containing multiple property keys into a List. Delimited by ',' or ';' and ignores leading and trailing whitespace around delimiter.
     *
     * @param multipleProperties a single String containing multiple properties, i.e. "nifi.property.1; nifi.property.2, nifi.property.3"
     * @return a List containing the split and trimmed properties
     */
    private static List<String> splitMultipleProperties(final String multipleProperties) {
        if (multipleProperties == null || multipleProperties.trim().isEmpty()) {
            return new ArrayList<>(0);
        } else {
            List<String> properties = new ArrayList<>(asList(multipleProperties.split("\\s*[,;]\\s*")));
            for (int i = 0; i < properties.size(); i++) {
                properties.set(i, properties.get(i).trim());
            }
            return properties;
        }
    }

    private String getProperty(final String key) {
        return protectedProperties.getApplicationProperties().getProperty(key);
    }

    private String getAdditionalSensitivePropertiesKeys() {
        return getProperty(protectedProperties.getAdditionalSensitivePropertiesKeysName());
    }

    private String getAdditionalSensitivePropertiesKeysName() {
        return protectedProperties.getAdditionalSensitivePropertiesKeysName();
    }

    @Override
    public List<String> getSensitivePropertyKeys() {
        final String additionalPropertiesString = getAdditionalSensitivePropertiesKeys();
        final String additionalPropertiesKeyName = protectedProperties.getAdditionalSensitivePropertiesKeysName();
        if (additionalPropertiesString == null || additionalPropertiesString.trim().isEmpty()) {
            return protectedProperties.getDefaultSensitiveProperties();
        } else {
            List<String> additionalProperties = splitMultipleProperties(additionalPropertiesString);
            /* Remove this key if it was accidentally provided as a sensitive key
             * because we cannot protect it and read from it
            */
            if (additionalProperties.contains(additionalPropertiesKeyName)) {
                logger.warn("The key '{}' contains itself. This is poor practice and should be removed", additionalPropertiesKeyName);
                additionalProperties.remove(additionalPropertiesKeyName);
            }
            additionalProperties.addAll(protectedProperties.getDefaultSensitiveProperties());
            return additionalProperties;
        }
    }

    @Override
    public List<String> getPopulatedSensitivePropertyKeys() {
        List<String> allSensitiveKeys = getSensitivePropertyKeys();
        return allSensitiveKeys.stream().filter(k -> isNotBlank(getProperty(k))).collect(Collectors.toList());
    }

    @Override
    public boolean hasProtectedKeys() {
        final List<String> sensitiveKeys = getSensitivePropertyKeys();
        for (String k : sensitiveKeys) {
            if (isPropertyProtected(k)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Map<String, String> getProtectedPropertyKeys() {
        final List<String> sensitiveKeys = getSensitivePropertyKeys();

        final Map<String, String> traditionalProtectedProperties = new HashMap<>();
        for (final String key : sensitiveKeys) {
            final String protection = getProperty(getProtectionKey(key));
            if (isNotBlank(protection) && isNotBlank(getProperty(key))) {
                traditionalProtectedProperties.put(key, protection);
            }
        }

        return traditionalProtectedProperties;
    }

    @Override
    public boolean isPropertySensitive(final String key) {
        // If the explicit check for ADDITIONAL_SENSITIVE_PROPERTIES_KEY is not here, this will loop infinitely
        return key != null && !key.equals(getAdditionalSensitivePropertiesKeysName()) && getSensitivePropertyKeys().contains(key.trim());
    }

    /**
     * Returns true if the property identified by this key is considered protected in this instance of {@code NiFiProperties}.
     * The property value is protected if the key is sensitive and the sibling key of key.protected is present.
     *
     * @param key the key
     * @return true if it is currently marked as protected
     * @see ApplicationPropertiesProtector#getSensitivePropertyKeys()
     */
    @Override
    public boolean isPropertyProtected(final String key) {
        return key != null && isPropertySensitive(key) && isNotBlank(getProperty(getProtectionKey(key)));
    }

    @Override
    public U getUnprotectedProperties() throws SensitivePropertyProtectionException {
        if (hasProtectedKeys()) {
            logger.debug("Protected Properties [{}] Sensitive Properties [{}]",
                    getProtectedPropertyKeys().size(),
                    getSensitivePropertyKeys().size());

            final Properties rawProperties = new Properties();

            final Set<String> failedKeys = new HashSet<>();

            for (final String key : getPropertyKeys()) {
                /* Three kinds of keys
                 * 1. protection schemes -- skip
                 * 2. protected keys -- unprotect and copy
                 * 3. normal keys -- copy over
                 */
                if (key.endsWith(PROTECTED_KEY_SUFFIX)) {
                    // Do nothing
                } else if (isPropertyProtected(key)) {
                    try {
                        rawProperties.setProperty(key, unprotectValue(key, getProperty(key)));
                    } catch (final SensitivePropertyProtectionException e) {
                        logger.warn("Failed to unprotect '{}'", key, e);
                        failedKeys.add(key);
                    }
                } else {
                    rawProperties.setProperty(key, getProperty(key));
                }
            }

            if (!failedKeys.isEmpty()) {
                final String failed = failedKeys.size() == 1 ? failedKeys.iterator().next() : String.join(", ", failedKeys);
                throw new SensitivePropertyProtectionException(String.format("Failed unprotected properties: %s", failed));
            }

            return protectedProperties.createApplicationProperties(rawProperties);
        } else {
            logger.debug("No protected properties");
            return protectedProperties.getApplicationProperties();
        }
    }

    @Override
    public void addSensitivePropertyProvider(final SensitivePropertyProvider sensitivePropertyProvider) {
        Objects.requireNonNull(sensitivePropertyProvider, "Provider required");

        final String identifierKey = sensitivePropertyProvider.getIdentifierKey();
        if (localProviderCache.containsKey(identifierKey)) {
            throw new UnsupportedOperationException(String.format("Sensitive Property Provider Identifier [%s] override not supported", identifierKey));
        }

        localProviderCache.put(identifierKey, sensitivePropertyProvider);
    }

    @Override
    public String toString() {
        return String.format("%s Properties [%d]", getClass().getSimpleName(), getPropertyKeys().size());
    }

    /**
     * If the value is protected, unprotects it and returns it. If not, returns the original value.
     *
     * @param key            the retrieved property key
     * @param retrievedValue the retrieved property value
     * @return the unprotected value
     */
    private String unprotectValue(final String key, final String retrievedValue) {
        // Checks if the key is sensitive and marked as protected
        if (isPropertyProtected(key)) {
            final String protectionSchemePath = getProperty(getProtectionKey(key));

            try {
                final SensitivePropertyProvider provider = findProvider(protectionSchemePath);
                return provider.unprotect(retrievedValue, ProtectedPropertyContext.defaultContext(key));
            } catch (final RuntimeException e) {
                throw new SensitivePropertyProtectionException(String.format("Property [%s] unprotect failed", key), e);
            }
        }
        return retrievedValue;
    }

    private SensitivePropertyProvider findProvider(final String protectionSchemePath) {
        Objects.requireNonNull(protectionSchemePath, "Protection Scheme Path required");
        return localProviderCache.entrySet()
                .stream()
                .filter(entry -> protectionSchemePath.startsWith(entry.getKey()))
                .findFirst()
                .map(Map.Entry::getValue)
                .orElseThrow(() -> new UnsupportedOperationException(String.format("Protection Scheme Path [%s] Provider not found", protectionSchemePath)));
    }

    private boolean isNotBlank(final String string) {
        return string != null && string.length() > 0;
    }
}
