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
package org.apache.nifi.registry.properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.properties.ApplicationPropertiesProtector;
import org.apache.nifi.properties.ProtectedProperties;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.properties.SensitivePropertyProtector;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Arrays.asList;

/**
 * Decorator class for intermediate phase when {@link NiFiRegistryPropertiesLoader} loads the
 * raw properties file and performs unprotection activities before returning a clean
 * implementation of {@link NiFiRegistryProperties}.
 * This encapsulates the sensitive property access logic from external consumers
 * of {@code NiFiRegistryProperties}.
 */
class ProtectedNiFiRegistryProperties extends NiFiRegistryProperties implements ProtectedProperties<NiFiRegistryProperties>,
        SensitivePropertyProtector<ProtectedNiFiRegistryProperties, NiFiRegistryProperties> {
    private static final Logger logger = LoggerFactory.getLogger(ProtectedNiFiRegistryProperties.class);

    private SensitivePropertyProtector<ProtectedNiFiRegistryProperties, NiFiRegistryProperties> propertyProtectionDelegate;

    private NiFiRegistryProperties applicationProperties;

    // Additional "sensitive" property key
    public static final String ADDITIONAL_SENSITIVE_PROPERTIES_KEY = "nifi.registry.sensitive.props.additional.keys";

    // Default list of "sensitive" property keys
    public static final List<String> DEFAULT_SENSITIVE_PROPERTIES = new ArrayList<>(asList(
            NiFiRegistryProperties.SECURITY_KEY_PASSWD,
            NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD,
            NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD));

    public ProtectedNiFiRegistryProperties() {
        this(new NiFiRegistryProperties());
    }

    /**
     * Creates an instance containing the provided {@link NiFiRegistryProperties}.
     *
     * @param props the NiFiRegistryProperties to contain
     */
    public ProtectedNiFiRegistryProperties(final NiFiRegistryProperties props) {
        this.applicationProperties = props;
        this.propertyProtectionDelegate = new ApplicationPropertiesProtector<>(this);
        logger.debug("Loaded {} properties (including {} protection schemes) into ProtectedNiFiRegistryProperties", getApplicationProperties()
                .getPropertyKeys().size(), getProtectedPropertyKeys().size());
    }

    @Override
    public String getAdditionalSensitivePropertiesKeys() {
        return getProperty(getAdditionalSensitivePropertiesKeysName());
    }

    @Override
    public String getAdditionalSensitivePropertiesKeysName() {
        return ADDITIONAL_SENSITIVE_PROPERTIES_KEY;
    }

    @Override
    public List<String> getDefaultSensitiveProperties() {
        return DEFAULT_SENSITIVE_PROPERTIES;
    }

    /**
     * Returns the internal representation of the {@link NiFiRegistryProperties} -- protected
     * or not as determined by the current state. No guarantee is made to the
     * protection state of these properties. If the internal reference is null, a new
     * {@link NiFiRegistryProperties} instance is created.
     *
     * @return the internal properties
     */
    @Override
    public NiFiRegistryProperties getApplicationProperties() {
        if (this.applicationProperties == null) {
            this.applicationProperties = new NiFiRegistryProperties();
        }

        return this.applicationProperties;
    }

    @Override
    public NiFiRegistryProperties createApplicationProperties(final Properties rawProperties) {
        return new NiFiRegistryProperties(rawProperties);
    }

    /**
     * Retrieves the property value for the given property key.
     *
     * @param key the key of property value to lookup
     * @return value of property at given key or null if not found
     */
    @Override
    public String getProperty(String key) {
        return getApplicationProperties().getProperty(key);
    }

    /**
     * Retrieves all known property keys.
     *
     * @return all known property keys
     */
    @Override
    public Set<String> getPropertyKeys() {
        return propertyProtectionDelegate.getPropertyKeys();
    }

    /**
     * Returns the number of properties, excluding protection scheme properties.
     * <p>
     * Example:
     * <p>
     * key: E(value, key)
     * key.protected: aes/gcm/256
     * key2: value2
     * <p>
     * would return size 2
     *
     * @return the count of real properties
     */
    @Override
    public int size() {
        return propertyProtectionDelegate.size();
    }

    @Override
    public Set<String> getPropertyKeysIncludingProtectionSchemes() {
        return propertyProtectionDelegate.getPropertyKeysIncludingProtectionSchemes();
    }

    @Override
    public List<String> getSensitivePropertyKeys() {
        return propertyProtectionDelegate.getSensitivePropertyKeys();
    }

    @Override
    public List<String> getPopulatedSensitivePropertyKeys() {
        return propertyProtectionDelegate.getPopulatedSensitivePropertyKeys();
    }

    @Override
    public boolean hasProtectedKeys() {
        return propertyProtectionDelegate.hasProtectedKeys();
    }

    @Override
    public Map<String, String> getProtectedPropertyKeys() {
        return propertyProtectionDelegate.getProtectedPropertyKeys();
    }

    @Override
    public Set<String> getProtectionSchemes() {
        return propertyProtectionDelegate.getProtectionSchemes();
    }

    @Override
    public boolean isPropertySensitive(final String key) {
        return propertyProtectionDelegate.isPropertySensitive(key);
    }

    @Override
    public boolean isPropertyProtected(final String key) {
        return propertyProtectionDelegate.isPropertyProtected(key);
    }

    @Override
    public NiFiRegistryProperties getUnprotectedProperties() throws SensitivePropertyProtectionException {
        return propertyProtectionDelegate.getUnprotectedProperties();
    }

    @Override
    public void addSensitivePropertyProvider(final SensitivePropertyProvider sensitivePropertyProvider) {
        propertyProtectionDelegate.addSensitivePropertyProvider(sensitivePropertyProvider);
    }

    @Override
    public Map<String, SensitivePropertyProvider> getSensitivePropertyProviders() {
        return propertyProtectionDelegate.getSensitivePropertyProviders();
    }

    @Override
    public String toString() {
        final Set<String> providers = getSensitivePropertyProviders().keySet();
        return new StringBuilder("ProtectedNiFiRegistryProperties instance with ")
                .append(size()).append(" properties (")
                .append(getProtectedPropertyKeys().size())
                .append(" protected) and ")
                .append(providers.size())
                .append(" sensitive property providers: ")
                .append(StringUtils.join(providers, ", "))
                .toString();
    }
}
