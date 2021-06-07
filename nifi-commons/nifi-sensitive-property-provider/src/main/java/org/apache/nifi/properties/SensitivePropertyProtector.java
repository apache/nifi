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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Encapsulates methods needed to protect application properties.
 * @param <T> The ProtectedProperties type
 * @param <U> The ApplicationProperties type
 */
public interface SensitivePropertyProtector<T extends ProtectedProperties<U>, U extends ApplicationProperties> {

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
    int size();

    /**
     * Retrieves all known property keys.
     *
     * @return all known property keys
     */
    Set<String> getPropertyKeys();

    /**
     * Returns the complete set of property keys, including any protection keys (i.e. 'x.y.z.protected').
     *
     * @return the set of property keys
     */
    Set<String> getPropertyKeysIncludingProtectionSchemes();

    /**
     * Returns a list of the keys identifying "sensitive" properties. There is a default list,
     * and additional keys can be provided in the {@code nifi.sensitive.props.additional.keys} property in the ApplicationProperties.
     *
     * @return the list of sensitive property keys
     */
    List<String> getSensitivePropertyKeys();

    /**
     * Returns a list of the keys identifying "sensitive" properties. There is a default list,
     * and additional keys can be provided in the {@code nifi.sensitive.props.additional.keys} property in the ApplicationProperties.
     *
     * @return the list of sensitive property keys
     */
    List<String> getPopulatedSensitivePropertyKeys();

    /**
     * Returns true if any sensitive keys are protected.
     *
     * @return true if any key is protected; false otherwise
     */
    boolean hasProtectedKeys();

    /**
     * Returns the unique set of all protection schemes currently in use for this instance.
     *
     * @return the set of protection schemes
     */
    Set<String> getProtectionSchemes();

    /**
     * Returns a Map of the keys identifying "sensitive" properties that are currently protected and the "protection" key for each.
     * This may or may not include all properties marked as sensitive.
     *
     * @return the Map of protected property keys and the protection identifier for each
     */
    Map<String, String> getProtectedPropertyKeys();

    /**
     * Returns the local provider cache (null-safe) as a Map of protection schemes -> implementations.
     *
     * @return the map
     */
    Map<String, SensitivePropertyProvider> getSensitivePropertyProviders();

    /**
     * Returns true if the property identified by this key is considered sensitive in this instance of {@code ApplicationProperties}.
     * Some properties are sensitive by default, while others can be specified by
     * {@link ProtectedProperties#getAdditionalSensitivePropertiesKeys()}.
     *
     * @param key the key
     * @return true if it is sensitive
     * @see ApplicationPropertiesProtector#getSensitivePropertyKeys()
     */
    boolean isPropertySensitive(String key);

    /**
     * Returns the unprotected {@link ApplicationProperties} instance. If none of the properties
     * loaded are marked as protected, it will simply pass through the internal instance.
     * If any are protected, it will drop the protection scheme keys and translate each
     * protected value (encrypted, HSM-retrieved, etc.) into the raw value and store it
     * under the original key.
     * <p>
     * If any property fails to unprotect, it will save that key and continue. After
     * attempting all properties, it will throw an exception containing all failed
     * properties. This is necessary because the order is not enforced, so all failed
     * properties should be gathered together.
     *
     * @return the ApplicationProperties instance with all raw values
     * @throws SensitivePropertyProtectionException if there is a problem unprotecting one or more keys
     */
    boolean isPropertyProtected(String key);

    /**
     * Returns the unprotected ApplicationProperties.
     * @return The unprotected properties
     * @throws SensitivePropertyProtectionException if there is a problem unprotecting one or more keys
     */
    U getUnprotectedProperties() throws SensitivePropertyProtectionException;

    /**
     * Registers a new {@link SensitivePropertyProvider}. This method will throw a {@link UnsupportedOperationException}
     * if a provider is already registered for the protection scheme.
     *
     * @param sensitivePropertyProvider the provider
     */
    void addSensitivePropertyProvider(SensitivePropertyProvider sensitivePropertyProvider);
}
