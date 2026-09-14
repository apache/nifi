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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Factory for the Sensitive Property Context supplied to Property Encryption Providers.
 *
 * <p>A Provider may bind the context to the encrypted value, so the context supplied when decrypting a value must equal
 * the context supplied when the value was encrypted. Encryption and decryption call sites therefore build contexts
 * through this factory rather than assembling attribute maps directly.</p>
 *
 * <p>Attributes with a null value are omitted, which allows call sites that cannot resolve an attribute to produce the
 * same context on both sides of the operation.</p>
 */
public final class SensitivePropertyContextFactory {

    private static final String PROXY_PASSWORD_PROPERTY_NAME = "Proxy Password";

    private SensitivePropertyContextFactory() {
    }

    /**
     * Get the context for a sensitive property configured on a flow component
     *
     * @param componentId Identifier of the component instance that owns the property
     * @param componentType Type of the component that owns the property
     * @param propertyName Name of the property
     * @return Sensitive Property Context
     */
    public static SensitivePropertyContext forComponent(final String componentId, final String componentType, final String propertyName) {
        final Map<String, String> attributes = new LinkedHashMap<>();
        putAttribute(attributes, SensitivePropertyAttribute.COMPONENT_ID, componentId);
        putAttribute(attributes, SensitivePropertyAttribute.COMPONENT_TYPE, componentType);
        putAttribute(attributes, SensitivePropertyAttribute.PROPERTY_NAME, propertyName);
        return new SensitivePropertyContext(SensitivePropertyCategory.COMPONENT_PROPERTY, attributes);
    }

    /**
     * Get the context for the proxy password of a Remote Process Group. A Remote Process Group is not a configurable
     * extension, so the context carries no component type.
     *
     * @param remoteProcessGroupId Identifier of the Remote Process Group instance
     * @return Sensitive Property Context
     */
    public static SensitivePropertyContext forRemoteProcessGroupProxyPassword(final String remoteProcessGroupId) {
        return forComponent(remoteProcessGroupId, null, PROXY_PASSWORD_PROPERTY_NAME);
    }

    /**
     * Get the context for a sensitive Parameter value
     *
     * @param parameterContextName Name of the Parameter Context that contains the Parameter
     * @param parameterName Name of the Parameter
     * @return Sensitive Property Context
     */
    public static SensitivePropertyContext forParameter(final String parameterContextName, final String parameterName) {
        final Map<String, String> attributes = new LinkedHashMap<>();
        putAttribute(attributes, SensitivePropertyAttribute.PARAMETER_CONTEXT_NAME, parameterContextName);
        putAttribute(attributes, SensitivePropertyAttribute.PARAMETER_NAME, parameterName);
        return new SensitivePropertyContext(SensitivePropertyCategory.PARAMETER, attributes);
    }

    /**
     * Get the context for an authorization token stored on behalf of an authenticated user. The context carries no
     * attributes, because nothing describing the user is known before the stored token has been decrypted.
     *
     * @return Sensitive Property Context
     */
    public static SensitivePropertyContext forAuthorizationToken() {
        return new SensitivePropertyContext(SensitivePropertyCategory.AUTHORIZATION_TOKEN, Map.of());
    }

    private static void putAttribute(final Map<String, String> attributes, final SensitivePropertyAttribute attribute, final String value) {
        if (value != null) {
            attributes.put(attribute.getKey(), value);
        }
    }
}
