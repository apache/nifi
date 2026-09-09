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
package org.apache.nifi.parameter;

import org.apache.nifi.security.encryption.PropertyEncryptionEncoder;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.security.encryption.SensitivePropertyCodec;
import org.apache.nifi.security.encryption.SensitivePropertyContext;
import org.apache.nifi.security.encryption.SensitivePropertyContextFactory;

import java.util.Objects;

/**
 * Standard implementation with encryptor for sensitive values
 */
public class StandardParameterValueMapper implements ParameterValueMapper {
    static final String PROVIDED_MAPPING = "provided:parameter";

    private final PropertyEncryptionProvider propertyEncryptionProvider;

    /**
     * Standard Parameter Value Mapper with the Property Encryption Provider
     *
     * @param propertyEncryptionProvider Provider applied to sensitive Parameter values, which may be null
     */
    public StandardParameterValueMapper(final PropertyEncryptionProvider propertyEncryptionProvider) {
        this.propertyEncryptionProvider = propertyEncryptionProvider;
    }

    /**
     * Get mapped Parameter value based on properties
     *
     * @param parameterContextName Name of the Parameter Context that contains the Parameter
     * @param parameter Parameter with descriptor of attributes for mapping
     * @param value Parameter value to be mapped
     * @return Mapped Parameter value
     */
    @Override
    public String getMapped(final String parameterContextName, final Parameter parameter, final String value) {
        Objects.requireNonNull(parameter, "Parameter required");

        final ParameterDescriptor descriptor = parameter.getDescriptor();
        final String mapped;

        if (value == null) {
            mapped = null;
        } else if (parameter.isProvided()) {
            mapped = PROVIDED_MAPPING;
        } else if (descriptor.isSensitive()) {
            mapped = getEncrypted(parameterContextName, descriptor.getName(), value);
        } else {
            mapped = value;
        }

        return mapped;
    }

    private String getEncrypted(final String parameterContextName, final String parameterName, final String value) {
        final String processed;

        if (propertyEncryptionProvider == null) {
            processed = value;
        } else {
            final SensitivePropertyContext context = SensitivePropertyContextFactory.forParameter(parameterContextName, parameterName);
            final String encrypted = SensitivePropertyCodec.encrypt(propertyEncryptionProvider, value, context);
            processed = PropertyEncryptionEncoder.getEncoded(encrypted);
        }

        return processed;
    }
}
