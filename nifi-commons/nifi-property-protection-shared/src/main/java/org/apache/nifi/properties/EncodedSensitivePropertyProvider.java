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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/**
 * Encoded Sensitive Property Provider handles Base64 encoding and decoding of property values
 */
public abstract class EncodedSensitivePropertyProvider implements SensitivePropertyProvider {
    private static final Charset VALUE_CHARACTER_SET = StandardCharsets.UTF_8;

    private static final Base64.Encoder ENCODER = Base64.getEncoder().withoutPadding();

    private static final Base64.Decoder DECODER = Base64.getDecoder();

    /**
     * Protect property value and return Base64-encoded representation of encrypted bytes
     *
     * @param unprotectedValue Unprotected property value to be encrypted
     * @param context Property Context
     * @return Base64-encoded representation of encrypted bytes
     */
    @Override
    public String protect(final String unprotectedValue, final ProtectedPropertyContext context) {
        Objects.requireNonNull(unprotectedValue, "Value required");
        Objects.requireNonNull(context, "Context required");
        try {
            final byte[] bytes = unprotectedValue.getBytes(VALUE_CHARACTER_SET);
            final byte[] encrypted = getEncrypted(bytes);
            return ENCODER.encodeToString(encrypted);
        } catch (final RuntimeException e) {
            final String message = String.format("Property [%s] Encryption Failed", context.getContextKey());
            throw new SensitivePropertyProtectionException(message, e);
        }
    }

    /**
     * Unprotect Base64-encoded representation of encrypted property value and return string
     *
     * @param protectedValue Base64-encoded representation of encrypted bytes
     * @param context Property Context
     * @return Decrypted property value string
     */
    @Override
    public String unprotect(final String protectedValue, final ProtectedPropertyContext context) {
        Objects.requireNonNull(protectedValue, "Value required");
        Objects.requireNonNull(context, "Context required");
        try {
            final byte[] decoded = DECODER.decode(protectedValue);
            final byte[] decrypted = getDecrypted(decoded);
            return new String(decrypted, VALUE_CHARACTER_SET);
        } catch (final RuntimeException e) {
            final String message = String.format("Property [%s] Decryption Failed", context.getContextKey());
            throw new SensitivePropertyProtectionException(message, e);
        }
    }

    /**
     * Get encrypted byte array representation of bytes
     *
     * @param bytes Unprotected bytes
     * @return Encrypted bytes
     */
    protected abstract byte[] getEncrypted(byte[] bytes);

    /**
     * Get decrypted byte array representation of encrypted bytes
     *
     * @param bytes Encrypted bytes
     * @return Decrypted bytes
     */
    protected abstract byte[] getDecrypted(byte[] bytes);
}
