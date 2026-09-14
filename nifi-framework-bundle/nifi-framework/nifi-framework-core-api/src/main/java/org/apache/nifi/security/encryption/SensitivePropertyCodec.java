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

import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.Objects;

/**
 * Codec that adapts the byte-oriented Property Encryption Provider to the string representation of sensitive values
 * stored in the flow configuration.
 *
 * <p>Values are encoded as UTF-8 before encryption and encrypted values are represented as hexadecimal, matching the
 * representation written inside the {@code enc{}} wrapper of persisted flows.</p>
 */
public final class SensitivePropertyCodec {
    private static final HexFormat HEX_FORMAT = HexFormat.of();

    private SensitivePropertyCodec() {
    }

    /**
     * Encrypt a sensitive value and return the hexadecimal representation of the encrypted value
     *
     * @param provider Property Encryption Provider
     * @param value Sensitive value to be encrypted
     * @param context Context describing the value being protected
     * @return Hexadecimal representation of the encrypted value
     * @throws PropertyEncryptionException Thrown when encryption fails
     */
    public static String encrypt(final PropertyEncryptionProvider provider, final String value, final SensitivePropertyContext context) {
        Objects.requireNonNull(provider, "Property Encryption Provider required");
        Objects.requireNonNull(value, "Value required");

        final byte[] encrypted = provider.encrypt(value.getBytes(StandardCharsets.UTF_8), context);
        return HEX_FORMAT.formatHex(encrypted);
    }

    /**
     * Decrypt the hexadecimal representation of an encrypted sensitive value
     *
     * @param provider Property Encryption Provider
     * @param encryptedValue Hexadecimal representation of the encrypted value
     * @param context Context describing the value being protected, which must equal the context supplied on encryption
     * @return Decrypted sensitive value
     * @throws PropertyEncryptionException Thrown when decryption fails
     */
    public static String decrypt(final PropertyEncryptionProvider provider, final String encryptedValue, final SensitivePropertyContext context) {
        Objects.requireNonNull(provider, "Property Encryption Provider required");
        Objects.requireNonNull(encryptedValue, "Encrypted value required");

        final byte[] encrypted;
        try {
            encrypted = HEX_FORMAT.parseHex(encryptedValue);
        } catch (final IllegalArgumentException e) {
            throw new PropertyEncryptionException("Sensitive property is not a valid hexadecimal encrypted value", e);
        }

        final byte[] decrypted = provider.decrypt(encrypted, context);
        return new String(decrypted, StandardCharsets.UTF_8);
    }
}
