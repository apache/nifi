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

import java.util.Objects;

/**
 * Encoder for the standard representation of encrypted sensitive values in serialized flows.
 *
 * <p>Encrypted values are wrapped with the {@code enc{}} prefix and suffix. The wrapper distinguishes an encrypted
 * value from a value stored as plaintext, which occurs when a sensitive property references a Parameter or when a flow
 * is mapped without a Property Encryption Provider.</p>
 */
public final class PropertyEncryptionEncoder {

    private static final String PREFIX = "enc{";

    private static final String SUFFIX = "}";

    private PropertyEncryptionEncoder() {

    }

    /**
     * Determine whether a value is encoded as an encrypted value
     *
     * @param value Value to be evaluated, which may be null
     * @return Whether the value is wrapped with the encrypted value prefix and suffix
     */
    public static boolean isEncrypted(final String value) {
        return value != null && value.startsWith(PREFIX) && value.endsWith(SUFFIX);
    }

    /**
     * Get an encrypted value wrapped with the encrypted value prefix and suffix
     *
     * @param encryptedValue Encrypted value to be wrapped
     * @return Encrypted value wrapped with the prefix and suffix
     */
    public static String getEncoded(final String encryptedValue) {
        Objects.requireNonNull(encryptedValue, "Encrypted value required");
        return PREFIX + encryptedValue + SUFFIX;
    }

    /**
     * Get an encrypted value with the encrypted value prefix and suffix removed
     *
     * @param encodedValue Encrypted value wrapped with the prefix and suffix
     * @return Encrypted value without the prefix and suffix
     * @throws IllegalArgumentException Thrown when the value is not wrapped with the prefix and suffix
     */
    public static String getDecoded(final String encodedValue) {
        if (isEncrypted(encodedValue)) {
            return encodedValue.substring(PREFIX.length(), encodedValue.length() - SUFFIX.length());
        }

        throw new IllegalArgumentException("Value not encoded with required prefix and suffix delimiters");
    }
}
