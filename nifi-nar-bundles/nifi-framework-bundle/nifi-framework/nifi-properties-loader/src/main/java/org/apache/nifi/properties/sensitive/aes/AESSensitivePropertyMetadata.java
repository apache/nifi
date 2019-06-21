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
package org.apache.nifi.properties.sensitive.aes;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import javax.crypto.Cipher;
import org.apache.nifi.properties.sensitive.SensitivePropertyMetadata;
import org.apache.nifi.properties.sensitive.SensitivePropertyProtectionException;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the metadata container for AES/GCM encryption using {@link AESSensitivePropertyProvider} which contains the algorithm & mode (fixed) and the key length used (variable).
 */
public class AESSensitivePropertyMetadata implements SensitivePropertyMetadata {
    private static final Logger logger = LoggerFactory.getLogger(AESSensitivePropertyMetadata.class);

    private static final List<Integer> VALID_KEY_LENGTHS = getValidKeyLengths();
    private static final String AES_GCM = "aes/gcm";

    private int keyLength;
    private String algorithmAndMode;

    AESSensitivePropertyMetadata(String algorithmAndMode, int keyLength) {
        if (!AES_GCM.equalsIgnoreCase(algorithmAndMode)) {
            throw new SensitivePropertyProtectionException("The only algorithm and mode supported is aes/gcm");
        }
        this.algorithmAndMode = AES_GCM;
        if (VALID_KEY_LENGTHS.contains(keyLength)) {
            this.keyLength = keyLength;
        } else {
            throw new SensitivePropertyProtectionException("Key length " + keyLength + " not valid [" +
                    StringUtils.join(VALID_KEY_LENGTHS.stream().map(String::valueOf).collect(Collectors.toList()), ",") + "]");
        }
    }

    /**
     * Returns an implementation of {@link SensitivePropertyMetadata} for AES/GCM encryption. Accepts the identifier indicating the key length used.
     *
     * @param identifier the identifier from the properties file (i.e. {@code aes/gcm/128})
     * @return the metadata for encryption/decryption
     * @throws SensitivePropertyProtectionException if the identifier cannot be parsed
     */
    public static AESSensitivePropertyMetadata fromIdentifier(String identifier) throws SensitivePropertyProtectionException {
        try {
            return new AESSensitivePropertyMetadata(
                    identifier.substring(0, identifier.lastIndexOf("/")),
                    Integer.valueOf(identifier.substring(identifier.lastIndexOf("/") + 1)));
        } catch (NullPointerException | StringIndexOutOfBoundsException e) {
            throw new SensitivePropertyProtectionException("Could not parse " + identifier + " to algorithm & mode and key length");
        }
    }

    /**
     * Returns the key length used in bits.
     *
     * @return the key length
     */
    public int getKeyLength() {
        return keyLength;
    }

    /**
     * Returns the algorithm and mode used formatted for use in {@code .protected} property.
     * <p>
     * Example: {@code aes/gcm}
     *
     * @return the algorithm & mode
     */
    public String getAlgorithmAndMode() {
        return algorithmAndMode;
    }

    /**
     * Returns the string used in {@code .protected} property to indicate how this property was protected.
     * <p>
     * Example: {@code aes/gcm/256}
     *
     * @return the identifier
     */
    public String getIdentifier() {
        return algorithmAndMode + "/" + keyLength;
    }

    @Override
    public String toString() {
        return "AESSensitivePropertyMetadata{" +
                "identifier=" + getIdentifier() +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AESSensitivePropertyMetadata that = (AESSensitivePropertyMetadata) o;
        return keyLength == that.keyLength && Objects.equals(algorithmAndMode, that.algorithmAndMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyLength, algorithmAndMode);
    }

    /**
     * Returns a list of the valid key lengths in bits. Determines this by checking the {@code Cipher} behavior. This should not be required in Java 11, but is required for Java 8 before release u162.
     *
     * @return an unmodifiable list of the valid key lengths in ascending order
     */
    private static List<Integer> getValidKeyLengths() {
        try {
            if (Cipher.getMaxAllowedKeyLength("AES") > 128) {
                return Collections.unmodifiableList(Arrays.asList(128, 192, 256));
            }
        } catch (NoSuchAlgorithmException e) {
            logger.error("Encountered an error: ", e);
        }
        return Collections.unmodifiableList(Collections.singletonList(128));
    }
}
