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
package org.apache.nifi.security.util.crypto;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractEncryptor implements EncryptContent.Encryptor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEncryptor.class);

    private static final String MODIFIABLE_CHECK_KEY = "__NiFi_EncryptContent_Check__";
    protected transient Map<String, String> flowfileAttributes = new HashMap<>();

    /**
     * Updates the provided flowfile attribute map with the new attributes from this encryption/decryption operation.
     *
     * @param attributes a modifiable map of attributes
     */
    @Override
    public void updateAttributes(Map<String, String> attributes) {
        if (attributes == null) {
            throw new IllegalArgumentException("Cannot update null flowfile attributes");
        }

        try {
            attributes.put(MODIFIABLE_CHECK_KEY, "");
            attributes.remove(MODIFIABLE_CHECK_KEY);
        } catch (UnsupportedOperationException e) {
            throw new IllegalArgumentException("The provided attributes map must be modifiable");
        }

        attributes.putAll(flowfileAttributes);
        if (logger.isDebugEnabled()) {
            logger.debug("Added {} encryption metadata attributes to flowfile", flowfileAttributes.size());
        }
    }

    /**
     * Returns the map of "standard" {@code EncryptContent} attributes set for the provided operation.
     *
     * @param encryptionMethod the algorithm used
     * @param kdf              the {@link KeyDerivationFunction} used
     * @param iv               the Initialization Vector in byte[] form
     * @param bcis             the {@link ByteCountingInputStream} (plaintext for {@code encrypt}; cipher text for {@code decrypt})
     * @param bcos             the {@link ByteCountingOutputStream} (reverse of input stream)
     * @param encryptMode      {@code true} for {@code encrypt}; {@code false} for {@code decrypt}
     * @return the map of attributes to be added to the flowfile
     */
    static Map<String, String> writeAttributes(EncryptionMethod encryptionMethod,
                                               KeyDerivationFunction kdf, byte[] iv, ByteCountingInputStream bcis, ByteCountingOutputStream bcos, boolean encryptMode) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(EncryptContent.ACTION_ATTR, encryptMode ? "encrypted" : "decrypted");
        attributes.put(EncryptContent.ALGORITHM_ATTR, encryptionMethod.name());
        attributes.put(EncryptContent.KDF_ATTR, kdf.name());
        attributes.put(EncryptContent.TS_ATTR, CipherUtility.getTimestampString());
        attributes.put(EncryptContent.IV_ATTR, iv != null ? Hex.encodeHexString(iv) : "N/A");
        attributes.put(EncryptContent.IV_LEN_ATTR, iv != null ? String.valueOf(iv.length) : "0");

        // If encrypting, the plaintext is the input and the cipher text is the output
        if (encryptMode) {
            attributes.put(EncryptContent.PT_LEN_ATTR, String.valueOf(bcis.getBytesRead()));
            attributes.put(EncryptContent.CT_LEN_ATTR, String.valueOf(bcos.getBytesWritten()));
        } else {
            // If decrypting, switch the streams
            attributes.put(EncryptContent.PT_LEN_ATTR, String.valueOf(bcos.getBytesWritten()));
            attributes.put(EncryptContent.CT_LEN_ATTR, String.valueOf(bcis.getBytesRead()));
        }

        if (logger.isDebugEnabled()) {
            final ArrayList<String> sortedKeys = new ArrayList<>(attributes.keySet());
            Collections.sort(sortedKeys);
            for (String k : sortedKeys) {
                logger.debug("Added encryption metadata attribute: {} -> {}", k, attributes.get(k));
            }
        }

        return attributes;
    }
}
