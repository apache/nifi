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
package org.apache.nifi.processors.standard.crypto;

import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processors.standard.crypto.algorithm.CryptographicAlgorithm;
import org.apache.nifi.processors.standard.crypto.attributes.CryptographicAttributeKey;
import org.apache.nifi.processors.standard.crypto.attributes.CryptographicMethod;
import org.apache.nifi.security.util.crypto.CipherUtility;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.Security;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract Cryptographic Processor for methods shared across implementations
 */
public abstract class AbstractCryptographicProcessor extends AbstractProcessor {
    static {
        Security.addProvider(new BouncyCastleProvider());
    }

    /**
     * Get Cryptographic Flow File Attributes based on resolved Cryptographic Algorithm
     *
     * @param algorithm Cryptographic Algorithm
     * @return Flow File Attributes
     */
    protected Map<String, String> getCryptographicAttributes(final CryptographicAlgorithm algorithm) {
        final Map<String, String> attributes = new HashMap<>();

        attributes.put(CryptographicAttributeKey.ALGORITHM.key(), algorithm.toString());
        attributes.put(CryptographicAttributeKey.ALGORITHM_CIPHER.key(), algorithm.getCipher().getLabel());
        attributes.put(CryptographicAttributeKey.ALGORITHM_KEY_SIZE.key(), Integer.toString(algorithm.getKeySize()));
        attributes.put(CryptographicAttributeKey.ALGORITHM_BLOCK_CIPHER_MODE.key(), algorithm.getBlockCipherMode().getLabel());
        attributes.put(CryptographicAttributeKey.ALGORITHM_OBJECT_IDENTIFIER.key(), algorithm.getObjectIdentifier());

        attributes.put(CryptographicAttributeKey.METHOD.key(), getCryptographicMethod().toString());
        attributes.put(CryptographicAttributeKey.PROCESSING_COMPLETED.key(), CipherUtility.getTimestampString());
        return attributes;
    }

    /**
     * Get Cryptographic Method definition for implementing Processors
     *
     * @return Cryptographic Method
     */
    protected abstract CryptographicMethod getCryptographicMethod();
}
