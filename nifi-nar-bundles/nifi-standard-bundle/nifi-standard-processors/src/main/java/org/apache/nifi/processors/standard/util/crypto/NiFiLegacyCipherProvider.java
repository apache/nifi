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
package org.apache.nifi.processors.standard.util.crypto;

import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;

public class NiFiLegacyCipherProvider extends OpenSSLPKCS5CipherProvider implements PBECipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(NiFiLegacyCipherProvider.class);

    // Legacy magic number value
    private static final int ITERATION_COUNT = 1000;

    // TODO: Add deprecated annotation once support for legacy crypto is dropped

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using the NiFi legacy code, based on @see org.apache.nifi.processors.standard.util.crypto
     * .OpenSSLPKCS5CipherProvider#getCipher(java.lang.String, java.lang.String, java.lang.String, boolean) [essentially {@code MD5(password || salt) * 1000 }].
     *
     * @param algorithm   the algorithm name
     * @param provider    the provider name
     * @param password    the secret input
     * @param encryptMode true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(String algorithm, String provider, String password, boolean encryptMode) throws Exception {
        return getCipher(algorithm, provider, password, new byte[0], encryptMode);
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The key (and IV if necessary) are derived using the NiFi legacy code, based on @see org.apache.nifi.processors.standard.util.crypto
     * .OpenSSLPKCS5CipherProvider#getCipher(java.lang.String, java.lang.String, java.lang.String, byte[], boolean) [essentially {@code MD5(password || salt) * 1000 }].
     *
     * @param algorithm   the algorithm name
     * @param provider    the provider name
     * @param password    the secret input
     * @param salt        the salt
     * @param encryptMode true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(String algorithm, String provider, String password, byte[] salt, boolean encryptMode) throws Exception {
        try {
            // This method is defined in the OpenSSL implementation and just uses a locally-overridden iteration count
            return getInitializedCipher(algorithm, provider, password, salt, encryptMode);
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    protected int getIterationCount() {
        return ITERATION_COUNT;
    }
}
