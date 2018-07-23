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

import java.util.HashMap;
import java.util.Map;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("deprecation")
public class CipherProviderFactory {
    private static final Logger logger = LoggerFactory.getLogger(CipherProviderFactory.class);

    private static Map<KeyDerivationFunction, Class<? extends CipherProvider>> registeredCipherProviders;

    static {
        registeredCipherProviders = new HashMap<>();
        registeredCipherProviders.put(KeyDerivationFunction.NIFI_LEGACY, org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider.class);
        registeredCipherProviders.put(KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY, OpenSSLPKCS5CipherProvider.class);
        registeredCipherProviders.put(KeyDerivationFunction.PBKDF2, PBKDF2CipherProvider.class);
        registeredCipherProviders.put(KeyDerivationFunction.BCRYPT, BcryptCipherProvider.class);
        registeredCipherProviders.put(KeyDerivationFunction.SCRYPT, ScryptCipherProvider.class);
        registeredCipherProviders.put(KeyDerivationFunction.NONE, AESKeyedCipherProvider.class);
    }

    public static CipherProvider getCipherProvider(KeyDerivationFunction kdf) {
        logger.debug("{} KDFs registered", registeredCipherProviders.size());

        if (registeredCipherProviders.containsKey(kdf)) {
            Class<? extends CipherProvider> clazz = registeredCipherProviders.get(kdf);
            try {
                return clazz.newInstance();
            } catch (Exception e) {
               logger.error("Error instantiating new {} with default parameters for {}", clazz.getName(), kdf.getName());
                throw new ProcessException("Error instantiating cipher provider");
            }
        }

        throw new IllegalArgumentException("No cipher provider registered for " + kdf.getName());
    }
}
