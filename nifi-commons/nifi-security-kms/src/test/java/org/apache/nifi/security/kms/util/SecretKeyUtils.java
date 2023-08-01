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
package org.apache.nifi.security.kms.util;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;

public class SecretKeyUtils {
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final String KEY_ALGORITHM = "AES";

    private static final int KEY_LENGTH = 32;
    /**
     * Get Random AES Secret Key
     *
     * @return Secret Key
     */
    public static SecretKey getSecretKey() {
        final byte[] encodedKey = new byte[KEY_LENGTH];
        SECURE_RANDOM.nextBytes(encodedKey);
        return new SecretKeySpec(encodedKey, KEY_ALGORITHM);
    }
}
