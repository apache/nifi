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
package org.apache.nifi.registry.security.util;

import javax.crypto.Cipher;
import java.security.NoSuchAlgorithmException;

public class CryptoUtils {

    /**
     *  Required Cipher transformations according to Java SE 8 {@link Cipher} docs
     */
    private static final String[] standardCryptoTransformations = {
        "AES/CBC/NoPadding",
        "AES/CBC/PKCS5Padding",
        "AES/ECB/NoPadding",
        "AES/ECB/PKCS5Padding",
        "DES/CBC/NoPadding",
        "DES/CBC/PKCS5Padding",
        "DES/ECB/NoPadding",
        "DES/ECB/PKCS5Padding",
        "DESede/CBC/NoPadding",
        "DESede/CBC/PKCS5Padding",
        "DESede/ECB/NoPadding",
        "DESede/ECB/PKCS5Padding",
        "RSA/ECB/PKCS1Padding",
        "RSA/ECB/OAEPWithSHA-1AndMGF1Padding",
        "RSA/ECB/OAEPWithSHA-256AndMGF1Padding"
    };

    /**
     * Check if cryptographic strength available in this Java Runtime is restricted.
     *
     * Not every Java Platform supports "unlimited strength encryption",
     * so this convenience method provides a way to check if strength of crypto
     * functions (i.e., max key length) is unlimited or restricted in the
     * current Java runtime environment.
     *
     * @return true if it can be determined that max key lengths are less than unlimited
     *         false if key lengths are restricted
     *         null if max key length cannot be determined for any known Cipher transformations */
    public static Boolean isCryptoRestricted() {

        Boolean isCryptoRestricted = null;

        for (String transformation : standardCryptoTransformations) {
            try {
                return Cipher.getMaxAllowedKeyLength(transformation) < Integer.MAX_VALUE;
            } catch (final NoSuchAlgorithmException e) {
                // Unexpected as we are pulling from a list of transforms that every
                // java platform is required to support, but try the next one
            }
        }

        // Tried every standard Cipher transformation and none were available,
        // so crypto strength restrictions cannot be determined.
        return null;

    }

}
