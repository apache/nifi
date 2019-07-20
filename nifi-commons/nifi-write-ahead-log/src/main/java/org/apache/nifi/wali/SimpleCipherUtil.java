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
package org.apache.nifi.wali;

import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.AEADBlockCipher;
import org.bouncycastle.crypto.modes.EAXBlockCipher;
import org.bouncycastle.crypto.params.AEADParameters;
import org.bouncycastle.crypto.params.KeyParameter;

import javax.crypto.SecretKey;
import java.security.SecureRandom;

/**
 * Utility class for various cipher values and methods.
 *
 */
public class SimpleCipherUtil {
    // NB:  the cipher output stream uses this value to distinguish cipher text streams from (probably) plain text streams.
    // Using a magic value is suboptimal but for our use cases it gives us a measure of predictability:
    static byte MARKER_BYTE = 0x7f;
    static int IV_BYTE_LEN = 20;
    static int AAD_BYTE_LEN = 32;
    static int MAC_BIT_LEN = 128;

    public static final String ALGO = "AES";
    static SecureRandom random = new SecureRandom();

    /**
     * Construct, initialize, and return a new block cipher suitable for use as a stream cipher.
     *
     * @param key cipher key.
     * @param encrypt true if encrypting, false if decrypting.
     * @param iv initial vector bytes.
     * @param aad additional authentication data bytes.
     * @return AEADBlockCipher instance.
     */
    static AEADBlockCipher initCipher(SecretKey key, boolean encrypt, byte[] iv, byte[] aad) {
        if (key == null || iv == null || aad == null) {
            return null;
        }
        final AEADParameters param = new AEADParameters(new KeyParameter(key.getEncoded()), MAC_BIT_LEN, iv, aad);
        AEADBlockCipher cipher = new EAXBlockCipher(new AESEngine());
        cipher.init(encrypt, param);
        return cipher;
    }

    /**
     * @return byte array suitable for use as an IV.
     */
    static byte[] createIV() {
        return randomBytes(IV_BYTE_LEN);
    }

    /**
     * @return byte array suitable for use as AAD.
     */
    static byte[] createAAD() {
        return randomBytes(AAD_BYTE_LEN);
    }

    /**
     * Generate a byte array filled with random data.
     *
     * @param length length of new array
     * @return byte array filled with random data.
     */
    public static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        return bytes;
    }
}
