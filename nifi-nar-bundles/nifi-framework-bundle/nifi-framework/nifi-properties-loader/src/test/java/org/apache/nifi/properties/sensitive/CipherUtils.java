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
package org.apache.nifi.properties.sensitive;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.util.encoders.Hex;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;


/**
 * Common functionality for ciphers:  pre-initialized ciphers, IVs, random value generators, etc.
 */
public class CipherUtils {
    final static SecureRandom random = new SecureRandom();
    // IV of 12 bytes is recommended for AES/GCM.  See https://crypto.stackexchange.com/questions/41601/aes-gcm-recommended-iv-size-why-12-bytes
    public final static int IV_LENGTH = 12;

    /**
     * Generates a new random IV of 12 bytes using {@link java.security.SecureRandom}.
     *
     * @return the IV
     */
    public static byte[] generateIV() {
        byte[] bytes = new byte[IV_LENGTH];
        random.nextBytes(bytes);
        return bytes;
    }

    /**
     * Generates an IV of 12 bytes filled with zeros.
     *
     * @return the IV
     */
    public static byte[] zeroIV() {
        byte[] bytes = new byte[IV_LENGTH];
        // All bytes are initialized to zero, but if they were not, we would do this:
        // Arrays.fill(bytes, (byte) 0);
        return bytes;
    }


    /**
     * Generates an un-initialized AES/GCM cipher.
     *
     * @return AES/GCM/NoPadding cipher, un-initialized
     */
    public static Cipher blockCipher() throws NoSuchPaddingException, NoSuchAlgorithmException, NoSuchProviderException {
        Security.addProvider(new BouncyCastleProvider());
        return Cipher.getInstance("AES/GCM/NoPadding", "BC");
    }

    /**
     * Generates a string of random hex characters.
     *
     * @param size Number of bytes to generate; hex string will be 2x as long as this.
     * @return string of random hex values.
     */
    public static String getRandomHex(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("Random hex string size too small");
        }
        if (size*2 >= Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Random hex string size too large");
        }
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return Hex.toHexString(bytes);
    }

    /**
     * Generates a random int within the given range.
     * @param lower lower range
     * @param upper upper range
     * @return integer value such that upper >= value >= lower
     */
    public static int getRandomInt(int lower, int upper) {
        return random.nextInt(upper - lower) + lower;
    }
}
