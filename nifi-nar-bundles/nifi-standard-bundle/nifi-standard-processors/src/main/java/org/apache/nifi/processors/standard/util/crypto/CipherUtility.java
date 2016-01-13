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

import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CipherUtility {
    /**
     * Returns the cipher algorithm from the full algorithm name. Useful for getting key lengths, etc.
     * <p>
     * Ex: PBEWITHMD5AND128BITAES-CBC-OPENSSL -> AES
     *
     * @param algorithm the full algorithm name
     * @return the generic cipher name or the full algorithm if one cannot be extracted
     */
    public static String parseCipherFromAlgorithm(final String algorithm) {
        // This is not optimal but the algorithms do not have a standard format
        final String AES = "AES";
        final String TDES = "TRIPLEDES";
        final String DES = "DES";
        final String RC4 = "RC4";
        final String RC2 = "RC2";
        final String TWOFISH = "TWOFISH";
        final List<String> SYMMETRIC_CIPHERS = Arrays.asList(AES, TDES, DES, RC4, RC2, TWOFISH);

        // The algorithms contain "TRIPLEDES" but the cipher name is "DESede"
        final String ACTUAL_TDES_CIPHER = "DESede";

        for (String cipher : SYMMETRIC_CIPHERS) {
            if (algorithm.contains(cipher)) {
                if (cipher.equals(TDES)) {
                    return ACTUAL_TDES_CIPHER;
                } else {
                    return cipher;
                }
            }
        }

        return algorithm;
    }

    /**
     * Returns the cipher key length from the full algorithm name. Useful for getting key lengths, etc.
     * <p>
     * Ex: PBEWITHMD5AND128BITAES-CBC-OPENSSL -> 128
     *
     * @param algorithm the full algorithm name
     * @return the key length or -1 if one cannot be extracted
     */
    public static int parseKeyLengthFromAlgorithm(final String algorithm) {
        Pattern pattern = Pattern.compile("([\\d]+)BIT");
        Matcher matcher = pattern.matcher(algorithm);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            // Key length not explicitly named in algorithm
            String cipher = parseCipherFromAlgorithm(algorithm);
            return getDefaultKeyLengthForCipher(cipher);
        }
    }

    private static int getDefaultKeyLengthForCipher(String cipher) {
        // TODO: Clean up
        if (StringUtils.isEmpty(cipher)) {
            return -1;
        }
        cipher = cipher.toUpperCase();
        switch (cipher) {
            case "DESEDE":
                return 112;
            case "DES":
                return 64;
            case "RC2":
            case "RC4":
            case "RC5":
            default:
                return 128;
        }
    }
}
