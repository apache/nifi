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
package org.apache.nifi.security.crypto.key.bcrypt;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Base64 Decoder translates from bcrypt Base64 characters to RFC 4648 characters
 */
class BcryptBase64Decoder {
    /** Alphabet of shared characters following common ordering */
    private static final String SHARED_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

    /** bcrypt alphabet defined according to the OpenBSD bcrypt function beginning with control characters */
    private static final String BCRYPT_ALPHABET = String.format("./%s", SHARED_ALPHABET);

    /** Standard alphabet defined according to RFC 4648 ending with control characters */
    private static final String STANDARD_ALPHABET = String.format("%s+/", SHARED_ALPHABET);

    private static final Map<Character, Character> BCRYPT_STANDARD_CHARACTERS = new LinkedHashMap<>();

    private static final Base64.Decoder decoder = Base64.getDecoder();

    static {
        final char[] bcryptCharacters = BCRYPT_ALPHABET.toCharArray();
        final char[] standardCharacters = STANDARD_ALPHABET.toCharArray();

        for (int i = 0; i < bcryptCharacters.length; i++) {
            final char bcryptCharacter = bcryptCharacters[i];
            final char standardCharacter = standardCharacters[i];
            BCRYPT_STANDARD_CHARACTERS.put(bcryptCharacter, standardCharacter);
        }
    }

    /**
     * Decode bcrypt Base64 encoded ASCII characters to support reading salt and hash strings
     *
     * @param encoded ASCII string of characters encoded using bcrypt Base64 characters
     * @return Decoded bytes
     */
    static byte[] decode(final String encoded) {
        final int encodedLength = encoded.length();
        final byte[] converted = new byte[encodedLength];
        for (int i = 0; i < encodedLength; i++) {
            final char encodedCharacter = encoded.charAt(i);
            final char standardCharacter = getStandardCharacter(encodedCharacter);
            converted[i] = (byte) standardCharacter;
        }
        return decoder.decode(converted);
    }

    private static char getStandardCharacter(final char encodedCharacter) {
        final Character standardCharacter = BCRYPT_STANDARD_CHARACTERS.get(encodedCharacter);
        if (standardCharacter == null) {
            final String message = String.format("Encoded character [%c] not supported for bcrypt Base64 decoding", encodedCharacter);
            throw new IllegalArgumentException(message);
        }
        return standardCharacter;
    }
}
