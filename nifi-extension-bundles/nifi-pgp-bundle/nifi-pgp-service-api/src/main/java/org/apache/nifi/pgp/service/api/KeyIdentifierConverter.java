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
package org.apache.nifi.pgp.service.api;

import java.math.BigInteger;

/**
 * Key Identifier Converter from number to hexadecimal string
 */
public class KeyIdentifierConverter {
    private static final int HEXADECIMAL_RADIX = 16;

    private static final String KEY_ID_FORMAT = "%016X";

    /**
     * Format numeric key identifier as uppercase hexadecimal string
     *
     * @param keyId Key Identifier
     * @return Uppercase hexadecimal string
     */
    public static String format(final long keyId) {
        return String.format(KEY_ID_FORMAT, keyId);
    }

    /**
     * Parse hexadecimal key identifier to numeric key identifier
     *
     * @param keyId Hexadecimal string
     * @return Key Identifier
     */
    public static long parse(final String keyId) {
        final BigInteger parsed = new BigInteger(keyId, HEXADECIMAL_RADIX);
        return parsed.longValue();
    }
}
