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
package org.apache.nifi.hazelcast.services.util;

/**
 * Helper methods to work with long values effectively.
 */
public final class LongUtil {
    private static final int BYTE_MASK = 0xFF;

    private LongUtil() {
        // no op
    }

    /**
     * Converts the incoming long into a byte array representation.
     *
     * Note: <code>BigInteger.valueOf(input).toByteArray()</code> behaves similar but without padding (and performance wise weaker)
     *
     * @param input Long value.
     *
     * @return Array representation of the input value. The array always has the length of 8, padded with zeros if not all the bytes are needed for representation.
     */
    public static byte[] toBytes(long input) {
        byte[] result = new byte[Long.BYTES];

        for (int i = 7; i >= 0; i--) {
            result[i] = (byte)(input & BYTE_MASK);
            input >>= Long.BYTES;
        }

        return result;
    }

    /**
     * Converts a byte array into long value.
     *
     * Note: <code>new BigInteger(bytes).longValue()</code> behaves similarHazelcastMapCacheClientTest.
     *
     * @param bytes The incoming byte value. If the byte array is longer than the necessary number of bytes (8) the first 8 will be used.
     *
     * @return The long value.
     */
    public static long fromBytes(final byte[] bytes) {

        long result = 0;
        for (int i = 0; i < Long.BYTES; i++) {
            result <<= Long.BYTES;
            result |= (bytes[i] & BYTE_MASK);
        }
        return result;
    }
}
