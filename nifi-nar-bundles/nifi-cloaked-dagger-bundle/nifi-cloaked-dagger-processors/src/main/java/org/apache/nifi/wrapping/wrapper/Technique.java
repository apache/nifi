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
package org.apache.nifi.wrapping.wrapper;

import org.apache.commons.lang3.ArrayUtils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.BitSet;

/**
 * Byte[] representation of a Technique section to be used within the header.
 */
class Technique {
    // 4 byte field representing the technique. Fixed mask is 0x0001.
    public static final byte[] TECHNIQUE_1 = { 0, 0, 0, 1 };

    // 4 byte field representing the technique. Fixed mask is 0x0001.
    public static final byte[] TECHNIQUE_2 = { 0, 0, 0, 2 };

    // 2 byte field representing technique specific configuration.
    private final byte[] config;

    // 2 byte integer value representing length of the technique related data (in bytes).
    private final byte[] dataLength;

    // Technique related data.
    private final byte[] data;

    // Technique to use.
    private final byte[] technique;

    /**
     * Constructs a technique.
     *
     * @param technique
     *            - use static values provided with this class.
     * @param data
     *            - byte array.
     */
    protected Technique(final byte[] technique, final byte[] data) {
        this.technique = technique.clone();
        this.data = data.clone();

        // The config field is set up based on the length in bits of the mask. Get byte length then * 8.
        this.config = getConfigBytes(this.data.length * 8);
        this.dataLength = shortToBytes((short) this.data.length);
    }

    /**
     * Converts the short technique data length to an array of 2 bytes.
     *
     * @param value
     *            - short value to convert.
     * @return a byte array converted from the given short value.
     */
    private byte[] shortToBytes(final short value) {
        // Give the byte buffer a capacity of 2 bytes.
        ByteBuffer buffer = ByteBuffer.allocate(2);
        buffer.putShort(value);
        // Need network byte order.
        buffer.order(ByteOrder.BIG_ENDIAN);
        return buffer.array();
    }

    /**
     * Returns the complete header.
     *
     * @return the complete header as a byte array.
     */
    protected byte[] getTechniqueBytes() {
        final byte[] result1 = ArrayUtils.addAll(technique, config);

        final byte[] result2 = ArrayUtils.addAll(dataLength, data);

        return ArrayUtils.addAll(result1, result2);
    }

    /**
     * Returns the byte array to use for the 2 byte config section.
     *
     * @param configIn
     *            - length of mask in bits.
     * @return the config section in a 2-byte array.
     */
    private byte[] getConfigBytes(final int configIn) {
        // Sort out the config field. We only need to sort out the first three bits.
        final BitSet configTemp = new BitSet(8);

        // Value for 8 bit is 000, so ignore it.
        if (configIn == 16) {
            configTemp.set(0);
        } else if (configIn == 32) {
            configTemp.set(1);
        } else if (configIn == 64) {
            configTemp.set(0);
            configTemp.set(1);
        } else if (configIn == 128) {
            configTemp.set(2);
        } else if (configIn == 192) {
            configTemp.set(0);
            configTemp.set(2);
        } else if (configIn == 256) {
            configTemp.set(1);
            configTemp.set(2);
        }

        // Now use the BitSet to populate our config byte[]. NOTE - Java 7 includes a method on BitSet
        // to do this for us. Shame we only have version 6....
        // Now using version 7...
        // final byte[] bytes = new byte[(configTemp.length() + 7) / 8];
        // for (int i = 0; i < configTemp.length(); i++) {
        // if (configTemp.get(i)) {
        // bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
        // }
        // }
        final byte[] bytes = configTemp.toByteArray();

        // If we have an 8 bit mask we won't have populated anything.
        if (bytes.length == 0) {
            return new byte[] { 0, 0 };
        }

        // At this point we have a byte array with 1 byte. We need a second empty one to meet spec.
        return new byte[] { 0, bytes[0] };
    }
}
