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
package org.apache.nifi.confluent.schemaregistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class for reading, writing and decoding varint values from input streams.
 * This class provides methods for reading variable-length integers (varints),
 * writing zigzag-encoded varints, and decoding zigzag-encoded integers as per the Protocol Buffers specification.
 *
 * <p>Variable-length integers (varints) are a method of serializing integers using one or more bytes.
 * Smaller numbers take fewer bytes. Each byte in a varint, except the last byte, has the most
 * significant bit set – this indicates that there are further bytes to come. The lower 7 bits
 * of each byte are used to store the two's complement representation of the number in groups of 7 bits,
 * least significant group first.</p>
 *
 * <p>For more information about varint encoding, see:
 * <a href="https://en.wikipedia.org/wiki/Variable-length_quantity">Variable-length quantity - Wikipedia</a></p>
 *
 * <p>This implementation follows the Protocol Buffers varint encoding format as described in:
 * <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">Protocol Buffers Encoding</a></p>
 *
 */
final class VarintUtils {

    private VarintUtils() {
    }

    /**
     * Reads a single varint from the input stream.
     *
     * @param inputStream the input stream to read from
     * @return the decoded varint value
     * @throws IOException if unable to read from stream or invalid varint format
     */
    static int readVarintFromStream(final InputStream inputStream) throws IOException {
        final int firstByte = inputStream.read();
        if (firstByte == -1) {
            throw new IOException("Unexpected end of stream while reading varint");
        }
        return readVarintFromStreamAfterFirstByteConsumed(inputStream, firstByte);
    }

    /**
     * Reads a single varint from the input stream with a pre-read first byte.
     *
     * @param inputStream the input stream to read from
     * @param firstByte   the first byte already read from the stream
     * @return the decoded varint value
     * @throws IOException if unable to read from stream or invalid varint format
     */
    static int readVarintFromStreamAfterFirstByteConsumed(final InputStream inputStream, final int firstByte) throws IOException {
        // accumulated result
        int value = 0;

        // stores bit shift position (0, 7, 14, 21, 28 for consecutive bytes)
        int shift = 0;

        // Store the current byte being processed
        int currentByte = firstByte;

        // continues until we find a byte without the continuation bit
        while (true) {
            // Prevent overflow by limiting varint to 32 bits (5 bytes maximum: 5 * 7 = 35 bits, but we bail at 32)
            if (shift >= 32) {
                throw new IOException("Varint too long (more than 32 bits)");
            }

            // Extract the lower 7 bits of the current byte using & 0x7F (0111 1111)
            // Shift these 7 bits to their correct position in the final value
            // Combine with the accumulated value |=
            value |= (currentByte & 0x7F) << shift;

            // Check if this is the last byte by testing the MSB
            // If bit 7 is 0 (no continuation bit), we're done
            if ((currentByte & 0x80) == 0) {
                break;
            }

            // Increment the bit shift position by 7 for the next byte
            shift += 7;

            currentByte = inputStream.read();

            if (currentByte == -1) {
                throw new IOException("Unexpected end of stream while reading varint");
            }
        }

        return value;
    }

    /**
     * Decodes a zigzag encoded integer.
     * ZigZag encoding maps signed integers to unsigned integers so that
     * numbers with a small absolute value have a small varint encoding.
     *
     * @param encodedValue the zigzag encoded value
     * @return the decoded integer value
     */
    static int decodeZigZag(final int encodedValue) {
        return (encodedValue >>> 1) ^ -(encodedValue & 1);
    }

    /**
     * Encodes a zigzag encoded varint and returns it as a byte array.
     * ZigZag encoding maps signed integers to unsigned integers so that
     * numbers with a small absolute value have a small varint encoding.
     *
     * @param value the integer value to encode
     * @return byte array containing the zigzag encoded varint
     */
    static byte[] writeZigZagVarint(final int value) {
        final ByteArrayOutputStream output = new ByteArrayOutputStream(4);

        // Zigzag encode
        int encoded = (value << 1) ^ (value >> 31);

        // Write as varint
        while ((encoded & 0xFFFFFF80) != 0) {
            output.write((encoded & 0x7F) | 0x80);
            encoded >>>= 7;
        }
        output.write(encoded & 0x7F);

        return output.toByteArray();
    }
}