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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.apache.nifi.confluent.schemaregistry.VarintUtils.decodeZigZag;
import static org.apache.nifi.confluent.schemaregistry.VarintUtils.readVarintFromStream;
import static org.apache.nifi.confluent.schemaregistry.VarintUtils.readVarintFromStreamAfterFirstByteConsumed;
import static org.apache.nifi.confluent.schemaregistry.VarintUtils.writeZigZagVarint;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class VarintUtilsTest {

    @Test
    void testReadVarintFromStream_SingleByte() throws IOException {
        byte[] data = {0x08}; // 8 in varint format (0x08 = 00001000)
        InputStream inputStream = new ByteArrayInputStream(data);

        int result = readVarintFromStream(inputStream);
        assertEquals(8, result);
    }

    @Test
    void testReadVarintFromStream_MultiByte() throws IOException {
        byte[] data = {(byte) 0x96, 0x01}; // 150 in varint format (10010110 00000001)
        InputStream inputStream = new ByteArrayInputStream(data);

        int result = readVarintFromStream(inputStream);
        assertEquals(150, result);
    }

    @Test
    void testReadVarintFromStream_MaxValue() throws IOException {
        // Maximum 32-bit value in varint format (11111111 11111111 11111111 11111111 00001111)
        byte[] data = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F};
        InputStream inputStream = new ByteArrayInputStream(data);

        int result = readVarintFromStream(inputStream);
        assertEquals(0xFFFFFFFF, result);
    }

    @Test
    void testReadVarintFromStream_WithFirstByte() throws IOException {
        byte[] data = {0x08}; // 8 in varint format (0x08 = 00001000)
        InputStream inputStream = new ByteArrayInputStream(data);
        inputStream.read(); // Simulate consuming the first byte

        int result = readVarintFromStreamAfterFirstByteConsumed(inputStream, data[0]);
        assertEquals(8, result);
    }

    @Test
    void testReadVarintFromStream_WithFirstByte_WithContinuationBit() throws IOException {
        byte[] data = {(byte) 0x96, 0x01}; // 150 in varint format (10010110 00000001)
        InputStream inputStream = new ByteArrayInputStream(data);
        inputStream.read(); // Simulate consuming the first byte
        int result = readVarintFromStreamAfterFirstByteConsumed(inputStream, data[0]);
        assertEquals(150, result);
    }

    @Test
    void testReadVarintFromStream_Zero() throws IOException {
        byte[] data = {0x00}; // 0 in varint format (0x00 = 00000000)
        InputStream inputStream = new ByteArrayInputStream(data);

        int result = readVarintFromStream(inputStream);
        assertEquals(0, result);
    }

    @Test
    void testReadVarintFromStream_LargeValue() throws IOException {
        // 16384 in varint format (10000000 10000000 00000001)
        byte[] data = {(byte) 0x80, (byte) 0x80, 0x01};
        InputStream inputStream = new ByteArrayInputStream(data);

        int result = readVarintFromStream(inputStream);
        assertEquals(16384, result);
    }

    @Test
    void testReadVarintFromStream_EmptyStream() {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);

        IOException exception = assertThrows(IOException.class, () -> readVarintFromStream(inputStream));
        assertTrue(exception.getMessage().contains("Unexpected end of stream while reading varint"));
    }

    @Test
    void testReadVarintFromStream_TruncatedStream() {
        // Start of a multi-byte varint but missing continuation bytes (0x80 = 10000000)
        byte[] data = {(byte) 0x80}; // Indicates more bytes to follow but none provided
        InputStream inputStream = new ByteArrayInputStream(data);

        IOException exception = assertThrows(IOException.class, () -> readVarintFromStream(inputStream));
        assertTrue(exception.getMessage().contains("Unexpected end of stream while reading varint"));
    }

    @Test
    void testReadVarintFromStream_TooLong() {
        // Varint with more than 32 bits (5 bytes with continuation bit set)
        // (10000000 10000000 10000000 10000000 10000000 00000001)
        byte[] data = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01};
        InputStream inputStream = new ByteArrayInputStream(data);

        IOException exception = assertThrows(IOException.class, () -> readVarintFromStream(inputStream));
        assertTrue(exception.getMessage().contains("Varint too long (more than 32 bits)"));
    }

    @Test
    void testDecodeZigZag_PositiveValues() {
        // Test positive values
        assertEquals(0, decodeZigZag(0));   // 0 -> 0
        assertEquals(1, decodeZigZag(2));   // 1 -> 2
        assertEquals(2, decodeZigZag(4));   // 2 -> 4
        assertEquals(3, decodeZigZag(6));   // 3 -> 6
        assertEquals(150, decodeZigZag(300)); // 150 -> 300
    }

    @Test
    void testDecodeZigZag_NegativeValues() {
        // Test negative values
        assertEquals(-1, decodeZigZag(1));   // -1 -> 1
        assertEquals(-2, decodeZigZag(3));   // -2 -> 3
        assertEquals(-3, decodeZigZag(5));   // -3 -> 5
        assertEquals(-150, decodeZigZag(299)); // -150 -> 299
    }

    @Test
    void testDecodeZigZag_ExtremeValues() {
        // Test extreme values
        assertEquals(Integer.MAX_VALUE, decodeZigZag(0xFFFFFFFE));
        assertEquals(Integer.MIN_VALUE, decodeZigZag(0xFFFFFFFF));
    }

    @Test
    void testDecodeZigZag_LargeValues() {
        // Test some larger values to verify the algorithm
        assertEquals(1000, decodeZigZag(2000));  // 1000 -> 2000
        assertEquals(-1000, decodeZigZag(1999)); // -1000 -> 1999
    }

    @Test
    void testWriteZigZagVarint_Zero() {
        byte[] result = writeZigZagVarint(0);

        assertEquals(1, result.length);
        assertEquals(0x00, result[0]);
    }

    @Test
    void testWriteZigZagVarint_PositiveSmallNumbers() {
        byte[] result = writeZigZagVarint(1);
        assertEquals(1, result.length);
        assertEquals(0x02, result[0]); // 1 -> 2 (zigzag encoded)
    }

    @Test
    void testWriteZigZagVarint_NegativeSmallNumbers() {
        byte[] result = writeZigZagVarint(-1);
        assertEquals(1, result.length);
        assertEquals(0x01, result[0]); // -1 -> 1 (zigzag encoded)
    }

    @Test
    void testWriteZigZagVarint_LargePositiveNumber() {
        byte[] result = writeZigZagVarint(150);
        assertEquals(2, result.length);
        assertEquals((byte) 0xAC, result[0]); // Lower 7 bits of 300 with continuation bit
        assertEquals(0x02, result[1]); // Upper bits of 300 (150 -> 300 zigzag)
    }

    @Test
    void testWriteZigZagVarint_LargeNegativeNumber() {
        byte[] result = writeZigZagVarint(-150);

        assertEquals(2, result.length);
        assertEquals((byte) 0xAB, result[0]); // Lower 7 bits of 299 with continuation bit
        assertEquals(0x02, result[1]); // Upper bits of 299 (-150 -> 299 zigzag)
    }

    @Test
    void testWriteZigZagVarint_ExtremeValues() {
        byte[] result = writeZigZagVarint(Integer.MAX_VALUE);

        assertEquals(5, result.length);

        result = writeZigZagVarint(Integer.MIN_VALUE);

        assertEquals(5, result.length);
    }

    @Test
    void testWriteZigZagVarint_MultipleValues() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Write multiple values
        output.write(writeZigZagVarint(0));
        output.write(writeZigZagVarint(1));
        output.write(writeZigZagVarint(-1));
        output.write(writeZigZagVarint(150));

        byte[] result = output.toByteArray();

        // Verify we have the expected number of bytes
        assertTrue(result.length > 4); // At least one byte per value

        // Read back and verify
        ByteArrayInputStream input = new ByteArrayInputStream(result);

        assertEquals(0, decodeZigZag(readVarintFromStream(input)));
        assertEquals(1, decodeZigZag(readVarintFromStream(input)));
        assertEquals(-1, decodeZigZag(readVarintFromStream(input)));
        assertEquals(150, decodeZigZag(readVarintFromStream(input)));
    }
}
