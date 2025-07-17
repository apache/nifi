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
import static org.apache.nifi.confluent.schemaregistry.VarintUtils.writeZigZagVarint;
import static org.junit.jupiter.api.Assertions.*;

public class VarintUtilsTest {

    @Test
    public void testReadVarintFromStream_SingleByte() throws IOException {
        byte[] data = {0x08}; // 8 in varint format (0x08 = 00001000)
        InputStream inputStream = new ByteArrayInputStream(data);
        
        int result = readVarintFromStream(inputStream, -1);
        assertEquals(8, result);
    }

    @Test
    public void testReadVarintFromStream_MultiByte() throws IOException {
        byte[] data = {(byte) 0x96, 0x01}; // 150 in varint format (10010110 00000001)
        InputStream inputStream = new ByteArrayInputStream(data);
        
        int result = readVarintFromStream(inputStream, -1);
        assertEquals(150, result);
    }

    @Test
    public void testReadVarintFromStream_MaxValue() throws IOException {
        // Maximum 32-bit value in varint format (11111111 11111111 11111111 11111111 00001111)
        byte[] data = {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F};
        InputStream inputStream = new ByteArrayInputStream(data);
        
        int result = readVarintFromStream(inputStream, -1);
        assertEquals(0xFFFFFFFF, result);
    }

    @Test
    public void testReadVarintFromStream_WithFirstByte() throws IOException {
        byte[] data = {0x08}; // 8 in varint format (0x08 = 00001000)
        InputStream inputStream = new ByteArrayInputStream(data);
        
        int result = readVarintFromStream(inputStream, 0x08);
        assertEquals(8, result);
    }

    @Test
    public void testReadVarintFromStream_Zero() throws IOException {
        byte[] data = {0x00}; // 0 in varint format (0x00 = 00000000)
        InputStream inputStream = new ByteArrayInputStream(data);
        
        int result = readVarintFromStream(inputStream, -1);
        assertEquals(0, result);
    }

    @Test
    public void testReadVarintFromStream_LargeValue() throws IOException {
        // 16384 in varint format (10000000 10000000 00000001)
        byte[] data = {(byte) 0x80, (byte) 0x80, 0x01};
        InputStream inputStream = new ByteArrayInputStream(data);
        
        int result = readVarintFromStream(inputStream, -1);
        assertEquals(16384, result);
    }

    @Test
    public void testReadVarintFromStream_EmptyStream() {
        InputStream inputStream = new ByteArrayInputStream(new byte[0]);
        
        IOException exception = assertThrows(IOException.class, () -> readVarintFromStream(inputStream, -1));
        assertTrue(exception.getMessage().contains("Unexpected end of stream while reading varint"));
    }

    @Test
    public void testReadVarintFromStream_TruncatedStream() {
        // Start of a multi-byte varint but missing continuation bytes (0x80 = 10000000)
        byte[] data = {(byte) 0x80}; // Indicates more bytes to follow but none provided
        InputStream inputStream = new ByteArrayInputStream(data);
        
        IOException exception = assertThrows(IOException.class, () -> readVarintFromStream(inputStream, -1));
        assertTrue(exception.getMessage().contains("Unexpected end of stream while reading varint"));
    }

    @Test
    public void testReadVarintFromStream_TooLong() {
        // Varint with more than 32 bits (5 bytes with continuation bit set)
        // (10000000 10000000 10000000 10000000 10000000 00000001)
        byte[] data = {(byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80, 0x01};
        InputStream inputStream = new ByteArrayInputStream(data);
        
        IOException exception = assertThrows(IOException.class, () -> readVarintFromStream(inputStream, -1));
        assertTrue(exception.getMessage().contains("Varint too long (more than 32 bits)"));
    }

    @Test
    public void testDecodeZigZag_PositiveValues() {
        // Test positive values
        assertEquals(0, decodeZigZag(0));   // 0 -> 0
        assertEquals(1, decodeZigZag(2));   // 1 -> 2
        assertEquals(2, decodeZigZag(4));   // 2 -> 4
        assertEquals(3, decodeZigZag(6));   // 3 -> 6
        assertEquals(150, decodeZigZag(300)); // 150 -> 300
    }

    @Test
    public void testDecodeZigZag_NegativeValues() {
        // Test negative values
        assertEquals(-1, decodeZigZag(1));   // -1 -> 1
        assertEquals(-2, decodeZigZag(3));   // -2 -> 3
        assertEquals(-3, decodeZigZag(5));   // -3 -> 5
        assertEquals(-150, decodeZigZag(299)); // -150 -> 299
    }

    @Test
    public void testDecodeZigZag_ExtremeValues() {
        // Test extreme values
        assertEquals(Integer.MAX_VALUE, decodeZigZag(0xFFFFFFFE));
        assertEquals(Integer.MIN_VALUE, decodeZigZag(0xFFFFFFFF));
    }

    @Test
    public void testDecodeZigZag_LargeValues() {
        // Test some larger values to verify the algorithm
        assertEquals(1000, decodeZigZag(2000));  // 1000 -> 2000
        assertEquals(-1000, decodeZigZag(1999)); // -1000 -> 1999
    }

    @Test
    public void testWriteZigZagVarint_Zero() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeZigZagVarint(output, 0);
        byte[] result = output.toByteArray();
        
        assertEquals(1, result.length);
        assertEquals(0x00, result[0]);
    }

    @Test
    public void testWriteZigZagVarint_PositiveSmallNumbers() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeZigZagVarint(output, 1);
        byte[] result = output.toByteArray();
        
        assertEquals(1, result.length);
        assertEquals(0x02, result[0]); // 1 -> 2 (zigzag encoded)
        
        output = new ByteArrayOutputStream();
        writeZigZagVarint(output, 2);
        result = output.toByteArray();
        
        assertEquals(1, result.length);
        assertEquals(0x04, result[0]); // 2 -> 4 (zigzag encoded)
    }

    @Test
    public void testWriteZigZagVarint_NegativeSmallNumbers() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeZigZagVarint(output, -1);
        byte[] result = output.toByteArray();
        
        assertEquals(1, result.length);
        assertEquals(0x01, result[0]); // -1 -> 1 (zigzag encoded)
        
        output = new ByteArrayOutputStream();
        writeZigZagVarint(output, -2);
        result = output.toByteArray();
        
        assertEquals(1, result.length);
        assertEquals(0x03, result[0]); // -2 -> 3 (zigzag encoded)
    }

    @Test
    public void testWriteZigZagVarint_LargePositiveNumber() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeZigZagVarint(output, 150);
        byte[] result = output.toByteArray();
        
        assertEquals(2, result.length);
        assertEquals((byte) 0xAC, result[0]); // Lower 7 bits of 300 with continuation bit
        assertEquals(0x02, result[1]); // Upper bits of 300 (150 -> 300 zigzag)
    }

    @Test
    public void testWriteZigZagVarint_LargeNegativeNumber() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeZigZagVarint(output, -150);
        byte[] result = output.toByteArray();
        
        assertEquals(2, result.length);
        assertEquals((byte) 0xAB, result[0]); // Lower 7 bits of 299 with continuation bit
        assertEquals(0x02, result[1]); // Upper bits of 299 (-150 -> 299 zigzag)
    }

    @Test
    public void testWriteZigZagVarint_ExtremeValues() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        writeZigZagVarint(output, Integer.MAX_VALUE);
        byte[] result = output.toByteArray();
        
        assertEquals(5, result.length);
        
        output = new ByteArrayOutputStream();
        writeZigZagVarint(output, Integer.MIN_VALUE);
        result = output.toByteArray();
        
        assertEquals(5, result.length);
    }

    @Test
    public void testWriteZigZagVarint_MultipleValues() throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        
        // Write multiple values
        writeZigZagVarint(output, 0);
        writeZigZagVarint(output, 1);
        writeZigZagVarint(output, -1);
        writeZigZagVarint(output, 150);
        
        byte[] result = output.toByteArray();
        
        // Verify we have the expected number of bytes
        assertTrue(result.length > 4); // At least one byte per value
        
        // Read back and verify
        ByteArrayInputStream input = new ByteArrayInputStream(result);
        
        assertEquals(0, decodeZigZag(readVarintFromStream(input, -1)));
        assertEquals(1, decodeZigZag(readVarintFromStream(input, -1)));
        assertEquals(-1, decodeZigZag(readVarintFromStream(input, -1)));
        assertEquals(150, decodeZigZag(readVarintFromStream(input, -1)));
    }
}