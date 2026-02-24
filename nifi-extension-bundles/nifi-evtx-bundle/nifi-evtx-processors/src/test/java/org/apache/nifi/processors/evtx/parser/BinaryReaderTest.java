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

package org.apache.nifi.processors.evtx.parser;

import com.google.common.primitives.UnsignedInteger;
import com.google.common.primitives.UnsignedLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class BinaryReaderTest {
    private TestBinaryReaderBuilder testBinaryReaderBuilder;

    @BeforeEach
    public void setup() {
        testBinaryReaderBuilder = new TestBinaryReaderBuilder();
    }

    @Test
    public void testRead() throws IOException {
        final byte b = 0x23;
        final BinaryReader binaryReader = testBinaryReaderBuilder.put(b).build();
        assertEquals(b, binaryReader.read());
        assertEquals(1, binaryReader.getPosition());
    }

    @Test
    public void testPeek() throws IOException {
        final byte b = 0x23;
        final BinaryReader binaryReader = testBinaryReaderBuilder.put(new byte[]{b}).build();
        assertEquals(b, binaryReader.peek());
        assertEquals(0, binaryReader.getPosition());
    }

    @Test
    public void testReadBytesJustLength() throws IOException {
        final byte[] bytes = "Hello world".getBytes(StandardCharsets.US_ASCII);
        final BinaryReader binaryReader = testBinaryReaderBuilder.put(bytes).build();
        assertArrayEquals(Arrays.copyOfRange(bytes, 0, 5), binaryReader.readBytes(5));
        assertEquals(5, binaryReader.getPosition());
    }

    @Test
    public void testPeekBytes() throws IOException {
        final byte[] bytes = "Hello world".getBytes(StandardCharsets.US_ASCII);
        final BinaryReader binaryReader = testBinaryReaderBuilder.put(bytes).build();
        assertArrayEquals(Arrays.copyOfRange(bytes, 0, 5), binaryReader.peekBytes(5));
        assertEquals(0, binaryReader.getPosition());
    }

    @Test
    public void testReadBytesBufOffsetLength() throws IOException {
        final byte[] bytes = "Hello world".getBytes(StandardCharsets.US_ASCII);
        final byte[] buf = new byte[5];

        final BinaryReader binaryReader = testBinaryReaderBuilder.put(bytes).build();
        binaryReader.readBytes(buf, 0, 5);
        assertArrayEquals(Arrays.copyOfRange(bytes, 0, 5), buf);
        assertEquals(5, binaryReader.getPosition());
    }

    @Test
    public void testReadGuid() throws IOException {
        final String guid = "33323130-3534-3736-3839-616263646566";
        final BinaryReader binaryReader = testBinaryReaderBuilder.putGuid(guid).build();
        assertEquals(guid, binaryReader.readGuid());
        assertEquals(16, binaryReader.getPosition());
    }

    @Test
    public void testReadStringNotNullTerminated() throws IOException {
        final String value = "Hello world";

        final BinaryReader binaryReader = testBinaryReaderBuilder.put(value.getBytes(StandardCharsets.US_ASCII)).build();
        assertThrows(IOException.class, () -> binaryReader.readString(value.length()));
    }

    @Test
    public void testReadString() throws IOException {
        final String value = "Hello world";

        final BinaryReader binaryReader = testBinaryReaderBuilder.putString(value).build();
        assertEquals(value, binaryReader.readString(value.length() + 1));
        assertEquals(value.length() + 1, binaryReader.getPosition());
    }

    @Test
    public void testReadWString() throws IOException {
        final String value = "Hello world";
        final BinaryReader binaryReader = testBinaryReaderBuilder.putWString(value).build();

        assertEquals(value, binaryReader.readWString(value.length()));
        assertEquals(value.length() * 2, binaryReader.getPosition());
    }

    @Test
    public void testReadQWord() throws IOException {
        final UnsignedLong longValue = UnsignedLong.fromLongBits(Long.MAX_VALUE + 500);
        final BinaryReader binaryReader = testBinaryReaderBuilder.putQWord(longValue).build();

        assertEquals(longValue, binaryReader.readQWord());
        assertEquals(8, binaryReader.getPosition());
    }

    @Test
    public void testReadDWord() throws IOException {
        final UnsignedInteger intValue = UnsignedInteger.fromIntBits(Integer.MAX_VALUE + 500);
        final BinaryReader binaryReader = testBinaryReaderBuilder.putDWord(intValue).build();

        assertEquals(intValue, binaryReader.readDWord());
        assertEquals(4, binaryReader.getPosition());
    }

    @Test
    public void testReadDWordBE() throws IOException {
        final UnsignedInteger intValue = UnsignedInteger.fromIntBits(Integer.MAX_VALUE + 500);
        final BinaryReader binaryReader = testBinaryReaderBuilder.putDWordBE(intValue).build();

        assertEquals(intValue, binaryReader.readDWordBE());
        assertEquals(4, binaryReader.getPosition());
    }

    @Test
    public void testReadWord() throws IOException {
        final int intValue = Short.MAX_VALUE + 500;
        final BinaryReader binaryReader = testBinaryReaderBuilder.putWord(intValue).build();

        assertEquals(intValue, binaryReader.readWord());
        assertEquals(2, binaryReader.getPosition());
    }

    @Test
    public void testReadWordBE() throws IOException {
        final int intValue = Short.MAX_VALUE + 500;
        final BinaryReader binaryReader = testBinaryReaderBuilder.putWordBE(intValue).build();

        assertEquals(intValue, binaryReader.readWordBE());
        assertEquals(2, binaryReader.getPosition());
    }

    @Test
    public void testReadFileTIme() throws IOException {
        final Date date = new Date();
        final BinaryReader binaryReader = testBinaryReaderBuilder.putFileTime(date).build();

        assertEquals(date.getTime(), binaryReader.readFileTime().getTime());
        assertEquals(8, binaryReader.getPosition());
    }

    @Test
    public void testReadAndBase64EncodeBinary() throws IOException {
        final String orig = "Hello World";
        final String stringValue = Base64.getEncoder().encodeToString(orig.getBytes(StandardCharsets.US_ASCII));
        final BinaryReader binaryReader = testBinaryReaderBuilder.putBase64EncodedBinary(stringValue).build();

        assertEquals(stringValue, binaryReader.readAndBase64EncodeBinary(orig.length()));
        assertEquals(orig.length(), binaryReader.getPosition());
    }

    @Test
    public void testSkip() throws IOException {
        final BinaryReader binaryReader = new BinaryReader(null);
        binaryReader.skip(10);
        assertEquals(10, binaryReader.getPosition());
    }

    @Test
    public void testReaderPositionConstructor() throws IOException {
        final String value = "Hello world";
        final BinaryReader binaryReader = new BinaryReader(new BinaryReader(value.getBytes(StandardCharsets.UTF_16LE)), 2);

        assertEquals(value.substring(1), binaryReader.readWString(value.length() - 1));
        assertEquals(value.length() * 2, binaryReader.getPosition());
    }

    @Test
    public void testInputStreamSizeConstructor() throws IOException {
        final String value = "Hello world";
        final BinaryReader binaryReader = new BinaryReader(new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_16LE)), 10);

        assertEquals(value.substring(0, 5), binaryReader.readWString(5));
        assertEquals(10, binaryReader.getPosition());
    }
}
