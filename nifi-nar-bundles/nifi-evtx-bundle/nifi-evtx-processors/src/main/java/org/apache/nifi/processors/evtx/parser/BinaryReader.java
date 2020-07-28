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
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;

/**
 * Class to simplify reading binary values from the evtx file
 */
public class BinaryReader {
    public static final long EPOCH_OFFSET = 11644473600000L;
    public static final int[][] INDEX_ARRAYS = new int[][]{{3, 2, 1, 0}, {5, 4}, {7, 6}, {8, 9}, {10, 11, 12, 13, 14, 15}};
    private final byte[] bytes;
    private int position;

    /**
     * Constructs a binary reader with the given one's byte array and an arbitrary position
     *
     * @param binaryReader the source BinaryReader
     * @param position     the new position
     */
    public BinaryReader(BinaryReader binaryReader, int position) {
        this.bytes = binaryReader.bytes;
        this.position = position;
    }

    /**
     * Reads size bytes from the inputStream and creates a BinaryReader for them
     *
     * @param inputStream the input stream
     * @param size        the number of bytes
     * @throws IOException if there is an error reading from the input stream
     */
    public BinaryReader(InputStream inputStream, int size) throws IOException {
        byte[] bytes = new byte[size];
        int read = 0;
        while (read < size) {
            read += inputStream.read(bytes, read, size - read);
        }
        this.bytes = bytes;
        this.position = 0;
    }

    /**
     * Creates a BinaryReader for the given bytes
     *
     * @param bytes the bytes
     */
    public BinaryReader(byte[] bytes) {
        this.bytes = bytes;
        this.position = 0;
    }

    /**
     * Reads a single byte
     *
     * @return the byte's int value
     */
    public int read() {
        return bytes[position++];
    }

    /**
     * Returns the byte that would be read without changing the position
     *
     * @return the byte that would be read without changing the position
     */
    public int peek() {
        return bytes[position];
    }

    /**
     * Returns the next length bytes without changing the position
     *
     * @param length the number of bytes
     * @return the bytes
     */
    public byte[] peekBytes(int length) {
        return Arrays.copyOfRange(bytes, position, position + length);
    }

    /**
     * Reads the next length bytes
     *
     * @param length the number of bytes
     * @return the bytes
     */
    public byte[] readBytes(int length) {
        byte[] result = peekBytes(length);
        position += length;
        return result;
    }

    /**
     * Reads the next length bytes into the buf buffer at a given offset
     *
     * @param buf    the buffer
     * @param offset the offset
     * @param length the number of bytes
     */
    public void readBytes(byte[] buf, int offset, int length) {
        System.arraycopy(bytes, position, buf, offset, length);
        position += length;
    }

    /**
     * Reads an Evtx formatted guid (16 bytes arranged into the grouping described by INDEX_ARRAYS)
     *
     * @return the guid
     */
    public String readGuid() {
        StringBuilder result = new StringBuilder();
        int maxIndex = 0;
        for (int[] indexArray : INDEX_ARRAYS) {
            for (int index : indexArray) {
                maxIndex = Math.max(maxIndex, index);
                result.append(String.format("%02X", bytes[position + index]).toLowerCase());
            }
            result.append("-");
        }
        result.setLength(result.length() - 1);
        position += (maxIndex + 1);
        return result.toString();
    }

    /**
     * Reads a string made up of single byte characters
     *
     * @param length the length
     * @return the string
     * @throws IOException if the String wasn't null terminated
     */
    public String readString(int length) throws IOException {
        StringBuilder result = new StringBuilder(length - 1);
        boolean foundNull = false;
        int exclusiveLastIndex = position + length;
        for (int i = position; i < exclusiveLastIndex; i++) {
            byte b = bytes[i];
            if (b == 0) {
                foundNull = true;
                break;
            }
            result.append((char) b);
        }
        if (!foundNull) {
            throw new IOException("Expected null terminated string");
        }
        position += length;
        return result.toString();
    }

    /**
     * Reads a string encoded with UTF_16LE of a given length
     *
     * @param length the number of characters
     * @return the string
     */
    public String readWString(int length) {
        int numBytes = length * 2;
        String result = StandardCharsets.UTF_16LE.decode(ByteBuffer.wrap(bytes, position, numBytes)).toString();
        position += numBytes;
        return result;
    }

    /**
     * Reads 8 bytes in litte endian order and returns the UnsignedLong value
     *
     * @return the value
     */
    public UnsignedLong readQWord() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, position, 8);
        position += 8;
        return UnsignedLong.fromLongBits(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).getLong());
    }

    /**
     * Reads 4 bytes in little endian order and returns the UnsignedInteger value
     *
     * @return the value
     */
    public UnsignedInteger readDWord() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, position, 4);
        position += 4;
        return UnsignedInteger.fromIntBits(byteBuffer.order(ByteOrder.LITTLE_ENDIAN).getInt());
    }

    /**
     * Reads 4 bytes in big endian order and returns the UnsignedInteger value
     *
     * @return the value
     */
    public UnsignedInteger readDWordBE() {
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes, position, 4);
        position += 4;
        return UnsignedInteger.fromIntBits(byteBuffer.order(ByteOrder.BIG_ENDIAN).getInt());
    }

    /**
     * Reads 2 bytes in little endian order and returns the int value
     *
     * @return the value
     */
    public int readWord() {
        byte[] bytes = new byte[4];
        readBytes(bytes, 0, 2);
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
    }

    /**
     * Reads 2 bytes in big endian order and returns the int value
     *
     * @return the value
     */
    public int readWordBE() {
        byte[] bytes = new byte[4];
        readBytes(bytes, 2, 2);
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    /**
     * Reads a timestamp that is the number of hundreds of nanoseconds since Jan 1 1601
     * (see http://integriography.wordpress.com/2010/01/16/using-phython-to-parse-and-present-windows-64-bit-timestamps/)
     *
     * @return the date corresponding to the timestamp
     */
    public Date readFileTime() {
        UnsignedLong hundredsOfNanosecondsSinceJan11601 = readQWord();
        long millisecondsSinceJan11601 = hundredsOfNanosecondsSinceJan11601.dividedBy(UnsignedLong.valueOf(10000)).longValue();
        long millisecondsSinceEpoch = millisecondsSinceJan11601 - EPOCH_OFFSET;
        return new Date(millisecondsSinceEpoch);
    }

    /**
     * Reads length bytes and Bas64 encodes them
     *
     * @param length the number of bytes
     * @return a Base64 encoded string
     */
    public String readAndBase64EncodeBinary(int length) {
        return Base64.getEncoder().encodeToString(readBytes(length));
    }

    /**
     * Skips bytes in the BinaryReader
     *
     * @param bytes the number of bytes to skip
     */
    public void skip(int bytes) {
        position += bytes;
    }

    /**
     * Returns the current position of the BinaryReader
     *
     * @return the current position
     */
    public int getPosition() {
        return position;
    }

    /**
     * Returns the backing byte array
     *
     * @return the byte array
     */
    public byte[] getBytes() {
        return bytes;
    }
}
