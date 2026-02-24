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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class TestBinaryReaderBuilder {
    private final List<byte[]> data = new ArrayList<>();

    public TestBinaryReaderBuilder put(final byte val) {
        data.add(new byte[]{val});
        return this;
    }

    public TestBinaryReaderBuilder put(final byte[] bytes) {
        data.add(bytes);
        return this;
    }

    public TestBinaryReaderBuilder putGuid(final String guid) {
        final byte[] bytes = new byte[16];
        final String[] split = guid.split("-");
        int count = 0;
        int offset = 0;
        final int[][] indexArrays = BinaryReader.INDEX_ARRAYS;
        for (int i = 0; i < indexArrays.length; i++) {
            final String segment = split[i];
            for (int o = 0; o < indexArrays[i].length; o++) {
                final int beginIndex = (indexArrays[i][o] * 2) - offset;
                bytes[count++] = (byte) Integer.parseInt(segment.substring(beginIndex, beginIndex + 2), 16);
            }
            offset += segment.length();
        }
        put(bytes);
        return this;
    }

    public TestBinaryReaderBuilder putString(final String val) {
        data.add(val.getBytes(StandardCharsets.US_ASCII));
        data.add(new byte[]{0});
        return this;
    }

    public TestBinaryReaderBuilder putWString(final String val) {
        data.add(val.getBytes(StandardCharsets.UTF_16LE));
        return this;
    }

    public TestBinaryReaderBuilder putQWord(final long longBits) {
        data.add(ByteBuffer.wrap(new byte[8]).order(ByteOrder.LITTLE_ENDIAN).putLong(longBits).array());
        return this;
    }

    public TestBinaryReaderBuilder putQWord(final UnsignedLong val) {
        return putQWord(val.longValue());
    }

    public TestBinaryReaderBuilder putDWord(final int intBits) {
        data.add(ByteBuffer.wrap(new byte[4]).order(ByteOrder.LITTLE_ENDIAN).putInt(intBits).array());
        return this;
    }

    public TestBinaryReaderBuilder putDWordAt(final int intBits, final int position) throws IOException {
        ByteBuffer.wrap(toByteArray(), position, 4).order(ByteOrder.LITTLE_ENDIAN).putInt(intBits);
        return this;
    }

    public TestBinaryReaderBuilder putDWord(final UnsignedInteger val) {
        return putDWord(val.intValue());
    }

    public TestBinaryReaderBuilder putDWordBE(final int intBits) {
        data.add(ByteBuffer.wrap(new byte[4]).order(ByteOrder.BIG_ENDIAN).putInt(intBits).array());
        return this;
    }

    public TestBinaryReaderBuilder putDWordBE(final UnsignedInteger val) {
        return putDWordBE(val.intValue());
    }

    public TestBinaryReaderBuilder putWord(final int val) {
        data.add(ByteBuffer.wrap(new byte[2]).order(ByteOrder.LITTLE_ENDIAN).putShort((short) val).array());
        return this;
    }

    public TestBinaryReaderBuilder putWordBE(final int val) {
        data.add(ByteBuffer.wrap(new byte[2]).order(ByteOrder.BIG_ENDIAN).putShort((short) val).array());
        return this;
    }

    public TestBinaryReaderBuilder putFileTime(final Date date) {
        final UnsignedLong javaMillis = UnsignedLong.valueOf(date.getTime());
        final UnsignedLong windowsMillis = javaMillis.plus(UnsignedLong.valueOf(BinaryReader.EPOCH_OFFSET));
        final UnsignedLong windowsStamp = windowsMillis.times(UnsignedLong.valueOf(10000));
        return putQWord(windowsStamp);
    }

    public TestBinaryReaderBuilder putSystemtime(final Calendar calendar) {
        putWord(calendar.get(Calendar.YEAR));
        putWord(calendar.get(Calendar.MONTH));
        putWord(calendar.get(Calendar.DAY_OF_WEEK));
        putWord(calendar.get(Calendar.DAY_OF_MONTH));
        putWord(calendar.get(Calendar.HOUR_OF_DAY));
        putWord(calendar.get(Calendar.MINUTE));
        putWord(calendar.get(Calendar.SECOND));
        putWord(calendar.get(Calendar.MILLISECOND));
        return this;
    }

    public TestBinaryReaderBuilder putBase64EncodedBinary(final String base64EncodedBinary) {
        return put(Base64.getDecoder().decode(base64EncodedBinary));
    }

    public BinaryReader build() throws IOException {
        return new BinaryReader(toByteArray());
    }

    public byte[] toByteArray() throws IOException {
        if (data.isEmpty()) {
            return new byte[0];
        } else if (data.size() == 1) {
            return data.get(0);
        } else {
            final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            for (final byte[] bytes : data) {
                byteArrayOutputStream.write(bytes);
            }
            final byte[] bytes = byteArrayOutputStream.toByteArray();
            data.clear();
            data.add(bytes);
            return bytes;
        }
    }

    public byte[] toByteArray(final int size) throws IOException {
        final byte[] bytes = toByteArray();
        final byte[] result = new byte[size];
        System.arraycopy(bytes, 0, result, 0, Math.min(bytes.length, result.length));
        return result;
    }
}
