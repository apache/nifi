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
package org.apache.nifi.hdfs.repository;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

public class LimitedInputStreamTest {

    private final String dataStr = "onetwothreefour";
    private final byte[] dataBytes = dataStr.getBytes(StandardCharsets.UTF_8);

    /** Gets the length of the data bytes */
    private int length() {
        return dataBytes.length;
    }

    @Test
    public void fullReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, this::length)) {
            byte[] buffer = new byte[100];
            int read = inStream.read(buffer);
            assertEquals(dataBytes.length, read);
            assertEquals(dataStr, new String(buffer, 0, read, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void incrementalReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, this::length)) {
            byte[] buffer = new byte[100];

            assertEquals(3, inStream.read(buffer, 0, 3));
            assertEquals(3, inStream.read(buffer, 3, 3));
            assertEquals(5, inStream.read(buffer, 6, 5));
            assertEquals(4, inStream.read(buffer, 11, 100));

            assertEquals(dataStr, new String(buffer, 0, dataBytes.length, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void oneByteReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, this::length)) {
            byte[] buffer = new byte[100];

            int read = 0;
            int index = 0;
            while (read >= 0 && index < 100) {
                read = inStream.read();
                if (read < 0) {
                    break;
                }
                buffer[index] = (byte)read;
                index++;
            }

            assertEquals(15, index);
            assertEquals(dataStr, new String(buffer, 0, dataBytes.length, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void skipReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, this::length)) {
            byte[] buffer = new byte[100];
            assertEquals(6, inStream.skip(6));
            assertEquals(9, inStream.read(buffer));
            assertEquals(dataStr.substring(6), new String(buffer, 0, 9, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void skipFullTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, this::length)) {
            assertEquals(dataBytes.length, inStream.skip(100));
        }
    }

    @Test
    public void availableTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, this::length)) {
            assertEquals(dataBytes.length, inStream.available());
        }
    }

    @Test
    public void limitedFullReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, 6)) {
            byte[] buffer = new byte[100];
            assertEquals(6, inStream.read(buffer));
            assertEquals(dataStr.substring(0, 6), new String(buffer, 0, 6, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void limitedIncrementalReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, 6)) {
            byte[] buffer = new byte[100];

            assertEquals(3, inStream.read(buffer, 0, 3));
            assertEquals(3, inStream.read(buffer, 3, 100));

            assertEquals(dataStr.substring(0, 6), new String(buffer, 0, 6, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void limitedOneByteReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, 6)) {
            byte[] buffer = new byte[100];

            int read = 0;
            int index = 0;
            while (read >= 0 && index < 100) {
                read = inStream.read();
                if (read < 0) {
                    break;
                }
                buffer[index] = (byte)read;
                index++;
            }

            assertEquals(6, index);
            assertEquals(dataStr.substring(0, 6), new String(buffer, 0, 6, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void limitedSkipReadTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, 6)) {
            byte[] buffer = new byte[100];
            assertEquals(3, inStream.skip(3));
            assertEquals(3, inStream.read(buffer));
            assertEquals(dataStr.substring(3, 6), new String(buffer, 0, 3, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void limitedSkipFullTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, 6)) {
            assertEquals(6, inStream.skip(100));
        }
    }

    @Test
    public void limitedAvailableTest() throws IOException {
        ByteArrayInputStream data = new ByteArrayInputStream(dataBytes);
        try (LimitedInputStream inStream = new LimitedInputStream(data, 6)) {
            assertEquals(6, inStream.available());
        }
    }
}
