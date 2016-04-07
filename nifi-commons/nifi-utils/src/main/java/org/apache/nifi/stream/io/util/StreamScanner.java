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
package org.apache.nifi.stream.io.util;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public class StreamScanner {

    private final static int EOF = -1;

    private final InputStream is;

    private final byte[] delimiterBytes;

    private final int maxDataSize;

    private ByteBuffer buffer;

    private byte[] data;

    /**
     * Constructs a new instance
     *
     * @param is
     *            instance of {@link InputStream} representing the data
     * @param delimiterBytes
     *            byte array representing delimiter bytes used to split the
     *            input stream. Can be null
     * @param maxDataSize
     *            maximum size of data derived from the input stream. This means
     *            that neither {@link InputStream} nor its individual chunks (if
     *            delimiter is used) can ever be greater then this size.
     */
    public StreamScanner(InputStream is, byte[] delimiterBytes, int maxDataSize) {
        this(is, delimiterBytes, maxDataSize, 8192);
    }

    /**
     * Constructs a new instance
     *
     * @param is
     *            instance of {@link InputStream} representing the data
     * @param delimiterBytes
     *            byte array representing delimiter bytes used to split the
     *            input stream. Can be null
     * @param maxDataSize
     *            maximum size of data derived from the input stream. This means
     *            that neither {@link InputStream} nor its individual chunks (if
     *            delimiter is used) can ever be greater then this size.
     * @param initialBufferSize
     *            initial size of the buffer used to buffer {@link InputStream}
     *            or its parts (if delimiter is used) to create its byte[]
     *            representation. Must be positive integer. The buffer will grow
     *            automatically as needed up to the Integer.MAX_VALUE;
     *
     */
    public StreamScanner(InputStream is, byte[] delimiterBytes, int maxDataSize, int initialBufferSize) {
        this.is = new BufferedInputStream(is);
        this.delimiterBytes = delimiterBytes;
        this.buffer = ByteBuffer.allocate(initialBufferSize);
        this.maxDataSize = maxDataSize;
    }

    /**
     * Checks if there are more elements in the stream. This operation is
     * idempotent.
     *
     * @return <i>true</i> if there are more elements in the stream or
     *         <i>false</i> when it reaches the end of the stream after the last
     *         element was retrieved via {@link #next()} operation.
     */
    public boolean hasNext() {
        int j = 0;
        int readVal = 0;
        while (this.data == null && readVal != EOF) {
            this.expandBufferIfNecessary();
            try {
                readVal = this.is.read();
            } catch (IOException e) {
                throw new IllegalStateException("Failed while reading InputStream", e);
            }
            if (readVal == EOF) {
                this.extractDataToken(0);
            } else {
                byte byteVal = (byte)readVal;
                this.buffer.put(byteVal);
                if (this.buffer.position() > this.maxDataSize) {
                    throw new IllegalStateException("Maximum allowed data size of " + this.maxDataSize + " exceeded.");
                }
                if (this.delimiterBytes != null && this.delimiterBytes[j] == byteVal) {
                    if (++j == this.delimiterBytes.length) {
                        this.extractDataToken(this.delimiterBytes.length);
                        j = 0;
                    }
                } else {
                    j = 0;
                }
            }
        }
        return this.data != null;
    }

    /**
     * @return byte array representing the next segment in the stream or the
     *         whole stream if no delimiter is used
     */
    public byte[] next() {
        try {
            return this.data;
        } finally {
            this.data = null;
        }
    }

    /**
     *
     */
    private void expandBufferIfNecessary() {
        if (this.buffer.position() == Integer.MAX_VALUE ){
            throw new IllegalStateException("Internal buffer has reached the capacity and can not be expended any further");
        }
        if (this.buffer.remaining() == 0) {
            this.buffer.flip();
            int pos = this.buffer.capacity();
            int newSize = this.buffer.capacity() * 2 > Integer.MAX_VALUE ? Integer.MAX_VALUE : this.buffer.capacity() * 2;
            ByteBuffer bb = ByteBuffer.allocate(newSize);
            bb.put(this.buffer);
            this.buffer = bb;
            this.buffer.position(pos);
        }
    }

    /**
     *
     */
    private void extractDataToken(int lengthSubtract) {
        this.buffer.flip();
        if (this.buffer.limit() > 0){ // something must be in the buffer; at least delimiter (e.g., \n)
            this.data = new byte[this.buffer.limit() - lengthSubtract];
            this.buffer.get(this.data);
        }
        this.buffer.clear();
    }
}
