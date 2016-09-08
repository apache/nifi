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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.stream.io.exception.TokenTooLargeException;

/**
 * The <code>StreamDemarcator</code> class takes an input stream and demarcates
 * it so it could be read (see {@link #nextToken()}) as individual byte[]
 * demarcated by the provided delimiter. If delimiter is not provided the entire
 * stream will be read into a single token which may result in
 * {@link OutOfMemoryError} if stream is too large.
 */
public class StreamDemarcator implements Closeable {

    private final static int INIT_BUFFER_SIZE = 8192;

    private final InputStream is;

    private final byte[] delimiterBytes;

    private final int maxDataSize;

    private final int initialBufferSize;


    private byte[] buffer;

    private int index;

    private int mark;

    private int readAheadLength;

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
    public StreamDemarcator(InputStream is, byte[] delimiterBytes, int maxDataSize) {
        this(is, delimiterBytes, maxDataSize, INIT_BUFFER_SIZE);
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
    public StreamDemarcator(InputStream is, byte[] delimiterBytes, int maxDataSize, int initialBufferSize) {
        this.validateInput(is, delimiterBytes, maxDataSize, initialBufferSize);
        this.is = is;
        this.delimiterBytes = delimiterBytes;
        this.initialBufferSize = initialBufferSize;
        this.buffer = new byte[initialBufferSize];
        this.maxDataSize = maxDataSize;
    }

    /**
     * Will read the next data token from the {@link InputStream} returning null
     * when it reaches the end of the stream.
     *
     * @throws IOException if unable to read from the stream
     */
    public byte[] nextToken() throws IOException {
        byte[] data = null;
        int j = 0;

        while (data == null && this.buffer != null) {
            if (this.index >= this.readAheadLength) {
                this.fill();
            }
            if (this.index >= this.readAheadLength) {
                data = this.extractDataToken(0);
                this.buffer = null;
            } else {
                byte byteVal = this.buffer[this.index++];
                if (this.delimiterBytes != null && this.delimiterBytes[j] == byteVal) {
                    if (++j == this.delimiterBytes.length) {
                        data = this.extractDataToken(this.delimiterBytes.length);
                        this.mark = this.index;
                        j = 0;
                    }
                } else {
                    j = 0;
                }
            }
        }
        return data;
    }

    /**
     * Will fill the current buffer from current 'index' position, expanding it
     * and or shuffling it if necessary
     *
     * @throws IOException if unable to read from the stream
     */
    private void fill() throws IOException {
        if (this.index >= this.buffer.length) {
            if (this.mark == 0) { // expand
                byte[] newBuff = new byte[this.buffer.length + this.initialBufferSize];
                System.arraycopy(this.buffer, 0, newBuff, 0, this.buffer.length);
                this.buffer = newBuff;
            } else { // shuffle
                int length = this.index - this.mark;
                System.arraycopy(this.buffer, this.mark, this.buffer, 0, length);
                this.index = length;
                this.mark = 0;
            }
        }

        int bytesRead;
        do {
            bytesRead = this.is.read(this.buffer, this.index, this.buffer.length - this.index);
        } while (bytesRead == 0);

        if (bytesRead != -1) {
            this.readAheadLength = this.index + bytesRead;
            if (this.readAheadLength > this.maxDataSize) {
                throw new TokenTooLargeException("A message in the stream exceeds the maximum allowed message size of " + this.maxDataSize + " bytes.");
            }
        }
    }

    /**
     * Will extract data token from the current buffer. The length of the data
     * token is between the current 'mark' and 'index' minus 'lengthSubtract'
     * which signifies the length of the delimiter (if any). If the above
     * subtraction results in length 0, null is returned.
     */
    private byte[] extractDataToken(int lengthSubtract) {
        byte[] data = null;
        int length = this.index - this.mark - lengthSubtract;
        if (length > 0) {
            data = new byte[length];
            System.arraycopy(this.buffer, this.mark, data, 0, data.length);
        }
        return data;
    }

    /**
     *
     */
    private void validateInput(InputStream is, byte[] delimiterBytes, int maxDataSize, int initialBufferSize) {
        if (is == null) {
            throw new IllegalArgumentException("'is' must not be null");
        } else if (maxDataSize <= 0) {
            throw new IllegalArgumentException("'maxDataSize' must be > 0");
        } else if (initialBufferSize <= 0) {
            throw new IllegalArgumentException("'initialBufferSize' must be > 0");
        } else if (delimiterBytes != null && delimiterBytes.length == 0){
            throw new IllegalArgumentException("'delimiterBytes' is an optional argument, but when provided its length must be > 0");
        }
    }

    @Override
    public void close() throws IOException {
        is.close();
    }
}
