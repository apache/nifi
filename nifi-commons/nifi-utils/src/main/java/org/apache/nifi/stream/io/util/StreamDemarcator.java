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

import java.io.IOException;
import java.io.InputStream;

/**
 * The <code>StreamDemarcator</code> class takes an input stream and demarcates
 * it so it could be read (see {@link #nextToken()}) as individual byte[]
 * demarcated by the provided delimiter (see 'delimiterBytes'). If delimiter is
 * not provided the entire stream will be read into a single token which may
 * result in {@link OutOfMemoryError} if stream is too large. The 'maxDataSize'
 * controls the maximum size of the buffer that accumulates a token.
 * <p>
 * NOTE: Not intended for multi-thread usage hence not Thread-safe.
 * </p>
 */
public class StreamDemarcator extends AbstractDemarcator {

    private final byte[] delimiterBytes;

    /**
     * Constructs a new instance
     *
     * @param is
     *            instance of {@link InputStream} representing the data
     * @param delimiterBytes
     *            byte array representing delimiter bytes used to split the
     *            input stream. Can be 'null'. NOTE: the 'null' is allowed only
     *            for convenience and consistency since without delimiter this
     *            class is no different then BufferedReader which reads the
     *            entire stream into a byte array and there may be a more
     *            efficient ways to do that (if that is the case).
     * @param maxDataSize
     *            maximum size of data derived from the input stream. This means
     *            that neither {@link InputStream} nor its individual tokens (if
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
     *            input stream. Can be 'null'. NOTE: the 'null' is allowed only
     *            for convenience and consistency since without delimiter this
     *            class is no different then BufferedReader which reads the
     *            entire stream into a byte array and there may be a more
     *            efficient ways to do that (if that is the case).
     * @param maxDataSize
     *            maximum size of data derived from the input stream. This means
     *            that neither {@link InputStream} nor its individual tokens (if
     *            delimiter is used) can ever be greater then this size.
     * @param initialBufferSize
     *            initial size of the buffer used to buffer {@link InputStream}
     *            or its parts (if delimiter is used) to create its byte[]
     *            representation. Must be positive integer. The buffer will grow
     *            automatically as needed up to the Integer.MAX_VALUE;
     *
     */
    public StreamDemarcator(InputStream is, byte[] delimiterBytes, int maxDataSize, int initialBufferSize) {
        super(is, maxDataSize, initialBufferSize);
        this.validate(delimiterBytes);
        this.delimiterBytes = delimiterBytes;
    }

    /**
     * Will read the next data token from the {@link InputStream} returning null
     * when it reaches the end of the stream.
     *
     * @throws IOException if unable to read from the stream
     */
    public byte[] nextToken() throws IOException {
        byte[] token = null;
        int j = 0;
        nextTokenLoop:
        while (token == null && this.availableBytesLength != -1) {
            if (this.index >= this.availableBytesLength) {
                this.fill();
            }
            if (this.availableBytesLength != -1) {
                byte byteVal;
                int i;
                for (i = this.index; i < this.availableBytesLength; i++) {
                    byteVal = this.buffer[i];

                    boolean delimiterFound = false;
                    if (this.delimiterBytes != null && this.delimiterBytes[j] == byteVal) {
                        if (++j == this.delimiterBytes.length) {
                            delimiterFound = true;
                        }
                    } else {
                        j = 0;
                    }

                    if (delimiterFound) {
                        this.index = i + 1;
                        int size = this.index - this.mark - this.delimiterBytes.length;
                        token = this.extractDataToken(size);
                        this.mark = this.index;
                        j = 0;
                        if (token != null) {
                            break nextTokenLoop;
                        }
                    }
                }
                this.index = i;
            } else {
                token = this.extractDataToken(this.index - this.mark);
            }
        }

        return token;
    }


    /**
     * Validates prerequisites for constructor arguments
     */
    private void validate(byte[] delimiterBytes) {
        if (delimiterBytes != null && delimiterBytes.length == 0) {
            throw new IllegalArgumentException("'delimiterBytes' is an optional argument, but when provided its length must be > 0");
        }
    }
}
