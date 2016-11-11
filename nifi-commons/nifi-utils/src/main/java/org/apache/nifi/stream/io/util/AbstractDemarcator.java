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
 * Base class for implementing streaming demarcators.
 * <p>
 * NOTE: Not intended for multi-thread usage hence not Thread-safe.
 * </p>
 */
abstract class AbstractDemarcator implements Closeable {

    final static int INIT_BUFFER_SIZE = 8192;

    private final InputStream is;

    private final int initialBufferSize;

    private final int maxDataSize;

    byte[] buffer;

    int index;

    int mark;

    long offset;

    int bufferLength;

    /**
     * Constructs an instance of demarcator with provided {@link InputStream}
     * and max buffer size. Each demarcated token must fit within max buffer
     * size, otherwise the exception will be raised.
     */
    AbstractDemarcator(InputStream is, int maxDataSize) {
        this(is, maxDataSize, INIT_BUFFER_SIZE);
    }

    /**
     * Constructs an instance of demarcator with provided {@link InputStream}
     * and max buffer size and initial buffer size. Each demarcated token must
     * fit within max buffer size, otherwise the exception will be raised.
     */
    AbstractDemarcator(InputStream is, int maxDataSize, int initialBufferSize) {
        this.validate(is, maxDataSize, initialBufferSize);
        this.is = is;
        this.initialBufferSize = initialBufferSize;
        this.buffer = new byte[initialBufferSize];
        this.maxDataSize = maxDataSize;
    }

    @Override
    public void close() throws IOException {
        // noop
    }

    /**
     * Will fill the current buffer from current 'index' position, expanding it
     * and or shuffling it if necessary. If buffer exceeds max buffer size a
     * {@link TokenTooLargeException} will be thrown.
     *
     * @throws IOException
     *             if unable to read from the stream
     */
    void fill() throws IOException {
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
        this.bufferLength = bytesRead != -1 ? this.index + bytesRead : -1;
        if (this.bufferLength > this.maxDataSize) {
            throw new TokenTooLargeException("A message in the stream exceeds the maximum allowed message size of "
                    + this.maxDataSize + " bytes.");
        }
    }

    /**
     * Will extract data token of the provided length from the current buffer.
     * The length of the data token is between the current 'mark' and 'index'.
     * If the above subtraction results in length 0, null is returned.
     */

    byte[] extractDataToken(int length) {
        byte[] data = null;
        if (length > 0) {
            data = new byte[length];
            System.arraycopy(this.buffer, this.mark, data, 0, data.length);
        }
        return data;
    }

    /**
     * Validates prerequisites for constructor arguments
     */
    private void validate(InputStream is, int maxDataSize, int initialBufferSize) {
        if (is == null) {
            throw new IllegalArgumentException("'is' must not be null");
        } else if (maxDataSize <= 0) {
            throw new IllegalArgumentException("'maxDataSize' must be > 0");
        } else if (initialBufferSize <= 0) {
            throw new IllegalArgumentException("'initialBufferSize' must be > 0");
        }
    }
}
