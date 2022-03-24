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
import java.nio.BufferOverflowException;

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

    /*
     * The size of the initial buffer. Its value is also used when bufer needs
     * to be expanded.
     */
    private final int initialBufferSize;

    /*
     * The maximum allowed size of the token. In the event such size is exceeded
     * TokenTooLargeException is thrown.
     */
    private final int maxDataSize;

    /*
     * Buffer into which the bytes are read from the provided stream. The size
     * of the buffer is defined by the 'initialBufferSize' provided in the
     * constructor or defaults to the value of INIT_BUFFER_SIZE constant.
     */
    byte[] buffer;

    /*
     * Starting offset of the demarcated token within the current 'buffer'.
     */
    int index;

    /*
     * Starting offset of the demarcated token within the current 'buffer'. Keep
     * in mind that while most of the time it is the same as the 'index' it may
     * also have a value of 0 at which point it serves as a signal to the fill()
     * operation that buffer needs to be expended if end of token is not reached
     * (see fill() operation for more details).
     */
    int mark;

    /*
     * Starting offset (from the beginning of the stream) of the demarcated
     * token.
     */
    long offset;

    /*
     * The length of the bytes valid for reading. It is different from the
     * buffer length, since this number may be smaller (e.g., at he end of the
     * stream) then actual buffer length. It is set by the fill() operation
     * every time more bytes read into buffer.
     */
    int availableBytesLength;

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
        this.is.close();
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
                long expandedSize = this.buffer.length + this.initialBufferSize;
                if (expandedSize > Integer.MAX_VALUE) {
                    throw new BufferOverflowException(); // will probably OOM before this will ever happen, but just in case.
                }
                byte[] newBuff = new byte[(int) expandedSize];
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
        /*
         * The do/while pattern is used here similar to the way it is used in
         * BufferedReader essentially protecting from assuming the EOS until it
         * actually is since not every implementation of InputStream guarantees
         * that bytes are always available while the stream is open.
         */
        do {
            bytesRead = this.is.read(this.buffer, this.index, this.buffer.length - this.index);
        } while (bytesRead == 0);
        this.availableBytesLength = bytesRead != -1 ? this.index + bytesRead : -1;
    }

    /**
     * Will extract data token of the provided length from the current buffer
     * starting at the 'mark'.
     */
    byte[] extractDataToken(int length) throws IOException {
        if (length > this.maxDataSize) {
            throw new TokenTooLargeException("A message in the stream exceeds the maximum allowed message size of " + this.maxDataSize + " bytes.");
        }
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
