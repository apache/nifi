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
package org.apache.nifi.util;

import java.util.Arrays;

/**
 * <p>
 * A RingBuffer that can be used to scan byte sequences for subsequences.
 * </p>
 *
 * <p>
 * This class implements an efficient naive search algorithm, which allows the user of the library to identify byte sequences in a stream on-the-fly so that the stream can be segmented without having
 * to buffer the data.
 * </p>
 *
 * <p>
 * The intended usage paradigm is:
 * <code>
 * <pre>
 * final byte[] searchSequence = ...;
 * final CircularBuffer buffer = new CircularBuffer(searchSequence);
 * while ((int nextByte = in.read()) > 0) {
 *      if ( buffer.addAndCompare(nextByte) ) {
 *          // This byte is the last byte in the given sequence
 *      } else {
 *          // This byte does not complete the given sequence
 *      }
 * }
 * </pre>
 * </code>
 * </p>
 */
public class NaiveSearchRingBuffer {

    private final byte[] lookingFor;
    private final int[] buffer;
    private int insertionPointer = 0;
    private int bufferSize = 0;

    public NaiveSearchRingBuffer(final byte[] lookingFor) {
        this.lookingFor = lookingFor;
        this.buffer = new int[lookingFor.length];
        Arrays.fill(buffer, -1);
    }

    /**
     * @return the contents of the internal buffer, which represents the last X bytes added to the buffer, where X is the minimum of the number of bytes added to the buffer or the length of the byte
     * sequence for which we are looking
     */
    public byte[] getBufferContents() {
        final int contentLength = Math.min(lookingFor.length, bufferSize);
        final byte[] contents = new byte[contentLength];
        for (int i = 0; i < contentLength; i++) {
            final byte nextByte = (byte) buffer[(insertionPointer + i) % lookingFor.length];
            contents[i] = nextByte;
        }
        return contents;
    }

    /**
     * @return the oldest byte in the buffer
     */
    public int getOldestByte() {
        return buffer[insertionPointer];
    }

    /**
     * @return <code>true</code> if the number of bytes that have been added to the buffer is at least equal to the length of the byte sequence for which we are searching
     */
    public boolean isFilled() {
        return bufferSize >= buffer.length;
    }

    /**
     * Clears the internal buffer so that a new search may begin
     */
    public void clear() {
        Arrays.fill(buffer, -1);
        insertionPointer = 0;
        bufferSize = 0;
    }

    /**
     * Add the given byte to the buffer and notify whether or not the byte completes the desired byte sequence.
     *
     * @param data the data to add to the buffer
     * @return <code>true</code> if this byte completes the byte sequence, <code>false</code> otherwise.
     */
    public boolean addAndCompare(final byte data) {
        buffer[insertionPointer] = data;
        insertionPointer = (insertionPointer + 1) % lookingFor.length;

        bufferSize++;
        if (bufferSize < lookingFor.length) {
            return false;
        }

        for (int i = 0; i < lookingFor.length; i++) {
            final byte compare = (byte) buffer[(insertionPointer + i) % lookingFor.length];
            if (compare != lookingFor[i]) {
                return false;
            }
        }

        return true;
    }
}
