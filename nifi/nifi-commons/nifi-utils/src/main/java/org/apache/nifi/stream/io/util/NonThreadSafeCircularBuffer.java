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

import java.util.Arrays;

public class NonThreadSafeCircularBuffer {

    private final byte[] lookingFor;
    private final int[] buffer;
    private int insertionPointer = 0;
    private int bufferSize = 0;

    public NonThreadSafeCircularBuffer(final byte[] lookingFor) {
        this.lookingFor = lookingFor;
        buffer = new int[lookingFor.length];
        Arrays.fill(buffer, -1);
    }

    public byte[] getByteArray() {
        return lookingFor;
    }

    /**
     * Returns the oldest byte in the buffer
     *
     * @return the oldest byte
     */
    public int getOldestByte() {
        return buffer[insertionPointer];
    }

    public boolean isFilled() {
        return bufferSize >= buffer.length;
    }

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
