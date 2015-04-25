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
package org.apache.nifi.util.search;

import java.nio.charset.Charset;
import java.util.Arrays;

/**
 * This is an immutable thread safe object representing a search term
 *
 */
public class SearchTerm<T> {

    private final byte[] bytes;
    private final int hashCode;
    private final T reference;

    /**
     * Constructs a SearchTerm. Defensively copies the given byte array
     *
     * @param bytes the bytes of the search term
     * @throws IllegalArgumentException if given bytes are null or 0 length
     */
    public SearchTerm(final byte[] bytes) {
        this(bytes, true, null);
    }

    /**
     * Constructs a search term. Optionally performs a defensive copy of the
     * given byte array. If the caller indicates a defensive copy is not
     * necessary then they must not change the given arrays state any longer
     *
     * @param bytes the bytes of the new search term
     * @param defensiveCopy if true will make a defensive copy; false otherwise
     * @param reference a holder for an object which can be retrieved when this search term hits
     */
    public SearchTerm(final byte[] bytes, final boolean defensiveCopy, final T reference) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalArgumentException();
        }
        if (defensiveCopy) {
            this.bytes = Arrays.copyOf(bytes, bytes.length);
        } else {
            this.bytes = bytes;
        }
        this.hashCode = Arrays.hashCode(this.bytes);
        this.reference = reference;
    }

    public int get(final int index) {
        return bytes[index] & 0xff;
    }

    /**
     * @return size in of search term in bytes
     */
    public int size() {
        return bytes.length;
    }

    /**
     * @return reference object for this given search term
     */
    public T getReference() {
        return reference;
    }

    /**
     * Determines if the given window starts with the same bytes as this term
     *
     * @param window bytes from the haystack being evaluated
     * @param windowLength The length of the window to consider
     * @return true if this term starts with the same bytes of the given window
     */
    public boolean startsWith(byte[] window, int windowLength) {
        if (windowLength > window.length) {
            throw new IndexOutOfBoundsException();
        }
        if (bytes.length < windowLength) {
            return false;
        }
        for (int i = 0; i < bytes.length && i < windowLength; i++) {
            if (bytes[i] != window[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return a defensive copy of the internal byte structure
     */
    public byte[] getBytes() {
        return Arrays.copyOf(bytes, bytes.length);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SearchTerm other = (SearchTerm) obj;
        if (this.hashCode != other.hashCode) {
            return false;
        }
        return Arrays.equals(this.bytes, other.bytes);
    }

    @Override
    public String toString() {
        return new String(bytes);
    }

    public String toString(final Charset charset) {
        return new String(bytes, charset);
    }
}
