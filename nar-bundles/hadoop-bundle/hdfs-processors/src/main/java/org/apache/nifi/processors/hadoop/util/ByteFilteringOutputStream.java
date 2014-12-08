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
package org.apache.nifi.processors.hadoop.util;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * This class allows the user to define byte-array filters or single-byte
 * filters that will modify the content that is written to the underlying
 * stream. Each filter can be given a maximum number of replacements that it
 * should perform.
 */
public class ByteFilteringOutputStream extends FilterOutputStream {

    private final List<Filter> multiByteFilters = new ArrayList<>();
    private final List<Filter> singleByteFilters = new ArrayList<>();
    private final OutputStream wrapped;

    public ByteFilteringOutputStream(final OutputStream toWrap)
            throws IOException {
        super(toWrap);
        this.wrapped = toWrap;
    }

    @Override
    public synchronized void write(byte[] buffer, int offset, int length)
            throws IOException {
        for (final Filter filter : multiByteFilters) {
            if (filter.matches(buffer, offset, length)) {
                wrapped.write(filter.replaceWith);
            } else {
                wrapped.write(buffer, offset, length);
            }
        }
    }

    @Override
    public synchronized void write(int data) throws IOException {
        for (final Filter filter : singleByteFilters) {
            if (filter.matches((byte) data)) {
                wrapped.write(filter.replaceWith);
            } else {
                wrapped.write(data);
            }
        }
    }

    /**
     * Causes this stream to write <tt>replaceWith</tt> in place of
     * <tt>toReplace</tt> if {@link #write(byte[], int, int)} is called where
     * the value to write is equal to
     * <tt>toReplace</tt>.
     * <p/>
     * @param toReplace the byte array to replace
     * @param replaceWith the byte array to be substituted
     */
    public void addFilter(final byte[] toReplace, final byte[] replaceWith) {
        addFilter(toReplace, replaceWith, -1);
    }

    /**
     * Causes this stream to write <tt>replaceWith</tt> in place of
     * <tt>toReplace</tt> if {@link #write(byte[], int, int)} is called where
     * the value to write is equal to
     * <tt>toReplace</tt>.
     * <p/>
     * @param toReplace the byte array to replace
     * @param replaceWith the byte array to be substituted
     * @param maxReplacements the maximum number of replacements that should be
     * made
     */
    public void addFilter(final byte[] toReplace, final byte[] replaceWith, final int maxReplacements) {
        multiByteFilters.add(new Filter(toReplace, replaceWith, maxReplacements));
    }

    /**
     * Causes this stream to write <tt>replaceWith</tt> in place of
     * <tt>toReplace</tt> if {@link #write(int)} is called where the value to
     * write is equal to
     * <tt>toReplace</tt>.
     * <p/>
     * @param toReplace the byte to replace
     * @param replaceWith the byte to be substituted
     */
    public void addFilter(final byte toReplace, final byte replaceWith) {
        addFilter(toReplace, replaceWith, -1);
    }

    /**
     * Causes this stream to write <tt>replaceWith</tt> in place of
     * <tt>toReplace</tt> if {@link #write(int)} is called where the value to
     * write is equal to
     * <tt>toReplace</tt>.
     * <p/>
     * @param toReplace the byte to replace
     * @param replaceWith the byte to be substituted
     * @param maxReplacements the maximum number of replacements that should be
     * made
     */
    public void addFilter(final byte toReplace, final byte replaceWith, final int maxReplacements) {
        singleByteFilters.add(new Filter(new byte[]{toReplace}, new byte[]{replaceWith}, maxReplacements));
    }

    static class Filter {

        final byte[] toReplace;
        final byte[] replaceWith;
        final int maxMatches;
        int numMatches = 0;

        public Filter(final byte[] toReplace, final byte[] replaceWith, final int maxReplacements) {
            this.toReplace = toReplace;
            this.replaceWith = replaceWith;
            this.maxMatches = maxReplacements;
        }

        public boolean matches(final byte candidate) {
            return matches(new byte[]{candidate}, 0, 1);
        }

        public boolean matches(final byte[] candidate, final int offset, final int length) {
            final boolean finsihedReplacing = (numMatches >= maxMatches && maxMatches > -1);

            if (finsihedReplacing || (length != toReplace.length)) {
                return false;
            }

            final byte[] compare;
            if (length == candidate.length) {
                compare = candidate;
            } else {
                compare = new byte[length];
                System.arraycopy(candidate, offset, compare, 0, length);
            }

            final boolean match = Arrays.equals(compare, toReplace);
            if (match) {
                numMatches++;
            }

            return match;
        }
    }
}
