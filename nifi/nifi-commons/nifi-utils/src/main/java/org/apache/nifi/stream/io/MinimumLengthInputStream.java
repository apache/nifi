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
package org.apache.nifi.stream.io;

import java.io.EOFException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * An InputStream that will throw EOFException if the underlying InputStream runs out of data before reaching the configured minimum amount of data
 */
public class MinimumLengthInputStream extends FilterInputStream {

    private final long minLength;
    private long consumedCount = 0L;

    public MinimumLengthInputStream(final InputStream in, final long minLength) {
        super(in);
        this.minLength = minLength;
    }

    @Override
    public int read() throws IOException {
        final int b = super.read();
        if (b < 0 && consumedCount < minLength) {
            throw new EOFException();
        }

        if (b >= 0) {
            consumedCount++;
        }

        return b;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    public int read(byte[] b, int off, int len) throws IOException {
        final int num = super.read(b, off, len);

        if (num < 0 && consumedCount < minLength) {
            throw new EOFException();
        }

        if (num >= 0) {
            consumedCount += num;
        }

        return num;
    }

    @Override
    public long skip(final long n) throws IOException {
        long skipped = super.skip(n);
        if (skipped < 1) {
            final int b = super.read();
            if (b >= 0) {
                skipped = 1;
            }
        }

        if (skipped < 0 && consumedCount < minLength) {
            throw new EOFException();
        }

        if (skipped >= 0) {
            consumedCount += skipped;
        }

        return skipped;
    }

}
