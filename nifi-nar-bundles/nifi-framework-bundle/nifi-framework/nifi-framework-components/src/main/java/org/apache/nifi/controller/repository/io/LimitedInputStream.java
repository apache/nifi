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
package org.apache.nifi.controller.repository.io;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.LongSupplier;

public class LimitedInputStream extends InputStream {

    private final InputStream in;
    private final long limit;
    private final LongSupplier limitSupplier;
    private long bytesRead = 0;
    private long markOffset = -1L;

    public LimitedInputStream(final InputStream in, final LongSupplier limitSupplier) {
        this.in = in;
        this.limitSupplier = Objects.requireNonNull(limitSupplier);
        this.limit = -1;
    }

    public LimitedInputStream(final InputStream in, final long limit) {
        this.in = in;
        this.limit = limit;
        this.limitSupplier = null;
    }

    private long getLimit() {
        return limitSupplier == null ? limit : limitSupplier.getAsLong();
    }

    @Override
    public int read() throws IOException {
        if (bytesRead >= getLimit()) {
            return -1;
        }

        final int val = in.read();
        if (val > -1) {
            bytesRead++;
        }
        return val;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        final long limit = getLimit();
        if (bytesRead >= limit) {
            return -1;
        }

        final int maxToRead = (int) Math.min(b.length, limit - bytesRead);

        final int val = in.read(b, 0, maxToRead);
        if (val > 0) {
            bytesRead += val;
        }
        return val;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final long limit = getLimit();
        if (bytesRead >= limit) {
            return -1;
        }

        final int maxToRead = (int) Math.min(len, limit - bytesRead);

        final int val = in.read(b, off, maxToRead);
        if (val > 0) {
            bytesRead += val;
        }
        return val;
    }

    @Override
    public long skip(final long n) throws IOException {
        final long skipped = in.skip(Math.min(n, getLimit() - bytesRead));
        bytesRead += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return (int)(getLimit() - bytesRead);
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void mark(int readlimit) {
        in.mark(readlimit);
        markOffset = bytesRead;
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public void reset() throws IOException {
        in.reset();

        if (markOffset >= 0) {
            bytesRead = markOffset;
        }
        markOffset = -1;
    }
}
