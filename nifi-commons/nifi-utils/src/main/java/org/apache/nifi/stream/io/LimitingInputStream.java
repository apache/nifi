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

import java.io.IOException;
import java.io.InputStream;

public class LimitingInputStream extends InputStream {

    private final InputStream in;
    private final long limit;
    private long bytesRead = 0;
    private volatile boolean limitReached = false;

    /**
     * Constructs a limited input stream whereby if the limit is reached all
     * subsequent calls to read will return a -1 and hasLimitReached() will
     * indicate true. The limit is inclusive so if all 100 bytes of a 100 byte
     * stream are read it will be true, otherwise false.
     *
     * @param in the underlying input stream
     * @param limit maximum length of bytes to read from underlying input stream
     */
    public LimitingInputStream(final InputStream in, final long limit) {
        this.in = in;
        this.limit = limit;
    }

    public boolean hasReachedLimit() throws IOException {
        return limitReached;
    }

    private int markLimitReached() {
        limitReached = true;
        return -1;
    }

    @Override
    public int read() throws IOException {
        if (bytesRead >= limit) {
            return markLimitReached();
        }

        final int val = in.read();
        if (val > -1) {
            bytesRead++;
        }
        return val;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        if (bytesRead >= limit) {
            return markLimitReached();
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
        if (bytesRead >= limit) {
            return markLimitReached();
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
        final long skipped = in.skip(Math.min(n, limit - bytesRead));
        bytesRead += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return in.available();
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public void mark(int readlimit) {
        in.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public void reset() throws IOException {
        in.reset();
    }

    public long getLimit() {
        return limit;
    }
}
