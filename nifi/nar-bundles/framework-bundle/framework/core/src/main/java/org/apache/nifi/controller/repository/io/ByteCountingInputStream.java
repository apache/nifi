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

public class ByteCountingInputStream extends InputStream {

    private final LongHolder bytesReadHolder;
    private final InputStream in;
    private long bytesSkipped = 0L;

    public ByteCountingInputStream(final InputStream in, final LongHolder longHolder) {
        this.in = in;
        this.bytesReadHolder = longHolder;
    }

    @Override
    public int read() throws IOException {
        final int fromSuper = in.read();
        if (fromSuper >= 0) {
            bytesReadHolder.increment(1);
        }
        return fromSuper;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final int fromSuper = in.read(b, off, len);
        if (fromSuper >= 0) {
            bytesReadHolder.increment(fromSuper);
        }

        return fromSuper;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public long skip(final long n) throws IOException {
        final long skipped = in.skip(n);
        bytesSkipped += skipped;
        return skipped;
    }

    @Override
    public int available() throws IOException {
        return in.available();
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

    @Override
    public void close() throws IOException {
        in.close();
    }

    public long getBytesRead() {
        return bytesReadHolder.getValue();
    }

    public long getBytesSkipped() {
        return bytesSkipped;
    }

    public long getStreamLocation() {
        return getBytesRead() + getBytesSkipped();
    }
}
