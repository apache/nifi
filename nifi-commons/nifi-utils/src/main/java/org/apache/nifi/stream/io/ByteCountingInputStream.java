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

public class ByteCountingInputStream extends InputStream {

    private final InputStream in;
    private long bytesRead = 0L;
    private long bytesSkipped = 0L;

    private long bytesSinceMark = 0L;

    public ByteCountingInputStream(final InputStream in) {
        this.in = in;
    }

    public ByteCountingInputStream(final InputStream in, final long initialOffset) {
        this.in = in;
        this.bytesSkipped = initialOffset;
    }

    @Override
    public int read() throws IOException {
        final int fromSuper = in.read();
        if (fromSuper >= 0) {
            bytesRead++;
            bytesSinceMark++;
        }
        return fromSuper;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final int fromSuper = in.read(b, off, len);
        if (fromSuper >= 0) {
            bytesRead += fromSuper;
            bytesSinceMark += fromSuper;
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
        if (skipped >= 0) {
            bytesSkipped += skipped;
            bytesSinceMark += skipped;
        }
        return skipped;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public long getBytesSkipped() {
        return bytesSkipped;
    }

    public long getBytesConsumed() {
        return getBytesRead() + getBytesSkipped();
    }

    @Override
    public void mark(final int readlimit) {
        in.mark(readlimit);

        bytesSinceMark = 0L;
    }

    @Override
    public boolean markSupported() {
        return in.markSupported();
    }

    @Override
    public void reset() throws IOException {
        in.reset();
        bytesRead -= bytesSinceMark;
        bytesSinceMark = 0L;
    }

    @Override
    public void close() throws IOException {
        in.close();
    }

    @Override
    public int available() throws IOException {
        return in.available();
    }
}
