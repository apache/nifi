/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.stream.io;

import org.apache.nifi.processor.DataUnit;

import java.io.IOException;
import java.io.InputStream;

public class MaxLengthInputStream extends InputStream {

    private static final String ERROR_MESSAGE_FORMAT = "Unable to read past maximum allowed length of %s %s";

    private final InputStream in;
    private final long limit;
    private long bytesRead = 0;
    private long markOffset = -1L;
    private final String errorMessage;

    /**
     * Constructs an input stream that will throw an exception if reading past a maximum length of bytes.
     *
     * @param in the underlying input stream
     * @param limit maximum length of bytes to read from underlying input stream
     */
    public MaxLengthInputStream(final InputStream in, final long limit) {
        this.in = in;
        this.limit = (limit + 1);
        if (limit >= DataUnit.MB.toB(1)) {
            this.errorMessage = ERROR_MESSAGE_FORMAT.formatted(DataUnit.B.toMB(limit), "MB");
        } else {
            this.errorMessage = ERROR_MESSAGE_FORMAT.formatted(limit, "B");
        }
    }

    @Override
    public int read() throws IOException {
        if (bytesRead >= limit) {
            throw new IOException(errorMessage);
        }

        final int val = in.read();
        if (val > -1) {
            bytesRead++;
        }
        return val;
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (bytesRead >= limit) {
            throw new IOException(errorMessage);
        }

        final int maxToRead = (int) Math.min(len, limit - bytesRead);

        final int val = in.read(b, off, maxToRead);
        if (val > 0) {
            bytesRead += val;
            if (bytesRead >= limit) {
                throw new IOException(errorMessage);
            }
        }
        return val;
    }

    @Override
    public long skip(final long n) throws IOException {
        final long toSkip = Math.min(n, limit - bytesRead);
        final long skipped = in.skip(toSkip);
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

    public long getLimit() {
        return limit;
    }
}
