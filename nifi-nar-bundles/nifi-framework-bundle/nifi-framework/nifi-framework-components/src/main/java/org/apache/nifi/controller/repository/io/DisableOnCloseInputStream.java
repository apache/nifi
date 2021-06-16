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

/**
 * Wraps an existing InputStream, so that when {@link InputStream#close()} is called, the underlying InputStream is NOT closed but this InputStream can no longer be written to
 */
public class DisableOnCloseInputStream extends InputStream {

    private final InputStream wrapped;
    private boolean closed = false;

    public DisableOnCloseInputStream(final InputStream wrapped) {
        this.wrapped = wrapped;
    }

    @Override
    public int read() throws IOException {
        checkClosed();
        return wrapped.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        checkClosed();
        return wrapped.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        checkClosed();
        return wrapped.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        checkClosed();
        return wrapped.skip(n);
    }

    @Override
    public int available() throws IOException {
        return wrapped.available();
    }

    private void checkClosed() throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }
    }

    @Override
    public void close() throws IOException {
        closed = true;
    }

    @Override
    public void mark(int readlimit) {
        if (closed == false) {
            wrapped.mark(readlimit);
        }
    }

    @Override
    public synchronized void reset() throws IOException {
        checkClosed();
        wrapped.reset();
    }

    @Override
    public boolean markSupported() {
        return wrapped.markSupported();
    }
}
