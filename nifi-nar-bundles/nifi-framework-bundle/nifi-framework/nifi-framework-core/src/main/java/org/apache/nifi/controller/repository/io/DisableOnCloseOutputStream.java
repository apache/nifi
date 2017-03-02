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
import java.io.OutputStream;

/**
 * Wraps an existing OutputStream, so that when {@link OutputStream#close()} is called, the underlying OutputStream is NOT closed but this OutputStream can no longer be written to
 */
public class DisableOnCloseOutputStream extends OutputStream {

    private final OutputStream wrapped;
    private boolean closed = false;

    public DisableOnCloseOutputStream(final OutputStream toWrap) {
        wrapped = toWrap;
    }

    @Override
    public void write(int b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        wrapped.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        wrapped.write(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        if (closed) {
            throw new IOException("Stream is closed");
        }

        wrapped.write(b);
    }

    @Override
    public void flush() throws IOException {
        wrapped.flush();
    }

    @Override
    public void close() throws IOException {
        wrapped.flush();
        closed = true;
    }
}
