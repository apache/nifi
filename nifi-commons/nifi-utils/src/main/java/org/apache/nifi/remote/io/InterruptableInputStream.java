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
package org.apache.nifi.remote.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.nifi.remote.exception.TransmissionDisabledException;

public class InterruptableInputStream extends InputStream {

    private volatile boolean interrupted = false;
    private final InputStream in;

    public InterruptableInputStream(final InputStream in) {
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        return in.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        return in.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        return in.read(b, off, len);
    }

    @Override
    public int available() throws IOException {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        return in.available();
    }

    @Override
    public void close() throws IOException {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        in.close();
    }

    @Override
    public synchronized void mark(int readlimit) {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        in.mark(readlimit);
    }

    @Override
    public boolean markSupported() {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        return in.markSupported();
    }

    @Override
    public synchronized void reset() throws IOException {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        in.reset();
    }

    @Override
    public long skip(long n) throws IOException {
        if (interrupted) {
            throw new TransmissionDisabledException();
        }

        return in.skip(n);
    }

    public void interrupt() {
        interrupted = true;
    }
}
