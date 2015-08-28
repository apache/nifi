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
import java.io.OutputStream;

public class SynchronizedByteCountingOutputStream extends ByteCountingOutputStream {

    public SynchronizedByteCountingOutputStream(final OutputStream out) {
        super(out);
    }

    public SynchronizedByteCountingOutputStream(final OutputStream out, final long byteCount) {
        super(out, byteCount);
    }

    @Override
    public synchronized void flush() throws IOException {
        super.flush();
    }

    @Override
    public synchronized void close() throws IOException {
        super.close();
    }

    @Override
    public synchronized long getBytesWritten() {
        return super.getBytesWritten();
    }

    @Override
    public synchronized OutputStream getWrappedStream() {
        return super.getWrappedStream();
    }

    @Override
    public synchronized void write(final byte[] b) throws IOException {
        super.write(b);
    }

    @Override
    public synchronized void write(final int b) throws IOException {
        super.write(b);
    }

    @Override
    public synchronized void write(final byte[] b, final int off, final int len) throws IOException {
        super.write(b, off, len);
    }
}
