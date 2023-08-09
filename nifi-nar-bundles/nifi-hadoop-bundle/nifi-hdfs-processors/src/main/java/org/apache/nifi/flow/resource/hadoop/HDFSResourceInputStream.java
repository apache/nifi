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
package org.apache.nifi.flow.resource.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.nifi.processors.hadoop.HDFSResourceHelper;

final class HDFSResourceInputStream extends InputStream {
    private final FileSystem fileSystem;
    private final FSDataInputStream payload;

    HDFSResourceInputStream(final FileSystem fileSystem, final FSDataInputStream payload) {
        this.fileSystem = fileSystem;
        this.payload = payload;
    }

    @Override
    public int read() throws IOException {
        return payload.read();
    }

    @Override
    public int read(final byte[] b) throws IOException {
        return payload.read(b);
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        return payload.read(b, off, len);
    }

    @Override
    public byte[] readAllBytes() throws IOException {
        return payload.readAllBytes();
    }

    @Override
    public byte[] readNBytes(final int len) throws IOException {
        return payload.readNBytes(len);
    }

    @Override
    public int readNBytes(final byte[] b, final int off, final int len) throws IOException {
        return payload.readNBytes(b, off, len);
    }

    @Override
    public long skip(final long n) throws IOException {
        return payload.skip(n);
    }

    @Override
    public void skipNBytes(final long n) throws IOException {
        payload.skipNBytes(n);
    }

    @Override
    public int available() throws IOException {
        return payload.available();
    }

    @Override
    public void close() throws IOException {
        payload.close();
        HDFSResourceHelper.closeFileSystem(fileSystem);
    }

    @Override
    public synchronized void mark(final int readlimit) {
        payload.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
        payload.reset();
    }

    @Override
    public boolean markSupported() {
        return payload.markSupported();
    }

    @Override
    public long transferTo(final OutputStream out) throws IOException {
        return payload.transferTo(out);
    }
}
