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
package org.apache.nifi.cluster.protocol.impl;

import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class CopyingInputStream extends FilterInputStream {

    private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    private final int maxBytesToCopy;
    private final InputStream in;

    public CopyingInputStream(final InputStream in, final int maxBytesToCopy) {
        super(in);
        this.maxBytesToCopy = maxBytesToCopy;
        this.in = in;
    }

    @Override
    public int read() throws IOException {
        final int delegateRead = in.read();
        if (delegateRead != -1 && getNumberOfBytesCopied() < maxBytesToCopy) {
            baos.write(delegateRead);
        }

        return delegateRead;
    }

    @Override
    public int read(byte[] b) throws IOException {
        final int delegateRead = in.read(b);
        if (delegateRead >= 0) {
            baos.write(b, 0, Math.min(delegateRead, maxBytesToCopy - getNumberOfBytesCopied()));
        }

        return delegateRead;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        final int delegateRead = in.read(b, off, len);
        if (delegateRead >= 0) {
            baos.write(b, off, Math.min(delegateRead, maxBytesToCopy - getNumberOfBytesCopied()));
        }

        return delegateRead;
    }

    public byte[] getBytesRead() {
        return baos.toByteArray();
    }

    public void writeBytes(final OutputStream out) throws IOException {
        baos.writeTo(out);
    }

    public int getNumberOfBytesCopied() {
        return baos.size();
    }
}
