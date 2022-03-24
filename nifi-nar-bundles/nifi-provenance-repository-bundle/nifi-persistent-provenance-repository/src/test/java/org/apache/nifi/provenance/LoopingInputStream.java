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

package org.apache.nifi.provenance;

import java.io.IOException;
import java.io.InputStream;

public class LoopingInputStream extends InputStream {

    private final byte[] buffer;
    private int index;

    private final byte[] header;
    private int headerIndex;
    private boolean headerComplete = false;

    public LoopingInputStream(final byte[] header, final byte[] toRepeat) {
        this.header = header;
        this.buffer = toRepeat;
    }

    @Override
    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    @Override
    public int read() throws IOException {
        if (headerComplete) {
            final byte nextByte = buffer[index++];
            if (index >= buffer.length) {
                index = 0;
            }

            final int returnValue = nextByte & 0xFF;
            return returnValue;
        } else {
            final byte nextByte = header[headerIndex++];
            if (headerIndex >= header.length) {
                headerComplete = true;
            }

            final int returnValue = nextByte & 0xFF;
            return returnValue;
        }
    }

    @Override
    public int read(final byte[] b, final int off, final int len) throws IOException {
        if (headerComplete) {
            final int toRead = Math.min(len, buffer.length - index);
            System.arraycopy(buffer, index, b, off, toRead);
            index += toRead;
            if (index >= buffer.length) {
                index = 0;
            }

            return toRead;
        } else {
            final int toRead = Math.min(len, header.length - headerIndex);
            System.arraycopy(header, headerIndex, b, off, toRead);
            headerIndex += toRead;
            if (headerIndex >= header.length) {
                headerComplete = true;
            }

            return toRead;
        }
    }

    @Override
    public int available() throws IOException {
        return 1;
    }

    @Override
    public void close() throws IOException {
    }
}
