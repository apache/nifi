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
package org.apache.nifi.parquet.stream;

import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.parquet.io.DelegatingSeekableInputStream;

import java.io.IOException;

public class NifiSeekableInputStream extends DelegatingSeekableInputStream {

    private final ByteCountingInputStream input;

    public NifiSeekableInputStream(final ByteCountingInputStream input) {
        super(input);
        this.input = input;
        this.input.mark(Integer.MAX_VALUE);
    }

    @Override
    public long getPos() throws IOException {
        return input.getBytesConsumed();
    }

    @Override
    public void seek(long newPos) throws IOException {
        final long currentPos = getPos();
        if (newPos == currentPos) {
            return;
        }

        if (newPos < currentPos) {
            // seeking backwards so first reset back to beginning of the stream then seek
            input.reset();
            input.mark(Integer.MAX_VALUE);
        }

        // must call getPos() again in case reset was called above
        StreamUtils.skip(input, newPos - getPos());
    }

    @Override
    public boolean markSupported() {
        return false;
    }

    @Override
    public synchronized void mark(int readlimit) {
        throw new UnsupportedOperationException("Mark/reset is not supported");
    }

    @Override
    public synchronized void reset() throws IOException {
        throw new UnsupportedOperationException("Mark/reset is not supported");
    }
}
