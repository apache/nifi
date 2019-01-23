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

import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

public class StandardInput implements CommunicationsInput {

    private static final Logger LOG = LoggerFactory.getLogger(StandardInput.class);

    private final InputStream baseIn;
    private final ByteCountingInputStream countingIn;
    private final InputStream bufferedIn;
    private final InterruptableInputStream interruptableIn;

    public StandardInput(final InputStream inputStream) {
        this(inputStream, true);
    }

    public StandardInput(final InputStream inputStream, final boolean wrapWithBufferedInputStream) {
        baseIn = inputStream;
        countingIn = new ByteCountingInputStream(baseIn);
        bufferedIn = wrapWithBufferedInputStream ? new BufferedInputStream(countingIn) : countingIn;
        interruptableIn = new InterruptableInputStream(bufferedIn);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return interruptableIn;
    }

    public void setTimeout(final int millis) {
    }

    public boolean isDataAvailable() {
        try {
            return interruptableIn.available() > 0;
        } catch (final Exception e) {
            return false;
        }
    }

    @Override
    public long getBytesRead() {
        return countingIn.getBytesRead();
    }

    public void interrupt() {
        interruptableIn.interrupt();
    }

    @Override
    public void consume() throws IOException {
        if (!isDataAvailable()) {
            return;
        }

        final byte[] b = new byte[4096];
        int bytesRead;
        do {
            bytesRead = interruptableIn.read(b);
        } while (bytesRead > 0);
    }
}
