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
package org.apache.nifi.remote.io.socket;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SocketChannel;
import org.apache.nifi.remote.io.InterruptableInputStream;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.stream.io.ByteCountingInputStream;

public class SocketChannelInput implements CommunicationsInput {

    private final SocketChannelInputStream socketIn;
    private final ByteCountingInputStream countingIn;
    private final InputStream bufferedIn;
    private final InterruptableInputStream interruptableIn;

    public SocketChannelInput(final SocketChannel socketChannel) throws IOException {
        this.socketIn = new SocketChannelInputStream(socketChannel);
        countingIn = new ByteCountingInputStream(socketIn);
        bufferedIn = new BufferedInputStream(countingIn);
        interruptableIn = new InterruptableInputStream(bufferedIn);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return interruptableIn;
    }

    public void setTimeout(final int millis) {
        socketIn.setTimeout(millis);
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
        socketIn.consume();
    }
}
