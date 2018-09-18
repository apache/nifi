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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.SocketChannel;
import org.apache.nifi.remote.io.InterruptableOutputStream;
import org.apache.nifi.remote.protocol.CommunicationsOutput;
import org.apache.nifi.stream.io.ByteCountingOutputStream;

public class SocketChannelOutput implements CommunicationsOutput {

    private final SocketChannelOutputStream socketOutStream;
    private final ByteCountingOutputStream countingOut;
    private final OutputStream bufferedOut;
    private final InterruptableOutputStream interruptableOut;

    public SocketChannelOutput(final SocketChannel socketChannel) throws IOException {
        socketOutStream = new SocketChannelOutputStream(socketChannel);
        countingOut = new ByteCountingOutputStream(socketOutStream);
        bufferedOut = new BufferedOutputStream(countingOut);
        interruptableOut = new InterruptableOutputStream(bufferedOut);
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        return interruptableOut;
    }

    public void setTimeout(final int timeout) {
        socketOutStream.setTimeout(timeout);
    }

    @Override
    public long getBytesWritten() {
        return countingOut.getBytesWritten();
    }

    public void interrupt() {
        interruptableOut.interrupt();
    }
}
