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
package org.apache.nifi.remote.io.socket.ssl;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.stream.io.ByteCountingInputStream;

public class SSLSocketChannelInput implements CommunicationsInput {

    private final SSLSocketChannelInputStream in;
    private final ByteCountingInputStream countingIn;
    private final InputStream bufferedIn;

    public SSLSocketChannelInput(final SSLSocketChannel socketChannel) {
        in = new SSLSocketChannelInputStream(socketChannel);
        countingIn = new ByteCountingInputStream(in);
        this.bufferedIn = new BufferedInputStream(countingIn);
    }

    @Override
    public InputStream getInputStream() throws IOException {
        return bufferedIn;
    }

    public boolean isDataAvailable() throws IOException {
        return bufferedIn.available() > 0;
    }

    @Override
    public long getBytesRead() {
        return countingIn.getBytesRead();
    }

    @Override
    public void consume() throws IOException {
        in.consume();
    }
}
