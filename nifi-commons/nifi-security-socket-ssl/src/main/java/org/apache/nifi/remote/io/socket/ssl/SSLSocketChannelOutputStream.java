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

import java.io.IOException;
import java.io.OutputStream;

public class SSLSocketChannelOutputStream extends OutputStream {

    private final SSLSocketChannel channel;

    public SSLSocketChannelOutputStream(final SSLSocketChannel channel) {
        this.channel = channel;
    }

    @Override
    public void write(final int b) throws IOException {
        channel.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        channel.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        channel.write(b, off, len);
    }

    /**
     * Closes the underlying SSLSocketChannel, which also will close the InputStream and the connection
     */
    @Override
    public void close() throws IOException {
        channel.close();
    }
}
