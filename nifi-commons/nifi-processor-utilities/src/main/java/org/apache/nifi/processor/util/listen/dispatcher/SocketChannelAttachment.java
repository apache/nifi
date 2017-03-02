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
package org.apache.nifi.processor.util.listen.dispatcher;

import org.apache.nifi.remote.io.socket.ssl.SSLSocketChannel;

import java.nio.ByteBuffer;

/**
 * Wrapper class so we can attach a buffer and/or an SSLSocketChannel to the selector key.
 * */
public class SocketChannelAttachment {

    private final ByteBuffer byteBuffer;
    private final SSLSocketChannel sslSocketChannel;

    public SocketChannelAttachment(final ByteBuffer byteBuffer, final SSLSocketChannel sslSocketChannel) {
        this.byteBuffer = byteBuffer;
        this.sslSocketChannel = sslSocketChannel;
    }

    public ByteBuffer getByteBuffer() {
        return byteBuffer;
    }

    public SSLSocketChannel getSslSocketChannel() {
        return sslSocketChannel;
    }

}
