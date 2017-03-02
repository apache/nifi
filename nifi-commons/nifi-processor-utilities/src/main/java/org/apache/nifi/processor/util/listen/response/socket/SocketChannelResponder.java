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
package org.apache.nifi.processor.util.listen.response.socket;

import org.apache.nifi.processor.util.listen.response.ChannelResponder;
import org.apache.nifi.processor.util.listen.response.ChannelResponse;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A ChannelResponder for SocketChannels. The SocketChannel should first be registered with a selector,
 * upon being selected for writing the respond() method should be executed.
 */
public class SocketChannelResponder implements ChannelResponder<SocketChannel> {

    protected final List<ChannelResponse> responses;
    protected final SocketChannel socketChannel;

    public SocketChannelResponder(final SocketChannel socketChannel) {
        this.responses = new ArrayList<>();
        this.socketChannel = socketChannel;
    }

    @Override
    public SocketChannel getChannel() {
        return socketChannel;
    }

    @Override
    public List<ChannelResponse> getResponses() {
        return Collections.unmodifiableList(responses);
    }

    @Override
    public void addResponse(ChannelResponse response) {
        this.responses.add(response);
    }

    @Override
    public void respond() throws IOException {
        for (final ChannelResponse response : responses) {
            final ByteBuffer responseBuffer = ByteBuffer.wrap(response.toByteArray());

            while (responseBuffer.hasRemaining()) {
                socketChannel.write(responseBuffer);
            }
        }
    }

}
