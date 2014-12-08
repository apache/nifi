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
package org.apache.nifi.io.nio;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import org.apache.nifi.io.nio.consumer.StreamConsumerFactory;

/**
 *
 * @author none
 */
public final class SocketChannelReader extends AbstractChannelReader {

    public SocketChannelReader(final String id, final SelectionKey key, final BufferPool empties, final StreamConsumerFactory consumerFactory) {
        super(id, key, empties, consumerFactory);
    }

    /**
     * Receives TCP data from the socket channel for the given key.
     *
     * @param key
     * @param buffer
     * @return
     * @throws IOException
     */
    @Override
    protected int fillBuffer(final SelectionKey key, final ByteBuffer buffer) throws IOException {
        int bytesRead = 0;
        final SocketChannel sChannel = (SocketChannel) key.channel();
        while (key.isValid() && key.isReadable()) {
            bytesRead = sChannel.read(buffer);
            if (bytesRead <= 0) {
                break;
            }
        }
        return bytesRead;
    }
}
