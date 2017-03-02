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
import java.nio.channels.DatagramChannel;
import java.nio.channels.SelectionKey;

import org.apache.nifi.io.nio.consumer.StreamConsumerFactory;

public final class DatagramChannelReader extends AbstractChannelReader {

    public static final int MAX_UDP_PACKET_SIZE = 65507;

    private final boolean readSingleDatagram;

    public DatagramChannelReader(final String id, final SelectionKey key, final BufferPool empties, final StreamConsumerFactory consumerFactory,
            final boolean readSingleDatagram) {
        super(id, key, empties, consumerFactory);
        this.readSingleDatagram = readSingleDatagram;
    }

    /**
     * Will receive UDP data from channel and won't receive anything unless the
     * given buffer has enough space for at least one full max udp packet.
     *
     * @param key selection key
     * @param buffer to fill
     * @return bytes read
     * @throws IOException if error filling buffer from channel
     */
    @Override
    protected int fillBuffer(final SelectionKey key, final ByteBuffer buffer) throws IOException {
        final DatagramChannel dChannel = (DatagramChannel) key.channel();
        final int initialBufferPosition = buffer.position();
        while (buffer.remaining() > MAX_UDP_PACKET_SIZE && key.isValid() && key.isReadable()) {
            if (dChannel.receive(buffer) == null || readSingleDatagram) {
                break;
            }
        }
        return buffer.position() - initialBufferPosition;
    }

}
