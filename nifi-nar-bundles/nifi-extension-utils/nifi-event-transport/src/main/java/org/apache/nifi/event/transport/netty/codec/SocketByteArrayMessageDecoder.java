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
package org.apache.nifi.event.transport.netty.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.apache.nifi.event.transport.message.ByteArrayMessage;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Message Decoder for bytes received from Socket Channels
 */
public class SocketByteArrayMessageDecoder extends MessageToMessageDecoder<byte[]> {
    /**
     * Decode bytes to Byte Array Message with remote address from Channel.remoteAddress()
     *
     * @param channelHandlerContext Channel Handler Context
     * @param bytes Message Bytes
     * @param decoded Decoded Messages
     */
    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final byte[] bytes, final List<Object> decoded) {
        final InetSocketAddress remoteAddress = (InetSocketAddress) channelHandlerContext.channel().remoteAddress();
        final String address = remoteAddress.getHostString();
        final ByteArrayMessage message = new ByteArrayMessage(bytes, address);
        decoded.add(message);
    }
}
