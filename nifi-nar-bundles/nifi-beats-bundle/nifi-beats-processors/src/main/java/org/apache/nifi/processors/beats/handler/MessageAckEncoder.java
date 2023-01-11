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
package org.apache.nifi.processors.beats.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.beats.protocol.FrameType;
import org.apache.nifi.processors.beats.protocol.MessageAck;
import org.apache.nifi.processors.beats.protocol.ProtocolVersion;

import java.util.Objects;

/**
 * Beats Message Acknowledgement Encoder writes Protocol Version 2 ACK packets with a specified sequence number
 */
@ChannelHandler.Sharable
public class MessageAckEncoder extends MessageToByteEncoder<MessageAck> {
    private final ComponentLog log;

    /**
     * Message Acknowledgment Encoder with required arguments
     *
     * @param log Processor Log
     */
    public MessageAckEncoder(final ComponentLog log) {
        this.log = Objects.requireNonNull(log, "Component Log required");
    }

    /**
     * Encode Message Acknowledgement to the buffer with Protocol Version 2 and ACK Frame Type
     *
     * @param context Channel Handler Context
     * @param messageAck Message Acknowledgement containing Sequence Number
     * @param buffer Byte Buffer
     */
    @Override
    protected void encode(final ChannelHandlerContext context, final MessageAck messageAck, final ByteBuf buffer) {
        buffer.writeByte(ProtocolVersion.VERSION_2.getCode());
        buffer.writeByte(FrameType.ACK.getCode());

        final int sequenceNumber = messageAck.getSequenceNumber();
        buffer.writeInt(sequenceNumber);

        final Channel channel = context.channel();
        log.debug("Encoded Message Ack Sequence Number [{}] Local [{}] Remote [{}]", sequenceNumber, channel.localAddress(), channel.remoteAddress());
    }
}
