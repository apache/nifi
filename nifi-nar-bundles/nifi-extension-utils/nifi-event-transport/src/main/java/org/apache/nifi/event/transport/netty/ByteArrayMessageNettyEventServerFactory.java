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
package org.apache.nifi.event.transport.netty;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.channel.ByteArrayMessageChannelHandler;
import org.apache.nifi.event.transport.netty.codec.DatagramByteArrayMessageDecoder;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.event.transport.netty.codec.SocketByteArrayMessageDecoder;
import org.apache.nifi.logging.ComponentLog;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Netty Event Server Factory for Byte Array Messages
 */
public class ByteArrayMessageNettyEventServerFactory extends NettyEventServerFactory {
    private static final boolean STRIP_DELIMITER = true;

    /**
     * Netty Event Server Factory with configurable delimiter and queue of Byte Array Messages
     *
     * @param log Component Log
     * @param address Remote Address
     * @param port Remote Port Number
     * @param protocol Channel Protocol
     * @param delimiter Message Delimiter
     * @param maxFrameLength Maximum Frame Length for delimited TCP messages
     * @param messages Blocking Queue for events received
     */
    public ByteArrayMessageNettyEventServerFactory(final ComponentLog log,
                                                   final String address,
                                                   final int port,
                                                   final TransportProtocol protocol,
                                                   final byte[] delimiter,
                                                   final int maxFrameLength,
                                                   final BlockingQueue<ByteArrayMessage> messages) {
        super(address, port, protocol);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);
        final ByteArrayMessageChannelHandler byteArrayMessageChannelHandler = new ByteArrayMessageChannelHandler(messages);

        if (TransportProtocol.UDP.equals(protocol)) {
            setHandlerSupplier(() -> Arrays.asList(
                    logExceptionChannelHandler,
                    new DatagramByteArrayMessageDecoder(),
                    byteArrayMessageChannelHandler
            ));
        } else {
            setHandlerSupplier(() -> Arrays.asList(
                    logExceptionChannelHandler,
                    new DelimiterBasedFrameDecoder(maxFrameLength, STRIP_DELIMITER, Unpooled.wrappedBuffer(delimiter)),
                    new ByteArrayDecoder(),
                    new SocketByteArrayMessageDecoder(),
                    byteArrayMessageChannelHandler
            ));
        }
    }
}
