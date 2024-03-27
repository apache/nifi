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
import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.bytes.ByteArrayDecoder;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.message.ByteArrayMessage;
import org.apache.nifi.event.transport.netty.channel.ByteArrayMessageChannelHandler;
import org.apache.nifi.event.transport.netty.channel.FilteringByteArrayMessageChannelHandler;
import org.apache.nifi.event.transport.netty.codec.DatagramByteArrayMessageDecoder;
import org.apache.nifi.event.transport.netty.codec.OctetCountingFrameDecoder;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.event.transport.netty.codec.SocketByteArrayMessageDecoder;
import org.apache.nifi.logging.ComponentLog;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Netty Event Server Factory for Byte Array Messages
 */
public class ByteArrayMessageNettyEventServerFactory extends NettyEventServerFactory {
    public static final int MAX_LENGTH_FIELD_LENGTH = 9; // max 999_999_999 Bytes (aka 1GB)
    private static final boolean STRIP_DELIMITER = true;

    /**
     * Netty Event Server Factory with configurable delimiter and queue of Byte Array Messages
     *
     * @param log Component Log
     * @param address Listen Address
     * @param port Listen Port Number
     * @param protocol Channel Protocol
     * @param delimiter Message Delimiter
     * @param maxFrameLength Maximum Frame Length for delimited TCP messages
     * @param messages Blocking Queue for events received
     */
    public ByteArrayMessageNettyEventServerFactory(final ComponentLog log,
                                                   final InetAddress address,
                                                   final int port,
                                                   final TransportProtocol protocol,
                                                   final byte[] delimiter,
                                                   final int maxFrameLength,
                                                   final BlockingQueue<ByteArrayMessage> messages) {
        this(log, address, port, protocol, delimiter, maxFrameLength, messages, ParsingStrategy.SPLIT_ON_DELIMITER, FilteringStrategy.DISABLED);
    }


    /**
     * Netty Event Server Factory with configurable delimiter and queue of Byte Array Messages
     *
     * @param log Component Log
     * @param address Listen Address
     * @param port Listen Port Number
     * @param protocol Channel Protocol
     * @param delimiter Message Delimiter
     * @param maxFrameLength Maximum Frame Length for delimited TCP messages
     * @param messages Blocking Queue for events received
     * @param parsingStrategy Message Parsing Strategy
     * @param filteringStrategy Message Filtering Strategy
     */
    public ByteArrayMessageNettyEventServerFactory(final ComponentLog log,
                                                   final InetAddress address,
                                                   final int port,
                                                   final TransportProtocol protocol,
                                                   final byte[] delimiter,
                                                   final int maxFrameLength,
                                                   final BlockingQueue<ByteArrayMessage> messages,
                                                   final ParsingStrategy parsingStrategy,
                                                   final FilteringStrategy filteringStrategy) {
        super(address, port, protocol);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);

        final ByteArrayMessageChannelHandler byteArrayMessageChannelHandler;
        if (FilteringStrategy.EMPTY == filteringStrategy) {
            byteArrayMessageChannelHandler = new FilteringByteArrayMessageChannelHandler(messages);
        } else {
            byteArrayMessageChannelHandler = new ByteArrayMessageChannelHandler(messages);
        }

        if (TransportProtocol.UDP.equals(protocol)) {
            setHandlerSupplier(() -> Arrays.asList(
                    new DatagramByteArrayMessageDecoder(),
                    byteArrayMessageChannelHandler,
                    logExceptionChannelHandler
            ));
        } else {
            setHandlerSupplier(() -> {
                ArrayList<ChannelHandler> handlers = new ArrayList<>(Arrays.asList(
                        new ByteArrayDecoder(),
                        new SocketByteArrayMessageDecoder(),
                        byteArrayMessageChannelHandler,
                        logExceptionChannelHandler
                ));
                switch (parsingStrategy) {
                    case ParsingStrategy.DISABLED:
                        // don't add any handler
                        break;
                    case ParsingStrategy.SPLIT_ON_DELIMITER:
                        handlers.add(0, new DelimiterBasedFrameDecoder(maxFrameLength, STRIP_DELIMITER, Unpooled.wrappedBuffer(delimiter)));
                        break;
                    case ParsingStrategy.OCTET_COUNTING_TOLERANT:
                        handlers.add(0, new OctetCountingFrameDecoder(maxFrameLength, MAX_LENGTH_FIELD_LENGTH, Unpooled.wrappedBuffer(delimiter)));
                        break;
                    case ParsingStrategy.OCTET_COUNTING_STRICT:
                        handlers.add(0, new OctetCountingFrameDecoder(maxFrameLength, MAX_LENGTH_FIELD_LENGTH, null));
                        break;
                }
                return handlers;
            });
        }
    }
}
