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
package org.apache.nifi.processors.beats.server;

import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.beats.handler.BatchChannelInboundHandler;
import org.apache.nifi.processors.beats.handler.BatchDecoder;
import org.apache.nifi.processors.beats.handler.MessageAckEncoder;
import org.apache.nifi.processors.beats.protocol.BatchMessage;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Beats Message Protocol extends of Netty Event Server Factory
 */
public class BeatsMessageServerFactory extends NettyEventServerFactory {
    /**
     * Beats Message Server Factory constructor with standard configuration arguments
     *
     * @param log Component Log
     * @param address Server Address
     * @param port Server Port Number
     * @param events Blocking Queue for events received
     */
    public BeatsMessageServerFactory(final ComponentLog log,
                                     final InetAddress address,
                                     final int port,
                                     final BlockingQueue<BatchMessage> events) {
        super(address, port, TransportProtocol.TCP);
        final MessageAckEncoder messageAckEncoder = new MessageAckEncoder(log);
        final BatchChannelInboundHandler batchChannelInboundHandler = new BatchChannelInboundHandler(log, events);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);

        setHandlerSupplier(() -> Arrays.asList(
                messageAckEncoder,
                new BatchDecoder(log),
                batchChannelInboundHandler,
                logExceptionChannelHandler
        ));
    }
}
