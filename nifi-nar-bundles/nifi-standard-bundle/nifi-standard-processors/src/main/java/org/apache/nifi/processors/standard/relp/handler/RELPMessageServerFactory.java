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
package org.apache.nifi.processors.standard.relp.handler;

import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.NettyEventServerFactory;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.standard.relp.event.RELPMessage;
import org.apache.nifi.processors.standard.relp.frame.RELPFrameDecoder;
import org.apache.nifi.processors.standard.relp.frame.RELPMessageChannelHandler;

import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Netty Event Server Factory implementation for RELP Messages
 */
public class RELPMessageServerFactory extends NettyEventServerFactory {

    /**
     * RELP Message Server Factory to receive RELP messages
     * @param log Component Log
     * @param address Server Address
     * @param port Server Port Number
     * @param charset Charset to use when decoding RELP messages
     * @param events Blocking Queue for events received
     */
    public RELPMessageServerFactory(final ComponentLog log,
                                    final InetAddress address,
                                    final int port,
                                    final Charset charset,
                                    final BlockingQueue<RELPMessage> events) {
        super(address, port, TransportProtocol.TCP);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);
        final RELPMessageChannelHandler relpChannelHandler = new RELPMessageChannelHandler(events, charset);

        setHandlerSupplier(() -> Arrays.asList(
                new RELPFrameDecoder(log, charset),
                relpChannelHandler,
                logExceptionChannelHandler
        ));
    }
}
