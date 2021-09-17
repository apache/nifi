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
import org.apache.nifi.processors.standard.relp.event.RELPNettyEvent;
import org.apache.nifi.processors.standard.relp.frame.RELPFrameDecoder;
import org.apache.nifi.processors.standard.relp.frame.RELPNettyEventChannelHandler;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

/**
 * Netty Event Server Factory for Byte Array Messages
 */
public class RELPNettyEventServerFactory extends NettyEventServerFactory {

    /**
     * Netty Event Server Factory with configurable delimiter and queue of Byte Array Messages
     *  @param log Component Log
     * @param address Remote Address
     * @param port Remote Port Number
     * @param events Blocking Queue for events received
     */
    public RELPNettyEventServerFactory(final ComponentLog log,
                                       final String address,
                                       final int port,
                                       final Charset charset,
                                       final BlockingQueue<RELPNettyEvent> events) {
        super(address, port, TransportProtocol.TCP);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);
        final RELPNettyEventChannelHandler relpChannelHandler = new RELPNettyEventChannelHandler(events);

        setHandlerSupplier(() -> Arrays.asList(
                logExceptionChannelHandler,
                new RELPFrameDecoder(log, charset),
                relpChannelHandler
        ));
    }
}
