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

import io.netty.handler.timeout.ReadTimeoutHandler;
import org.apache.nifi.event.transport.configuration.TransportProtocol;
import org.apache.nifi.event.transport.netty.channel.LogExceptionChannelHandler;
import org.apache.nifi.event.transport.netty.channel.RecordReaderSession;
import org.apache.nifi.event.transport.netty.channel.RecordReaderHandler;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The Record Reader Event Server for reading in records from a Netty server/socket
 */
public class RecordReaderEventServerFactory extends NettyEventServerFactory {

    /**
     * Netty Event Server Factory which allows building a netty server that can read generate populated record readers
     *
     * @param log Component Log
     * @param address Listen Address
     * @param port Listen Port Number
     * @param protocol Channel Protocol
     */
    public RecordReaderEventServerFactory(final ComponentLog log,
                                          final InetAddress address,
                                          final int port,
                                          final TransportProtocol protocol,
                                          final RecordReaderFactory readerFactory,
                                          final BlockingQueue<RecordReaderSession> senderStreams,
                                          final Duration readTimeout) {
        super(address, port, protocol);
        final LogExceptionChannelHandler logExceptionChannelHandler = new LogExceptionChannelHandler(log);

        setHandlerSupplier(() -> Arrays.asList(
                new ReadTimeoutHandler(readTimeout.getSeconds(), TimeUnit.SECONDS),
                new RecordReaderHandler(readerFactory, senderStreams, log),
                logExceptionChannelHandler
        ));
    }
}
