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

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandler;
import org.apache.nifi.processor.util.listen.handler.ChannelHandlerFactory;

/**
 * Default factory for creating Beats socket channel handlers.
 */
public class BeatsSocketChannelHandlerFactory<E extends Event<SocketChannel>> implements ChannelHandlerFactory<E, AsyncChannelDispatcher> {

    @Override
    public ChannelHandler<E, AsyncChannelDispatcher> createHandler(final SelectionKey key,
                                           final AsyncChannelDispatcher dispatcher,
                                           final Charset charset,
                                           final EventFactory<E> eventFactory,
                                           final BlockingQueue<E> events,
                                           final ComponentLog logger) {
        return new BeatsSocketChannelHandler<>(key, dispatcher, charset, eventFactory, events, logger);
    }

    @Override
    public ChannelHandler<E, AsyncChannelDispatcher> createSSLHandler(final SelectionKey key,
                                              final AsyncChannelDispatcher dispatcher,
                                              final Charset charset,
                                              final EventFactory<E> eventFactory,
                                              final BlockingQueue<E> events,
                                              final ComponentLog logger) {
        return new BeatsSSLSocketChannelHandler<>(key, dispatcher, charset, eventFactory, events, logger);
    }

}
