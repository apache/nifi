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
package org.apache.nifi.processor.util.listen.handler.socket;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.AsyncChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.handler.ChannelHandler;

import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * Base class for socket channel handlers.
 */
public abstract class SocketChannelHandler<E extends Event<SocketChannel>> extends ChannelHandler<E, AsyncChannelDispatcher> {

    static final byte TCP_DELIMITER = '\n';

    public SocketChannelHandler(final SelectionKey key,
                                final AsyncChannelDispatcher dispatcher,
                                final Charset charset,
                                final EventFactory<E> eventFactory,
                                final BlockingQueue<E> events,
                                final ComponentLog logger) {
        super(key, dispatcher, charset, eventFactory, events, logger);
    }

    /**
     * @return the byte used as the delimiter between messages for the given handler
     */
    public abstract byte getDelimiter();

}
