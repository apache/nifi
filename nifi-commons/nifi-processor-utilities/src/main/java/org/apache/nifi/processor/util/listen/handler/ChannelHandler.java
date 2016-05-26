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
package org.apache.nifi.processor.util.listen.handler;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.listen.dispatcher.ChannelDispatcher;
import org.apache.nifi.processor.util.listen.event.Event;
import org.apache.nifi.processor.util.listen.event.EventFactory;
import org.apache.nifi.processor.util.listen.event.EventQueue;

import java.nio.channels.SelectionKey;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * Base class for all channel handlers.
 */
public abstract class ChannelHandler<E extends Event, D extends ChannelDispatcher> implements Runnable {

    protected final SelectionKey key;
    protected final D dispatcher;
    protected final Charset charset;
    protected final EventFactory<E> eventFactory;
    protected final EventQueue<E> events;
    protected final ComponentLog logger;

    public ChannelHandler(final SelectionKey key,
                          final D dispatcher,
                          final Charset charset,
                          final EventFactory<E> eventFactory,
                          final BlockingQueue<E> events,
                          final ComponentLog logger) {
        this.key = key;
        this.dispatcher = dispatcher;
        this.charset = charset;
        this.eventFactory = eventFactory;
        this.logger = logger;
        this.events = new EventQueue<E>(events, logger);
    }

}
