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

import java.nio.channels.SelectionKey;
import java.nio.charset.Charset;
import java.util.concurrent.BlockingQueue;

/**
 * Factory that can produce ChannelHandlers for the given type of Event and ChannelDispatcher.
 */
public interface ChannelHandlerFactory<E extends Event, D extends ChannelDispatcher> {

    ChannelHandler<E, D> createHandler(final SelectionKey key,
                                    final D dispatcher,
                                    final Charset charset,
                                    final EventFactory<E> eventFactory,
                                    final BlockingQueue<E> events,
                                    final ComponentLog logger);

    ChannelHandler<E, D> createSSLHandler(final SelectionKey key,
                                       final D dispatcher,
                                       final Charset charset,
                                       final EventFactory<E> eventFactory,
                                       final BlockingQueue<E> events,
                                       final ComponentLog logger);
}
