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
package org.apache.nifi.jms.processors.helpers;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.MessageProducer;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link Session} invocation handler for Session proxy instances.
 * @see ConnectionFactoryInvocationHandler
 */
final class SessionInvocationHandler implements InvocationHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionInvocationHandler.class);

    private final AtomicInteger closeCalled = new AtomicInteger();
    private final List<MessageProducerInvocationHandler> handlers = new CopyOnWriteArrayList<>();
    private final AtomicInteger openedProducers = new AtomicInteger();
    private final Session session;

    public SessionInvocationHandler(Session session) {
        this.session = Objects.requireNonNull(session);
    }

    public Session getSession() {
        return session;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final Object o = session.getClass().getMethod(method.getName(), method.getParameterTypes()).invoke(session, args);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Method {} called on {}", method.getName(), session);
        }
        if (method.getName().equals("createProducer")) {
            MessageProducer messageProducer = (MessageProducer) o;
            LOGGER.info("Created a Message Producer {} using session {}", messageProducer, session);
            openedProducers.incrementAndGet();
            MessageProducerInvocationHandler mp = new MessageProducerInvocationHandler(messageProducer);
            handlers.add(mp);
            MessageProducer messageProducerProxy = (MessageProducer) Proxy.newProxyInstance(o.getClass().getClassLoader(), new Class[] { MessageProducer.class }, mp);
            return messageProducerProxy;
        }
        if ("close".equals(method.getName())) {
            closeCalled.incrementAndGet();
            LOGGER.info("Session close method called {} times for {}", closeCalled, session);
        }
        return o;
    }

    public boolean isClosed() {
        boolean closed = closeCalled.get() >= 1;
        for (MessageProducerInvocationHandler handler : handlers) {
            boolean handlerClosed = handler.isClosed();
            closed = closed && handlerClosed;
            if (!handlerClosed) {
                LOGGER.warn("MessageProducer is not closed {}", handler.getMessageProducer());
            }
        }
        return closed;
    }

    public int openedProducers() {
        return openedProducers.get();
    }

}
