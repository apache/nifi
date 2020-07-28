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

import javax.annotation.concurrent.ThreadSafe;
import javax.jms.Connection;
import javax.jms.Session;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple {@link Connection} proxy utility to detect opened and unclosed resources.
 */
@ThreadSafe
final class ConnectionInvocationHandler implements InvocationHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionInvocationHandler.class);
    private final AtomicInteger closeCalled = new AtomicInteger();
    private final Connection connection;
    private final List<SessionInvocationHandler> handlers = new CopyOnWriteArrayList<>();
    private final AtomicInteger openedSessions = new AtomicInteger();

    public ConnectionInvocationHandler(Connection connection) {
        this.connection = Objects.requireNonNull(connection);
    }

    public Connection getConnection() {
        return connection;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final Object o = connection.getClass().getMethod(method.getName(), method.getParameterTypes()).invoke(connection, args);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Method {} called on {}", method.getName(), connection);
        }
        if (method.getName().equals("createSession")) {
            Session session = (Session) o;
            LOGGER.info("Session created {} using connection {}", session, connection);
            openedSessions.incrementAndGet();
            SessionInvocationHandler sp = new SessionInvocationHandler(session);
            handlers.add(sp);
            Session sessionProxy = (Session) Proxy.newProxyInstance(o.getClass().getClassLoader(), new Class[] { Session.class }, sp);
            return sessionProxy;
        }
        if ("close".equals(method.getName())) {
            closeCalled.incrementAndGet();
            LOGGER.info("Connection close method called {} times for {}", closeCalled, connection);
        }
        return o;
    }

    /**
     * @return true if {@link Connection#close()} method was closed to {@link #connection} and all resources created from the connection were closed too.
     */
    public boolean isClosed() {
        boolean closed = closeCalled.get() >= 1;
        for (SessionInvocationHandler handler : handlers) {
            boolean handlerClosed = handler.isClosed();
            closed = closed && handlerClosed;
            if (!handlerClosed) {
                LOGGER.warn("Session is not closed {}", handler.getSession());
            }
        }
        return closed;
    }

    /**
     * @return number opened producers.
     */
    public int openedProducers() {
        int producers = 0;
        for (SessionInvocationHandler handler : handlers) {
            producers += handler.openedProducers();
        }
        return producers;
    }

    /**
     * @return the number of opened sessions.
     */
    public int openedSessions() {
        return openedSessions.get();
    }

}
