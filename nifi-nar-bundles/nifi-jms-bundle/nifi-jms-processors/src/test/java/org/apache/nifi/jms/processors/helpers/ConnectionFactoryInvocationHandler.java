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
import javax.jms.ConnectionFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ConnectionFactory}'s invocation handler to be used to create {@link Proxy} instances. This handler stores
 * useful information to validate the proper resources handling of underlying connection factory.
 */
@ThreadSafe
public final class ConnectionFactoryInvocationHandler implements InvocationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionFactoryInvocationHandler.class);

    private final ConnectionFactory connectionFactory;
    private final List<ConnectionInvocationHandler> handlers = new CopyOnWriteArrayList<>();
    private final AtomicInteger openedConnections = new AtomicInteger();

    public ConnectionFactoryInvocationHandler(ConnectionFactory connectionFactory) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final Object o = connectionFactory.getClass().getMethod(method.getName(), method.getParameterTypes()).invoke(connectionFactory, args);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Method {} called on {}", method.getName(), connectionFactory);
        }
        if ("createConnection".equals(method.getName())) {
            Connection connection = (Connection) o;
            ConnectionInvocationHandler cp = new ConnectionInvocationHandler(connection);
            handlers.add(cp);
            openedConnections.incrementAndGet();
            LOGGER.info("Connection created {}", connection);
            return (Connection) Proxy.newProxyInstance(o.getClass().getClassLoader(), new Class[] { Connection.class }, cp);
        }
        return o;
    }

    /**
     * @return true if all opened resources were closed.
     */
    public boolean isAllResourcesClosed() {
        boolean closed = true;
        for (ConnectionInvocationHandler handler : handlers) {
            boolean handlerClosed = handler.isClosed();
            closed = closed && handlerClosed;
            if (!handlerClosed) {
                LOGGER.warn("Connection is not closed {}", handler.getConnection());
            }
        }
        return closed;
    }

    /**
     * @return number of opened connections.
     */
    public int openedConnections() {
        return openedConnections.get();
    }

    /**
     * @return all opened producers.
     */
    public int openedProducers() {
        int producers = 0;
        for (ConnectionInvocationHandler handler : handlers) {
            producers += handler.openedProducers();
        }
        return producers;
    }

    /**
     * @return number of opened sessions.
     */
    public int openedSessions() {
        int sessions = 0;
        for (ConnectionInvocationHandler handler : handlers) {
            sessions += handler.openedSessions();
        }
        return sessions;
    }

}
