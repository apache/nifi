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
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.MessageProducer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link MessageProducer} invocation handler.
 */
final class MessageProducerInvocationHandler implements InvocationHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(SessionInvocationHandler.class);

    private final AtomicInteger closeCalled = new AtomicInteger();
    private final MessageProducer messageProducer;

    public MessageProducerInvocationHandler(MessageProducer messageProducer) {
        this.messageProducer = Objects.requireNonNull(messageProducer);
    }

    public MessageProducer getMessageProducer() {
        return messageProducer;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        final Object o = messageProducer.getClass().getMethod(method.getName(), method.getParameterTypes()).invoke(messageProducer, args);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Method {} called on {}", method.getName(), messageProducer);
        }
        if ("close".equals(method.getName())) {
            closeCalled.incrementAndGet();
            LOGGER.info("MessageProducer closed {}", messageProducer);
        }
        return o;
    }

    public boolean isClosed() {
        return closeCalled.get() >= 1;
    }

}
