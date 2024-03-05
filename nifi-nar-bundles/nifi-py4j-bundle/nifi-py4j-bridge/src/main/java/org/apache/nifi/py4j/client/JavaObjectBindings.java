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

package org.apache.nifi.py4j.client;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py4j.JVMView;
import py4j.Protocol;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class JavaObjectBindings {
    private static final Logger logger = LoggerFactory.getLogger(JavaObjectBindings.class);

    private final AtomicLong idGenerator = new AtomicLong(0L);
    private final Map<String, Object> bindings = new HashMap<>();
    private final Map<String, Integer> bindingCounts = new HashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final Lock writeLock = rwLock.writeLock();


    public JavaObjectBindings() {
        bind(Protocol.DEFAULT_JVM_OBJECT_ID, new JVMView("default", Protocol.DEFAULT_JVM_OBJECT_ID), 1);
    }

    public String bind(final Object object, final int count) {
        final String id = "o" + idGenerator.getAndIncrement();
        return bind(id, object, count);
    }

    public String bind(final String objectId, final Object object, final int count) {
        if (count == 0) {
            logger.debug("Will not bind {} to ID {} because count is 0", object, objectId);
            return objectId;
        }

        writeLock.lock();
        try {
            bindings.put(objectId, object);
            bindingCounts.put(objectId, count);
        } finally {
            writeLock.unlock();
        }

        logger.debug("Bound {} to ID {} with count {}", object, objectId, count);
        return objectId;
    }

    public Object getBoundObject(final String objectId) {
        readLock.lock();
        try {
            return bindings.get(objectId);
        } finally {
            readLock.unlock();
        }
    }

    public Object unbind(final String objectId) {
        writeLock.lock();
        try {
            final Integer currentValue = bindingCounts.remove(objectId);
            final int updatedValue = (currentValue == null) ? 0 : currentValue - 1;
            if (updatedValue < 1) {
                final Object unbound = bindings.remove(objectId);
                logger.debug("Unbound {} from ID {}", unbound, objectId);
                return unbound;
            }

            bindingCounts.put(objectId, updatedValue);
            logger.debug("Decremented binding count for ID {} to {}", objectId, updatedValue);
            return bindings.get(objectId);
        } finally {
            writeLock.unlock();
        }
    }


    public Map<String, Integer> getCountsPerClass() {
        final Map<String, Integer> counts = new HashMap<>();

        readLock.lock();
        try {
            bindings.values().forEach(object -> {
                final String className = (object == null) ? "<null>" : object.getClass().getName();
                counts.merge(className, 1, Integer::sum);
            });
        } finally {
            readLock.unlock();
        }

        return counts;
    }
}
