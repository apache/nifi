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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class JavaObjectBindings {
    private static final Logger logger = LoggerFactory.getLogger(JavaObjectBindings.class);

    private final AtomicLong idGenerator = new AtomicLong(0L);
    private final Map<String, Object> bindings = new ConcurrentHashMap<>();


    public JavaObjectBindings() {
        bind(Protocol.DEFAULT_JVM_OBJECT_ID, new JVMView("default", Protocol.DEFAULT_JVM_OBJECT_ID));
    }

    public String bind(final Object object) {
        final String id = "o" + idGenerator.getAndIncrement();
        return bind(id, object);
    }

    public String bind(final String objectId, final Object object) {
        bindings.put(objectId, object);

        logger.debug("Bound {} to ID {}", object, objectId);
        return objectId;
    }

    public Object getBoundObject(final String objectId) {
        return bindings.get(objectId);
    }

    public Object unbind(final String objectId) {
        final Object unbound = bindings.remove(objectId);
        logger.debug("Unbound {} from ID {}", unbound, objectId);

        return unbound;
    }

    public Map<String, Integer> getCountsPerClass() {
        final Map<String, Integer> counts = new HashMap<>();
        bindings.values().forEach(object -> {
            final String className = (object == null) ? "<null>" : object.getClass().getName();
            counts.merge(className, 1, Integer::sum);
        });

        return counts;
    }
}
