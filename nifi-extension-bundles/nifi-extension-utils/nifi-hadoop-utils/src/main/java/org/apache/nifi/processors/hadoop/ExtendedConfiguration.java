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
package org.apache.nifi.processors.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.function.BiConsumer;

/**
 * Extending Hadoop Configuration to prevent it from caching classes that can't be found. Since users may be
 * adding additional JARs to the classpath we don't want them to have to restart the JVM to be able to load
 * something that was previously not found, but might now be available.
 *
 * Reference the original getClassByNameOrNull from Configuration.
 */
public class ExtendedConfiguration extends Configuration {

    private final BiConsumer<String, Throwable> loggerMethod;
    private final Map<ClassLoader, Map<String, WeakReference<Class<?>>>> CACHE_CLASSES = new WeakHashMap<>();

    public ExtendedConfiguration(final Logger logger) {
        this.loggerMethod = logger::error;
    }

    public ExtendedConfiguration(final ComponentLog logger) {
        this.loggerMethod = logger::error;
    }

    @Override
    public Class<?> getClassByNameOrNull(String name) {
        final ClassLoader classLoader = getClassLoader();

        Map<String, WeakReference<Class<?>>> map;
        synchronized (CACHE_CLASSES) {
            map = CACHE_CLASSES.get(classLoader);
            if (map == null) {
                map = Collections.synchronizedMap(new WeakHashMap<>());
                CACHE_CLASSES.put(classLoader, map);
            }
        }

        Class<?> clazz = null;
        WeakReference<Class<?>> ref = map.get(name);
        if (ref != null) {
            clazz = ref.get();
        }

        if (clazz == null) {
            try {
                clazz = Class.forName(name, true, classLoader);
            } catch (ClassNotFoundException e) {
                loggerMethod.accept(e.getMessage(), e);
                return null;
            }
            // two putters can race here, but they'll put the same class
            map.put(name, new WeakReference<>(clazz));
            return clazz;
        } else {
            // cache hit
            return clazz;
        }
    }

}
