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
package org.apache.nifi.questdb;

import org.apache.commons.lang3.function.TriConsumer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

abstract class FillerBasedInsertRowDataSource implements InsertRowDataSource {
    private final static Map<Class<?>, TriConsumer<Integer, Object, InsertRowContext>> FILLERS = new HashMap<>() {{
        put(Integer.class, (p, o, r) -> r.addInt(p, (Integer) o));
        put(Long.class, (p, o, r) -> r.addLong(p, (Long) o));
        put(String.class, (p, o, r) -> r.addString(p, (String) o));
        put(Instant.class, (p, o, r) -> r.addInstant(p, (Instant) o));
    }};

    protected TriConsumer<Integer, Object, InsertRowContext> getFillerForFieldType(final Class<?> type) {
        if (!FILLERS.containsKey(type)) {
            throw new IllegalArgumentException(String.format("Unknown field type \"{}\"", type));
        }

        return FILLERS.get(type);
    }
}
