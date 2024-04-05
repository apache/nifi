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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;

public interface QueryResultProcessor<R> {
    Map<Class<?>, BiFunction<Integer, QueryRowContext, ?>> ENTRY_FILLERS = new HashMap<>() {{
        put(Integer.class, (p, r) -> r.getInt(p));
        put(Long.class, (p, r) -> r.getLong(p));
        put(String.class, (p, r) -> r.getString(p));
        put(Instant.class, (p, r) -> microsToInstant(r.getTimestamp(p)));
    }};

    private static Instant microsToInstant(final long micros) {
        return Instant.EPOCH.plus(micros, ChronoUnit.MICROS);
    }

    void processRow(QueryRowContext context);

    R getResult();
}
