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

import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

final class SimpleInsertMapping<T> implements InsertMapping<T> {
    private final Class<T> type;
    private final int timestampPosition;
    private final List<Pair<Class<?>, Function<T, ?>>> fieldMappings;

    SimpleInsertMapping(
        final Class<T> type,
        final int timestampPosition,
        final List<Pair<Class<?>, Function<T, ?>>> fieldMappings
    ) {
        this.type = type;
        this.timestampPosition = timestampPosition;
        this.fieldMappings = new ArrayList<>(fieldMappings);
    }

    @Override
    public Class<T> getType() {
        return type;
    }

    @Override
    public int getNumberOfFields() {
        return fieldMappings.size();
    }

    @Override
    public int getTimestampPosition() {
        return timestampPosition;
    }

    @Override
    public Class<?> getFieldTypeAt(final int position) {
        return fieldMappings.get(position).getKey();
    }

    @Override
    public Function<T, ?> getMappingAt(final int position) {
        return fieldMappings.get(position).getValue();
    }
}
