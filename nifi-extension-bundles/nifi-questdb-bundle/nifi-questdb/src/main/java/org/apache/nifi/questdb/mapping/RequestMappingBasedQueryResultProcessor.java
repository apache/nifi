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
package org.apache.nifi.questdb.mapping;

import org.apache.nifi.questdb.QueryResultProcessor;
import org.apache.nifi.questdb.QueryRowContext;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

final class RequestMappingBasedQueryResultProcessor<T> implements QueryResultProcessor<List<T>> {
    private final RequestMapping<T> mapping;
    private final List<T> result = new ArrayList<>();

    RequestMappingBasedQueryResultProcessor(final RequestMapping<T> mapping) {
        this.mapping = mapping;
    }

    @Override
    public void processRow(final QueryRowContext context) {
        final T entry = mapping.getNewInstance();

        for (int position = 0; position < mapping.getNumberOfFields(); position++) {
            if (!ENTRY_FILLERS.containsKey(mapping.getFieldType(position))) {
                throw new IllegalArgumentException(String.format("Unknown field type \"%s\"", mapping.getFieldType(position)));
            }

            final BiFunction<Integer, QueryRowContext, ?> integerRecordBiFunction = ENTRY_FILLERS.get(mapping.getFieldType(position));
            mapping.getMapping(position).accept(entry, integerRecordBiFunction.apply(position, context));
        }

        result.add(entry);
    }

    @Override
    public List<T> getResult() {
        return result;
    }
}
