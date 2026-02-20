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
package org.apache.nifi.services.iceberg.parquet.io.recordconverters;

import org.apache.iceberg.data.Record;
import org.apache.nifi.services.iceberg.parquet.io.IcebergRecordConverter;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class IcebergParquetRecordWriterConverter implements IcebergRecordConverter {

    private final Map<Class<?>, Function<Object, Object>> typeHandlers = Map.of(
            java.sql.Timestamp.class, (ts) -> ((java.sql.Timestamp) ts).toLocalDateTime(),
            Record.class, (rec) -> {
                convertRecord((Record) rec);
                return rec;
            },
            List.class, (list) -> {
                convertList((List<?>) list);
                return list;
            }
    );

    @Override
    public void convertRecord(Record record) {
        if (record == null) {
            return;
        }

        for (int i = 0; i < record.struct().fields().size(); i++) {
            Object value = record.get(i);
            if (value == null) {
                continue;
            }

            Function<Object, Object> handler = findHandler(value.getClass());
            if (handler != null) {
                record.set(i, handler.apply(value));
            }
        }
    }

    public void convertList(List<?> list) {
        if (list == null) {
            return;
        }

        for (Object element : list) {
            if (element == null) {
                continue;
            }

            Function<Object, Object> handler = findHandler(element.getClass());
            if (handler != null) {
                handler.apply(element);
            }
        }
    }

    private Function<Object, Object> findHandler(Class<?> clazz) {
        if (typeHandlers.containsKey(clazz)) {
            return typeHandlers.get(clazz);
        }

        return typeHandlers.entrySet().stream()
                .filter(entry -> entry.getKey().isAssignableFrom(clazz))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
    }
}
