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

package org.apache.nifi.record.path.util;

import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;

public class Filters {

    public static Predicate<FieldValue> fieldTypeFilter(final RecordFieldType fieldType, final RecordFieldType... alternativeTypes) {
        return fieldVal -> {
            final RecordFieldType recordFieldType = fieldVal.getField().getDataType().getFieldType();
            if (recordFieldType == fieldType) {
                return true;
            }

            for (final RecordFieldType alternate : alternativeTypes) {
                if (recordFieldType == alternate) {
                    return true;
                }
            }

            return false;
        };
    }

    public static <T> Stream<T> presentValues(final Stream<Optional<T>> stream) {
        return stream.filter(opt -> opt.isPresent())
            .map(opt -> opt.get());
    }

    public static boolean isRecord(final FieldValue fieldValue) {
        final DataType dataType = fieldValue.getField().getDataType();
        final Object value = fieldValue.getValue();
        return isRecord(dataType, value);
    }

    public static boolean isRecord(final DataType dataType, final Object value) {
        if (dataType.getFieldType() == RecordFieldType.RECORD) {
            return true;
        }

        if (value == null) {
            return false;
        }

        if (value instanceof Record) {
            return true;
        }

        return false;
    }
}
