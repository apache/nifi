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

package org.apache.nifi.json;

import java.util.Optional;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public class PropertyNameUtil {

    public static String getFieldName(final String propertyName) {
        final int colonIndex = propertyName.indexOf(":");
        if (colonIndex > -1 && colonIndex < propertyName.length() - 1) {
            return propertyName.substring(0, colonIndex);
        }

        return propertyName;
    }

    public static boolean hasFieldType(final String propertyName) {
        final int colonIndex = propertyName.indexOf(":");
        return (colonIndex > -1 && colonIndex < propertyName.length() - 1);
    }

    public static Optional<String> getFieldTypeName(final String propertyName) {
        if (hasFieldType(propertyName)) {
            final String[] splits = propertyName.split("\\:");
            if (splits.length > 1) {
                return Optional.of(splits[1]);
            }
            return Optional.empty();
        }

        return Optional.empty();
    }

    public static Optional<String> getFieldFormat(final String propertyName) {
        final String[] splits = propertyName.split("\\:");
        if (splits.length != 3) {
            return Optional.empty();
        }

        return Optional.of(splits[2]);
    }

    public static boolean isFieldTypeValid(final String propertyName) {
        final Optional<String> fieldType = getFieldTypeName(propertyName);
        if (!fieldType.isPresent()) {
            return false;
        }

        final String typeName = fieldType.get();
        final RecordFieldType recordFieldType = RecordFieldType.of(typeName);
        return recordFieldType != null;
    }

    public static Optional<DataType> getDataType(final String propertyName) {
        if (isFieldTypeValid(propertyName)) {
            final String typeName = getFieldTypeName(propertyName).get();
            final RecordFieldType fieldType = RecordFieldType.of(typeName);

            final Optional<String> format = getFieldFormat(propertyName);
            if (format.isPresent()) {
                return Optional.of(fieldType.getDataType(format.get()));
            } else {
                return Optional.of(fieldType.getDataType());
            }
        }

        return Optional.empty();
    }
}
