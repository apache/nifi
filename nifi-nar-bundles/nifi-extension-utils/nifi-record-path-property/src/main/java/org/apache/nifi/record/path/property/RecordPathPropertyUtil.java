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
package org.apache.nifi.record.path.property;

import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.stream.Collectors.toList;

public final class RecordPathPropertyUtil {

    private static final String NULL_VALUE = "null";
    private static final Pattern RECORD_PATH_PATTERN = Pattern.compile("@\\{(/.*?)\\}");

    private RecordPathPropertyUtil() {
    }

    /**
     * Resolves the property value to be handled as a record path or a constant value.
     *
     * @param propertyValue  property value to be resolved
     * @param record record to resolve the value from if the property value is a record path
     */
    public static String resolvePropertyValue(String propertyValue, Record record) {
        if (propertyValue != null && !propertyValue.isBlank()) {
            final Matcher matcher = RECORD_PATH_PATTERN.matcher(propertyValue);
            if (matcher.matches()) {
                return resolveRecordState(matcher.group(1), record);
            } else {
                return propertyValue;
            }
        }

        return null;
    }

    private static String resolveRecordState(String pathValue, final Record record) {
        final RecordPath recordPath = RecordPath.compile(pathValue);
        final RecordPathResult result = recordPath.evaluate(record);
        final List<FieldValue> fieldValues = result.getSelectedFields().collect(toList());
        final FieldValue fieldValue = getMatchingFieldValue(recordPath, fieldValues);

        if (fieldValue.getValue() == null || fieldValue.getValue() == NULL_VALUE) {
            return null;
        }

        return getFieldValue(recordPath, fieldValue);
    }

    /**
     * The method checks if only one result were received for the give record path.
     *
     * @param recordPath path to the requested field
     * @param resultList result list
     * @return matching field
     */
    private static FieldValue getMatchingFieldValue(final RecordPath recordPath, final List<FieldValue> resultList) {
        if (resultList.isEmpty()) {
            throw new ProcessException(String.format("Evaluated RecordPath [%s] against Record but got no results", recordPath));
        }

        if (resultList.size() > 1) {
            throw new ProcessException(String.format("Evaluated RecordPath [%s] against Record and received multiple distinct results [%s]", recordPath, resultList));
        }

        return resultList.get(0);
    }

    /**
     * The method checks the field's type and filters out every non-compatible type.
     *
     * @param recordPath path to the requested field
     * @param fieldValue record field
     * @return value of the record field
     */
    private static String getFieldValue(final RecordPath recordPath, FieldValue fieldValue) {
        final RecordFieldType fieldType = fieldValue.getField().getDataType().getFieldType();

        if (fieldType == RecordFieldType.RECORD || fieldType == RecordFieldType.ARRAY || fieldType == RecordFieldType.MAP) {
            throw new ProcessException(String.format("The provided RecordPath [%s] points to a [%s] type value", recordPath, fieldType));
        }

        if (fieldType == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) fieldValue.getField().getDataType();
            final List<DataType> possibleTypes = choiceDataType.getPossibleSubTypes();
            if (possibleTypes.stream().anyMatch(type -> type.getFieldType() == RecordFieldType.RECORD)) {
                throw new ProcessException(String.format("The provided RecordPath [%s] points to a [CHOICE] type value with Record subtype", recordPath));
            }
        }

        return String.valueOf(fieldValue.getValue());
    }
}
