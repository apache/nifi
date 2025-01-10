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

package org.apache.nifi.record.path.functions;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class ArrayOf extends RecordPathSegment {
    private final RecordPathSegment[] elementPaths;

    public ArrayOf(final RecordPathSegment[] elementPaths, final boolean absolute) {
        super("arrayOf", null, absolute);
        this.elementPaths = elementPaths;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final List<Object> values = new ArrayList<>();
        final List<FieldValue> fieldValues = new ArrayList<>();

        for (final RecordPathSegment elementPath : elementPaths) {
            final Stream<FieldValue> stream = elementPath.evaluate(context);
            stream.forEach(fv -> {
                fieldValues.add(fv);
                values.add(fv.getValue());
            });
        }

        if (fieldValues.isEmpty()) {
            return Stream.of();
        }

        DataType merged = null;
        for (final FieldValue fieldValue : fieldValues) {
            final DataType dataType = getDataType(fieldValue);
            if (merged == null) {
                merged = dataType;
                continue;
            }

            merged = DataTypeUtils.mergeDataTypes(merged, dataType);
        }

        final Object[] array = values.toArray();
        final RecordField field = new RecordField("arrayOf", merged);
        final FieldValue fieldValue = new StandardFieldValue(array, field, null);
        return Stream.of(fieldValue);
    }

    private DataType getDataType(final FieldValue fieldValue) {
        final RecordField recordField = fieldValue.getField();
        if (recordField != null) {
            return recordField.getDataType();
        } else {
            return DataTypeUtils.inferDataType(fieldValue.getValue(), RecordFieldType.STRING.getDataType());
        }
    }
}
