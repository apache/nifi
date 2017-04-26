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

package org.apache.nifi.record.path.paths;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.nifi.record.path.ArrayIndexFieldValue;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.NumericRange;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.util.Filters;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ArrayDataType;

public class MultiArrayIndexPath extends RecordPathSegment {
    private final List<NumericRange> indices;

    MultiArrayIndexPath(final List<NumericRange> indices, final RecordPathSegment parent, final boolean absolute) {
        super(indices.toString(), parent, absolute);
        this.indices = indices;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> parentResult = getParentPath().evaluate(context);

        return parentResult
            .filter(Filters.fieldTypeFilter(RecordFieldType.ARRAY))
            .flatMap(fieldValue -> {
                final ArrayDataType arrayDataType = (ArrayDataType) fieldValue.getField().getDataType();
                final DataType elementDataType = arrayDataType.getElementType();
                final RecordField arrayField = new RecordField(fieldValue.getField().getFieldName(), elementDataType);

                final Object[] values = (Object[]) fieldValue.getValue();

                return indices.stream()
                    .filter(range -> values.length > Math.abs(range.getMin()))
                    .flatMap(range -> {
                        final List<Object> valuesWithinRange = new ArrayList<>();

                        final int min = range.getMin() < 0 ? values.length + range.getMin() : range.getMin();
                        final int max = range.getMax() < 0 ? values.length + range.getMax() : range.getMax();

                        for (int i = min; i <= max; i++) {
                            if (values.length > i) {
                                valuesWithinRange.add(values[i]);
                            }
                        }

                        return IntStream.range(0, valuesWithinRange.size())
                            .mapToObj(index -> {
                                final RecordField elementField = new RecordField(arrayField.getFieldName() + "[" + index + "]", elementDataType);
                                return new ArrayIndexFieldValue(valuesWithinRange.get(index), elementField, fieldValue, index);
                            });
                    });

            });
    }
}
