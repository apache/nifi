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

import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public class Substring extends RecordPathSegment {
    private final RecordPathSegment recordPath;
    private final RecordPathSegment startIndexPath;
    private final RecordPathSegment endIndexPath;

    public Substring(final RecordPathSegment recordPath, final RecordPathSegment startIndex, final RecordPathSegment endIndex, final boolean absolute) {
        super("substring", null, absolute);

        this.recordPath = recordPath;
        this.startIndexPath = startIndex;
        this.endIndexPath = endIndex;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> fieldValues = recordPath.evaluate(context);
        return fieldValues.filter(fv -> fv.getValue() != null)
            .map(fv -> {
                final OptionalInt startIndex = getIndex(startIndexPath, context);
                if (!startIndex.isPresent()) {
                    return new StandardFieldValue("", fv.getField(), fv.getParent().orElse(null));
                }

                final OptionalInt endIndex = getIndex(endIndexPath, context);
                if (!endIndex.isPresent()) {
                    return new StandardFieldValue("", fv.getField(), fv.getParent().orElse(null));
                }

                final int start = startIndex.getAsInt();
                final int end = endIndex.getAsInt();

                final String value = DataTypeUtils.toString(fv.getValue(), (String) null);

                // Allow for negative indices to be used to reference offset from string length. We add 1 here because we want -1 to refer
                // to the actual length of the string.
                final int evaluatedEndIndex = end < 0 ? value.length() + 1 + end : end;
                final int evaluatedStartIndex = start < 0 ? value.length() + 1 + start : start;

                if (evaluatedEndIndex <= evaluatedStartIndex || evaluatedStartIndex < 0 || evaluatedStartIndex > value.length()) {
                    return new StandardFieldValue("", fv.getField(), fv.getParent().orElse(null));
                }

                final String substring = value.substring(evaluatedStartIndex, Math.min(evaluatedEndIndex, value.length()));
                return new StandardFieldValue(substring, fv.getField(), fv.getParent().orElse(null));
            });
    }

    private OptionalInt getIndex(final RecordPathSegment indexSegment, final RecordPathEvaluationContext context) {
        final Optional<FieldValue> firstFieldValueOption = indexSegment.evaluate(context).findFirst();
        if (!firstFieldValueOption.isPresent()) {
            return OptionalInt.empty();
        }

        final FieldValue fieldValue = firstFieldValueOption.get();
        final Object indexObject = fieldValue.getValue();
        if (!DataTypeUtils.isIntegerTypeCompatible(indexObject)) {
            return OptionalInt.empty();
        }

        final String fieldName;
        final RecordField field = fieldValue.getField();
        fieldName = field == null ? "<Unknown Field>" : field.getFieldName();

        return OptionalInt.of(DataTypeUtils.toInteger(indexObject, fieldName));
    }
}
