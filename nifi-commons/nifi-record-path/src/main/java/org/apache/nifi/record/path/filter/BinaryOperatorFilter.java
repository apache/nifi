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

package org.apache.nifi.record.path.filter;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.paths.RecordPathSegment;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.Optional;
import java.util.stream.Stream;

public abstract class BinaryOperatorFilter implements RecordPathFilter {
    private final RecordPathSegment lhs;
    private final RecordPathSegment rhs;

    public BinaryOperatorFilter(final RecordPathSegment lhs, final RecordPathSegment rhs) {
        this.lhs = lhs;
        this.rhs = rhs;
    }

    @Override
    public Stream<FieldValue> filter(final RecordPathEvaluationContext context, final boolean invert) {
        final Stream<FieldValue> rhsStream = rhs.evaluate(context);
        final Optional<FieldValue> firstMatch = rhsStream
            .filter(fieldVal -> fieldVal.getValue() != null)
            .findFirst();

        if (!firstMatch.isPresent()) {
            return Stream.empty();
        }

        final FieldValue fieldValue = firstMatch.get();
        final Object value = fieldValue.getValue();

        final Stream<FieldValue> lhsStream = lhs.evaluate(context);
        return lhsStream.filter(fieldVal -> {
            final boolean result = test(fieldVal, value);
            return invert ? !result : result;
        });
    }

    @Override
    public Stream<FieldValue> mapToBoolean(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> rhsStream = rhs.evaluate(context);
        final Optional<FieldValue> firstMatch = rhsStream
            .filter(fieldVal -> fieldVal.getValue() != null)
            .findFirst();

        if (!firstMatch.isPresent()) {
            return Stream.empty();
        }

        final FieldValue fieldValue = firstMatch.get();
        final Object value = fieldValue.getValue();

        final Stream<FieldValue> lhsStream = lhs.evaluate(context);
        return lhsStream.map(fieldVal -> {
            final boolean result = test(fieldVal, value);
            final FieldValue mapped = new StandardFieldValue(result, new RecordField(getOperator(), RecordFieldType.BOOLEAN.getDataType()), null);
            return mapped;
        });
    }

    @Override
    public String toString() {
        return lhs + " " + getOperator() + " " + rhs;
    }

    protected abstract String getOperator();

    protected abstract boolean test(FieldValue fieldValue, Object rhsValue);
}
