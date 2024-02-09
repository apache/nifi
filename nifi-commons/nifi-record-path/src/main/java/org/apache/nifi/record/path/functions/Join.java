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
import org.apache.nifi.record.path.util.RecordPathUtils;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class Join extends RecordPathSegment {
    private final RecordPathSegment delimiterPath;
    private final RecordPathSegment[] valuePaths;

    public Join(final RecordPathSegment delimiterPath, final RecordPathSegment[] valuePaths, final boolean absolute) {
        super("join", null, absolute);
        this.delimiterPath = delimiterPath;
        this.valuePaths = valuePaths;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        String delimiter = RecordPathUtils.getFirstStringValue(delimiterPath, context);
        if (delimiter == null) {
            delimiter = "";
        }

        final List<String> values = new ArrayList<>();
        for (final RecordPathSegment valuePath : valuePaths) {
            final Stream<FieldValue> stream = valuePath.evaluate(context);

            stream.forEach(fv -> {
                final Object value = fv.getValue();
                addStringValue(value, values);
            });
        }

        final String joined = String.join(delimiter, values);
        final RecordField field = new RecordField("join", RecordFieldType.STRING.getDataType());
        final FieldValue responseValue = new StandardFieldValue(joined, field, null);
        return Stream.of(responseValue);
    }

    private void addStringValue(final Object value, final List<String> values) {
        if (value == null) {
            values.add("null");
            return;
        }

        if (value instanceof final Object[] array) {
            for (final Object element : array) {
                addStringValue(element, values);
            }
        } else if (value instanceof final Iterable<?> iterable) {
            for (final Object element : iterable) {
                addStringValue(element, values);
            }
        } else {
            values.add(DataTypeUtils.toString(value, null));
        }
    }
}
