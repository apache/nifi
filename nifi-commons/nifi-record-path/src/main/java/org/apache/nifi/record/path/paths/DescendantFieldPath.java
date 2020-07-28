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
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.util.Filters;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;

public class DescendantFieldPath extends RecordPathSegment {
    private final String descendantName;

    DescendantFieldPath(final String descendantName, final RecordPathSegment parent, final boolean absolute) {
        super("//" + descendantName, parent, absolute);
        this.descendantName = descendantName;
    }


    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> parentResult = getParentPath().evaluate(context);

        return parentResult
            .flatMap(recordFieldVal -> findDescendants(recordFieldVal).stream());
    }

    private List<FieldValue> findDescendants(final FieldValue fieldValue) {
        if (fieldValue == null || fieldValue.getValue() == null) {
            return Collections.emptyList();
        }
        if (!Filters.isRecord(fieldValue)) {
            return Collections.emptyList();
        }

        final Record record = (Record) fieldValue.getValue();
        final List<FieldValue> matchingValues = new ArrayList<>();

        for (final RecordField childField : record.getSchema().getFields()) {
            if (childField.getFieldName().equals(descendantName) || childField.getAliases().contains(descendantName)) {
                final Object value = record.getValue(descendantName);
                if (value != null) {
                    final FieldValue descendantFieldValue = new StandardFieldValue(value, childField, fieldValue);
                    matchingValues.add(descendantFieldValue);
                }
            }

            final Object recordValue = record.getValue(childField.getFieldName());
            if (recordValue == null) {
                continue;
            }

            if (Filters.isRecord(childField.getDataType(), recordValue)) {
                final FieldValue childFieldValue = new StandardFieldValue(recordValue, childField, fieldValue);
                matchingValues.addAll(findDescendants(childFieldValue));
            }
        }

        return matchingValues;
    }
}
