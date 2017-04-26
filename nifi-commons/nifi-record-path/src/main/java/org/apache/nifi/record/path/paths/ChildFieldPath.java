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

import java.util.Optional;
import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.StandardFieldValue;
import org.apache.nifi.record.path.util.Filters;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

public class ChildFieldPath extends RecordPathSegment {
    private final String childName;

    ChildFieldPath(final String childName, final RecordPathSegment parent, final boolean absolute) {
        super("/" + childName, parent, absolute);
        this.childName = childName;
    }

    private Optional<FieldValue> getChild(final FieldValue fieldValue) {
        if (fieldValue == null) {
            return Optional.empty();
        }

        if (fieldValue.getField().getDataType().getFieldType() != RecordFieldType.RECORD) {
            return Optional.empty();
        }

        final Record record = (Record) fieldValue.getValue();
        final Object value = record.getValue(childName);
        if (value == null) {
            return Optional.empty();
        }

        final Optional<RecordField> field = record.getSchema().getField(childName);
        if (!field.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new StandardFieldValue(value, field.get(), fieldValue));
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        return Filters.presentValues(
            getParentPath().evaluate(context)
            // map to Optional<FieldValue> containing child element
            .map(fieldVal -> getChild(fieldVal)));
    }
}
