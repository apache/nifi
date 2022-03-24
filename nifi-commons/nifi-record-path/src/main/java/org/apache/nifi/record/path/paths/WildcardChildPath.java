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

public class WildcardChildPath extends RecordPathSegment {

    WildcardChildPath(final RecordPathSegment parent, final boolean absolute) {
        super("/*", parent, absolute);
    }


    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        return getParentPath().evaluate(context)
            // map to Optional<FieldValue> containing child element
            .flatMap(fieldVal -> getChildren(fieldVal));
    }

    private Stream<FieldValue> getChildren(final FieldValue fieldValue) {
        if (fieldValue == null || fieldValue.getValue() == null || !Filters.isRecord(fieldValue)) {
            return Stream.empty();
        }

        final Record record = (Record) fieldValue.getValue();
        return Filters.presentValues(record.getSchema().getFields().stream()
            .map(field -> {
                final Object value = record.getValue(field);
                if (value == null) {
                    return Optional.empty();
                }

                return Optional.of(new StandardFieldValue(value, field, fieldValue));
            }));
    }

}
