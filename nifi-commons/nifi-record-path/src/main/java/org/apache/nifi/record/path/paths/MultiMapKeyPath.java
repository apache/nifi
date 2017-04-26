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

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.MapEntryFieldValue;
import org.apache.nifi.record.path.RecordPathEvaluationContext;
import org.apache.nifi.record.path.util.Filters;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.MapDataType;

public class MultiMapKeyPath extends RecordPathSegment {
    private final List<String> mapKeys;

    MultiMapKeyPath(final List<String> mapKeys, final RecordPathSegment parent, final boolean absolute) {
        super(mapKeys.toString(), parent, absolute);
        this.mapKeys = mapKeys;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Stream<FieldValue> parentResult = getParentPath().evaluate(context);

        return parentResult
            .filter(Filters.fieldTypeFilter(RecordFieldType.MAP))
            .flatMap(fieldValue -> {
                final Map<String, ?> map = (Map<String, ?>) fieldValue.getValue();
                return mapKeys.stream().map(key -> {
                    final DataType valueType = ((MapDataType) fieldValue.getField().getDataType()).getValueType();
                    final RecordField elementField = new RecordField(fieldValue.getField().getFieldName() + "['" + key + "']", valueType);
                    return new MapEntryFieldValue(map.get(key), elementField, fieldValue, key);
                });
            });
    }
}
