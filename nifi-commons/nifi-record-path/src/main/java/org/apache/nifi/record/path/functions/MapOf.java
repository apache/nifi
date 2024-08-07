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
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class MapOf extends RecordPathSegment {
    private final RecordPathSegment[] valuePaths;

    public MapOf(final RecordPathSegment[] valuePaths, final boolean absolute) {
        super("mapOf", null, absolute);
        this.valuePaths = valuePaths;
    }

    @Override
    public Stream<FieldValue> evaluate(final RecordPathEvaluationContext context) {
        final Map<String, Object> values = new HashMap<>();

        for (int i = 0; i + 1 < valuePaths.length; i += 2) {
            final String key = valuePaths[i].evaluate(context).findFirst().get().toString();
            final String value = valuePaths[i + 1].evaluate(context).findFirst().get().toString();
            values.put(key, value);
        }

        final RecordField field = new RecordField("mapOf", RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));
        final FieldValue responseValue = new StandardFieldValue(values, field, null);

        return Stream.of(responseValue);
    }

}