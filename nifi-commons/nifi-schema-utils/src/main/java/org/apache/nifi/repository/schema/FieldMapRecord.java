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

package org.apache.nifi.repository.schema;

import java.util.HashMap;
import java.util.Map;

public class FieldMapRecord implements Record {
    private final Map<String, Object> values;
    private final RecordSchema schema;

    public FieldMapRecord(final Map<RecordField, Object> values, final RecordSchema schema) {
        this.schema = schema;
        this.values = convertFieldToName(values);
    }

    private static Map<String, Object> convertFieldToName(final Map<RecordField, Object> map) {
        final Map<String, Object> nameMap = new HashMap<>(map.size());
        for (final Map.Entry<RecordField, Object> entry : map.entrySet()) {
            nameMap.put(entry.getKey().getFieldName(), entry.getValue());
        }
        return nameMap;
    }

    @Override
    public Object getFieldValue(final RecordField field) {
        return values.get(field.getFieldName());
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Object getFieldValue(final String fieldName) {
        return values.get(fieldName);
    }

    @Override
    public String toString() {
        return "FieldMapRecord[" + values + "]";
    }

    @Override
    public int hashCode() {
        return 33 + 41 * values.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        if (!(obj instanceof FieldMapRecord)) {
            return false;
        }

        final FieldMapRecord other = (FieldMapRecord) obj;
        return values.equals(other.values);
    }
}
