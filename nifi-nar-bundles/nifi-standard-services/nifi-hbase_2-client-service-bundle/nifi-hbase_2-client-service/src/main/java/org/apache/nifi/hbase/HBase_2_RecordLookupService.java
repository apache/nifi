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

package org.apache.nifi.hbase;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.lookup.LookupFailureException;
import org.apache.nifi.lookup.LookupService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Tags({"hbase", "record", "lookup", "service"})
@CapabilityDescription("A lookup service that retrieves one or more columns from HBase and returns them as a record. The lookup coordinates " +
        "must contain 'rowKey' which will be the HBase row id.")
public class HBase_2_RecordLookupService extends AbstractHBaseLookupService implements LookupService<Record> {
    @Override
    public Optional<Record> lookup(Map<String, Object> coordinates) throws LookupFailureException {
        if (coordinates.get(ROW_KEY_KEY) == null) {
            return Optional.empty();
        }

        final String rowKey = coordinates.get(ROW_KEY_KEY).toString();
        if (StringUtils.isBlank(rowKey)) {
            return Optional.empty();
        }

        final byte[] rowKeyBytes = rowKey.getBytes(StandardCharsets.UTF_8);
        try {
            final Map<String, Object> values = scan(rowKeyBytes);

            if (values.size() > 0) {
                final List<RecordField> fields = new ArrayList<>();
                for (String key : values.keySet()) {
                    fields.add(new RecordField(key, RecordFieldType.STRING.getDataType()));
                }
                final RecordSchema schema = new SimpleRecordSchema(fields);
                return Optional.ofNullable(new MapRecord(schema, values));
            } else {
                return Optional.empty();
            }
        } catch (IOException e) {
            getLogger().error("Error occurred loading {}", coordinates.get("rowKey"), e);
            throw new LookupFailureException(e);
        }
    }

    @Override
    public Class<?> getValueType() {
        return Record.class;
    }

    @Override
    public Set<String> getRequiredKeys() {
        return REQUIRED_KEYS;
    }
}

