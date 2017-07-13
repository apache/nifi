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

package org.apache.nifi.csv;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

public class CSVExplicitColumnsSchemaStrategy implements SchemaAccessStrategy {
    private static final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

    private final PropertyValue explicitColumnsPropertyValue;

    public CSVExplicitColumnsSchemaStrategy(final PropertyValue explicitColumnsPropertyValue) {
        this.explicitColumnsPropertyValue = explicitColumnsPropertyValue;
    }

    static final String INVALID_EXPLICIT_COLUMNS_PROPERTY_VALUE_EXPLANATION = "Property '" + CSVUtils.EXPLICIT_COLUMNS.getDisplayName() +
            "' must be defined and non-empty when using Schema Access Strategy '" + CSVUtils.EXPLICIT_COLUMNS_DISPLAY_NAME + "'.";

    static boolean isExplicitColumnsPropertyValueValid(PropertyValue explicitColumnsPropertyValue) {
        return explicitColumnsPropertyValue.isSet() && !explicitColumnsPropertyValue.getValue().isEmpty();
    }

    @Override
    public RecordSchema getSchema(FlowFile flowFile, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        if (!isExplicitColumnsPropertyValueValid(explicitColumnsPropertyValue)) {
            throw new SchemaNotFoundException(INVALID_EXPLICIT_COLUMNS_PROPERTY_VALUE_EXPLANATION);
        }

        List<RecordField> fields = new ArrayList<>();
        String[] columns = explicitColumnsPropertyValue.getValue().split(",");
        for (String column : columns) {
            fields.add(new RecordField(column.trim(), RecordFieldType.STRING.getDataType()));
        }

        return new SimpleRecordSchema(fields);
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
