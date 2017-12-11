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

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

public class CSVHeaderSchemaStrategy implements SchemaAccessStrategy {
    private static final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);

    private final ConfigurationContext context;

    public CSVHeaderSchemaStrategy(final ConfigurationContext context) {
        this.context = context;
    }

    public CSVHeaderSchemaStrategy(final ValidationContext context) {
        this.context = null;
    }

    @Override
    public RecordSchema getSchema(Map<String, String> variables, final InputStream contentStream, final RecordSchema readSchema) throws SchemaNotFoundException {
        if (this.context == null) {
            throw new SchemaNotFoundException("Schema Access Strategy intended only for validation purposes and cannot obtain schema");
        }

        try {
            final CSVFormat csvFormat = CSVUtils.createCSVFormat(context).withFirstRecordAsHeader();
            try (final Reader reader = new InputStreamReader(new BOMInputStream(contentStream));
                final CSVParser csvParser = new CSVParser(reader, csvFormat)) {

                final List<RecordField> fields = new ArrayList<>();
                for (final String columnName : csvParser.getHeaderMap().keySet()) {
                    fields.add(new RecordField(columnName, RecordFieldType.STRING.getDataType()));
                }

                return new SimpleRecordSchema(fields);
            }
        } catch (final Exception e) {
            throw new SchemaNotFoundException("Failed to read Header line from CSV", e);
        }
    }

    @Override
    public Set<SchemaField> getSuppliedSchemaFields() {
        return schemaFields;
    }
}
