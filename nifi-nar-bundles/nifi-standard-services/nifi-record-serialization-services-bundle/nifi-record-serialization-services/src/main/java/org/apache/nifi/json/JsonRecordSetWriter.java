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

package org.apache.nifi.json;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeTextRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

@Tags({"json", "resultset", "writer", "serialize", "record", "recordset", "row"})
@CapabilityDescription("Writes the results of a RecordSet as a JSON Array. Even if the RecordSet "
    + "consists of a single row, it will be written as an array with a single element.")
public class JsonRecordSetWriter extends DateTimeTextRecordSetWriter implements RecordSetWriterFactory {

    static final AllowableValue ALWAYS_SUPPRESS = new AllowableValue("always-suppress", "Always Suppress",
        "Fields that are missing (present in the schema but not in the record), or that have a value of null, will not be written out");
    static final AllowableValue NEVER_SUPPRESS = new AllowableValue("never-suppress", "Never Suppress",
        "Fields that are missing (present in the schema but not in the record), or that have a value of null, will be written out as a null value");
    static final AllowableValue SUPPRESS_MISSING = new AllowableValue("suppress-missing", "Suppress Missing Values",
        "When a field has a value of null, it will be written out. However, if a field is defined in the schema and not present in the record, the field will not be written out.");

    static final PropertyDescriptor SUPPRESS_NULLS = new PropertyDescriptor.Builder()
        .name("suppress-nulls")
        .displayName("Suppress Null Values")
        .description("Specifies how the writer should handle a null field")
        .allowableValues(NEVER_SUPPRESS, ALWAYS_SUPPRESS, SUPPRESS_MISSING)
        .defaultValue(NEVER_SUPPRESS.getValue())
        .required(true)
        .build();
    static final PropertyDescriptor PRETTY_PRINT_JSON = new PropertyDescriptor.Builder()
        .name("Pretty Print JSON")
        .description("Specifies whether or not the JSON should be pretty printed")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    private volatile boolean prettyPrint;
    private volatile NullSuppression nullSuppression;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PRETTY_PRINT_JSON);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        prettyPrint = context.getProperty(PRETTY_PRINT_JSON).asBoolean();

        final NullSuppression suppression;
        final String suppressNullValue = context.getProperty(SUPPRESS_NULLS).getValue();
        if (ALWAYS_SUPPRESS.getValue().equals(suppressNullValue)) {
            suppression = NullSuppression.ALWAYS_SUPPRESS;
        } else if (SUPPRESS_MISSING.getValue().equals(suppressNullValue)) {
            suppression = NullSuppression.SUPPRESS_MISSING;
        } else {
            suppression = NullSuppression.NEVER_SUPPRESS;
        }
        this.nullSuppression = suppression;
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final RecordSchema schema, final OutputStream out) throws SchemaNotFoundException, IOException {
        return new WriteJsonResult(logger, schema, getSchemaAccessWriter(schema), out, prettyPrint, nullSuppression,
            getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null));
    }

}
