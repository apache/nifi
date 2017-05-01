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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
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

    static final PropertyDescriptor PRETTY_PRINT_JSON = new PropertyDescriptor.Builder()
        .name("Pretty Print JSON")
        .description("Specifies whether or not the JSON should be pretty printed")
        .expressionLanguageSupported(false)
        .allowableValues("true", "false")
        .defaultValue("false")
        .required(true)
        .build();

    private boolean prettyPrint;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PRETTY_PRINT_JSON);
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        prettyPrint = context.getProperty(PRETTY_PRINT_JSON).asBoolean();
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger, final FlowFile flowFile, final InputStream flowFileContent) throws SchemaNotFoundException, IOException {
        final RecordSchema schema = getSchema(flowFile, flowFileContent);
        return new WriteJsonResult(logger, schema, getSchemaAccessWriter(schema), prettyPrint,
            getDateFormat().orElse(null), getTimeFormat().orElse(null), getTimestampFormat().orElse(null));
    }

}
