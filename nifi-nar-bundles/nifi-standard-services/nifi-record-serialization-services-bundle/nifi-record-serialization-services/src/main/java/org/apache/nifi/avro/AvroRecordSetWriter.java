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

package org.apache.nifi.avro;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.AbstractRecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;

@Tags({"avro", "result", "set", "writer", "serializer", "record", "row"})
@CapabilityDescription("Writes the contents of a Database ResultSet in Binary Avro format. The data types in the Result Set must match those "
    + "specified by the Avro Schema. No type coercion will occur, with the exception of Date, Time, and Timestamps fields because Avro does not provide "
    + "support for these types specifically. As a result, they will be converted to String fields using the configured formats. In addition, the label"
    + "of the column must be a valid Avro field name.")
public class AvroRecordSetWriter extends AbstractRecordSetWriter implements RecordSetWriterFactory {
    static final PropertyDescriptor SCHEMA = new PropertyDescriptor.Builder()
        .name("Avro Schema")
        .description("The Avro Schema to use when writing out the Result Set")
        .addValidator(new AvroSchemaValidator())
        .expressionLanguageSupported(false)
        .required(true)
        .build();

    private volatile Schema schema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(SCHEMA);
        return properties;
    }

    @OnEnabled
    public void storePropertyValues(final ConfigurationContext context) {
        schema = new Schema.Parser().parse(context.getProperty(SCHEMA).getValue());
    }

    @Override
    public RecordSetWriter createWriter(final ComponentLog logger) {
        return new WriteAvroResult(schema, getDateFormat(), getTimeFormat(), getTimestampFormat());
    }

}
