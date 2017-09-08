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
import java.util.Map;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;

@Tags({"json", "tree", "record", "reader", "parser"})
@CapabilityDescription("Parses JSON into individual Record objects. The Record that is produced will contain all top-level "
    + "elements of the corresponding JSON Object. "
    + "The root JSON element can be either a single element or an array of JSON elements, and each "
    + "element in that array will be treated as a separate record. "
    + "If the schema that is configured contains a field that is not present in the JSON, a null value will be used. If the JSON contains "
    + "a field that is not present in the schema, that field will be skipped. "
    + "See the Usage of the Controller Service for more information and examples.")
@SeeAlso(JsonPathReader.class)
public class JsonTreeReader extends SchemaRegistryService implements RecordReaderFactory {

    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @OnEnabled
    public void storeFormats(final ConfigurationContext context) {
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException {
        return new JsonTreeRowRecordReader(in, logger, getSchema(variables, in, null), dateFormat, timeFormat, timestampFormat);
    }
}
