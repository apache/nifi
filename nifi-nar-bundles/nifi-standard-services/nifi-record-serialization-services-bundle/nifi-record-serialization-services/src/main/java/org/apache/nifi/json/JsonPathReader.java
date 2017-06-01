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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

import com.jayway.jsonpath.JsonPath;

@Tags({"json", "jsonpath", "record", "reader", "parser"})
@CapabilityDescription("Parses JSON records and evaluates user-defined JSON Path's against each JSON object. The root element may be either "
    + "a single JSON object or a JSON array. If a JSON array is found, each JSON object within that array is treated as a separate record. "
    + "User-defined properties define the fields that should be extracted from the JSON in order to form the fields of a Record. Any JSON field "
    + "that is not extracted via a JSONPath will not be returned in the JSON Records.")
@SeeAlso(JsonTreeReader.class)
@DynamicProperty(name = "The field name for the record.",
    value="A JSONPath Expression that will be evaluated against each JSON record. The result of the JSONPath will be the value of the "
        + "field whose name is the same as the property name.",
    description="User-defined properties identifiy how to extract specific fields from a JSON object in order to create a Record",
    supportsExpressionLanguage=false)
public class JsonPathReader extends SchemaRegistryService implements RecordReaderFactory {

    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;
    private volatile LinkedHashMap<String, JsonPath> jsonPaths;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }


    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("JsonPath Expression that indicates how to retrieve the value from a JSON Object for the '" + propertyDescriptorName + "' column")
            .dynamic(true)
            .required(false)
            .addValidator(new JsonPathValidator())
            .build();
    }

    @OnEnabled
    public void compileJsonPaths(final ConfigurationContext context) {
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();

        final LinkedHashMap<String, JsonPath> compiled = new LinkedHashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (!descriptor.isDynamic()) {
                continue;
            }

            final String fieldName = descriptor.getName();
            final String expression = context.getProperty(descriptor).getValue();
            final JsonPath jsonPath = JsonPath.compile(expression);
            compiled.put(fieldName, jsonPath);
        }

        jsonPaths = compiled;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        boolean pathSpecified = false;
        for (final PropertyDescriptor property : validationContext.getProperties().keySet()) {
            if (property.isDynamic()) {
                pathSpecified = true;
                break;
            }
        }

        if (pathSpecified) {
            return Collections.emptyList();
        }

        return Collections.singleton(new ValidationResult.Builder()
            .subject("JSON Paths")
            .valid(false)
            .explanation("No JSON Paths were specified")
            .build());
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new JsonPathRowRecordReader(jsonPaths, schema, in, logger, dateFormat, timeFormat, timestampFormat);
    }

}
