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

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.schema.inference.RecordSourceFactory;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.nifi.schema.inference.SchemaInferenceUtil.INFER_SCHEMA;
import static org.apache.nifi.schema.inference.SchemaInferenceUtil.SCHEMA_CACHE;

@Tags({"json", "tree", "record", "reader", "parser"})
@CapabilityDescription("Parses JSON into individual Record objects. While the reader expects each record "
        + "to be well-formed JSON, the content of a FlowFile may consist of many records, each as a well-formed "
        + "JSON array or JSON object with optional whitespace between them, such as the common 'JSON-per-line' format. "
        + "If an array is encountered, each element in that array will be treated as a separate record. "
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
        properties.add(SCHEMA_CACHE);
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
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {
        final RecordSourceFactory<JsonNode> jsonSourceFactory = (var, in) -> new JsonRecordSource(in);
        final Supplier<SchemaInferenceEngine<JsonNode>> inferenceSupplier = () -> new JsonSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));

        return SchemaInferenceUtil.getSchemaAccessStrategy(strategy, context, getLogger(), jsonSourceFactory, inferenceSupplier,
            () -> super.getSchemaAccessStrategy(strategy, schemaRegistry, context));
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INFER_SCHEMA;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, MalformedRecordException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat);
    }
}
