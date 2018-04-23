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

package org.apache.nifi.xml;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.DateTimeUtils;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Tags({"xml", "record", "reader", "parser"})
@CapabilityDescription("Reads XML content and creates Record objects. Records are expected in the second level of " +
        "XML data, embedded in an enclosing root tag.")
public class XMLReader extends SchemaRegistryService implements RecordReaderFactory {

    public static final AllowableValue RECORD_SINGLE = new AllowableValue("false");
    public static final AllowableValue RECORD_ARRAY = new AllowableValue("true");
    public static final AllowableValue RECORD_EVALUATE = new AllowableValue("${xml.stream.is.array}","Use attribute xml.stream.is.array");

    public static final PropertyDescriptor RECORD_FORMAT = new PropertyDescriptor.Builder()
            .name("record_format")
            .displayName("Expect Records as Array")
            .description("This property defines whether the reader expects a single record an array of records. If the property is " +
                    "set to \"true\", the reader expects an array of records and the outer element of the XML will be treated as a " +
                    "wrapper for the records. If the property is set to \"false\", the reader expects a single record for each FlowFile " +
                    "(without wrapper-element). If the property is set to \"Use attribute xml.stream.is.array\", the attribute " +
                    "\"xml.stream.is.array\" will be evaluated for each FlowFile whether to treat its content as array " +
                    "of records (in the case of \"true\") or as single record (in the case of \"false\".")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(RECORD_SINGLE, RECORD_ARRAY, RECORD_EVALUATE)
            .defaultValue(RECORD_SINGLE.getValue())
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_PREFIX = new PropertyDescriptor.Builder()
            .name("attribute_prefix")
            .displayName("Attribute Prefix")
            .description("If this property is set, the name of attributes will be prepended with a prefix when they are added to a record.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor CONTENT_FIELD_NAME = new PropertyDescriptor.Builder()
            .name("content_field_name")
            .displayName("Field Name for Content")
            .description("If tags with content (e. g. <field>content</field>) are defined as nested records in the schema, " +
                    "the name of the tag will be used as name for the record and the value of this property will be used as name for the field. " +
                    "If tags with content shall be parsed together with attributes (e. g. <field attribute=\"123\">content</field>), " +
                    "they have to be defined as records. For additional information, see the section of processor usage.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(RECORD_FORMAT);
        properties.add(ATTRIBUTE_PREFIX);
        properties.add(CONTENT_FIELD_NAME);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final ConfigurationContext context = getConfigurationContext();

        final RecordSchema schema = getSchema(variables, in, null);

        final String attributePrefix = context.getProperty(ATTRIBUTE_PREFIX).isSet()
                ? context.getProperty(ATTRIBUTE_PREFIX).evaluateAttributeExpressions(variables).getValue().trim() : null;

        final String contentFieldName = context.getProperty(CONTENT_FIELD_NAME).isSet()
                ? context.getProperty(CONTENT_FIELD_NAME).evaluateAttributeExpressions(variables).getValue().trim() : null;

        final boolean isArray = Boolean.parseBoolean(context.getProperty(RECORD_FORMAT).evaluateAttributeExpressions(variables).getValue());

        return new XMLRecordReader(in, schema, isArray, attributePrefix, contentFieldName, dateFormat, timeFormat, timestampFormat, logger);
    }
}
