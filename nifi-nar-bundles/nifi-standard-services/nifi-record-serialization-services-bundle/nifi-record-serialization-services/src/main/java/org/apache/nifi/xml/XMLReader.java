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

    public static final AllowableValue RECORD_SINGLE = new AllowableValue("false", "false",
        "Each FlowFile will consist of a single record without any sort of \"wrapper\".");
    public static final AllowableValue RECORD_ARRAY = new AllowableValue("true", "true",
        "Each FlowFile will consist of zero or more records. The outer-most XML element is expected to be a \"wrapper\" and will be ignored.");
    public static final AllowableValue RECORD_EVALUATE = new AllowableValue("${xml.stream.is.array}", "Use attribute 'xml.stream.is.array'",
        "Whether to treat a FlowFile as a single Record or an array of multiple Records is determined by the value of the 'xml.stream.is.array' attribute. "
            + "If the value of the attribute is 'true' (case-insensitive), then the XML Reader will treat the FlowFile as a series of Records with the outer element being ignored. "
            + "If the value of the attribute is 'false' (case-insensitive), then the FlowFile is treated as a single Record and no wrapper element is assumed. "
            + "If the attribute is missing or its value is anything other than 'true' or 'false', then an Exception will be thrown and no records will be parsed.");

    public static final PropertyDescriptor RECORD_FORMAT = new PropertyDescriptor.Builder()
            .name("record_format")
            .displayName("Expect Records as Array")
            .description("This property defines whether the reader expects a FlowFile to consist of a single Record or a series of Records with a \"wrapper element\". Because XML does not "
                + "provide for a way to read a series of XML documents from a stream directly, it is common to combine many XML documents by concatenating them and then wrapping the entire "
                + "XML blob  with a \"wrapper element\". This property dictates whether the reader expects a FlowFile to consist of a single Record or a series of Records with a \"wrapper element\" "
                + "that will be ignored.")
            .allowableValues(RECORD_SINGLE, RECORD_ARRAY, RECORD_EVALUATE)
            .defaultValue(RECORD_SINGLE.getValue())
            .required(true)
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

        final boolean isArray;
        final String recordFormat = context.getProperty(RECORD_FORMAT).evaluateAttributeExpressions(variables).getValue().trim();
        if ("true".equalsIgnoreCase(recordFormat)) {
            isArray = true;
        } else if ("false".equalsIgnoreCase(recordFormat)) {
            isArray = false;
        } else {
            throw new IOException("Cannot parse XML Records because the '" + RECORD_FORMAT.getDisplayName() + "' property evaluates to '"
                + recordFormat + "', which is neither 'true' nor 'false'");
        }

        return new XMLRecordReader(in, schema, isArray, attributePrefix, contentFieldName, dateFormat, timeFormat, timestampFormat, logger);
    }
}
