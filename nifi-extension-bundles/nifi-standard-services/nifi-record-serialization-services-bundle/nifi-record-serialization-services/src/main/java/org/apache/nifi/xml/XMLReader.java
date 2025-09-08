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
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaAccessUtils;
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
import org.apache.nifi.xml.inference.XmlNode;
import org.apache.nifi.xml.inference.XmlRecordSource;
import org.apache.nifi.xml.inference.XmlSchemaInference;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.apache.nifi.schema.inference.SchemaInferenceUtil.INFER_SCHEMA;

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
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
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
                    "they have to be defined as records. In such a case, the name of the tag will be used as the name for the record and  " +
                    "the value of this property will be used as the name for the field holding the original content. The name of the attribute " +
                    "will be used to create a new record field, the content of which will be the value of the attribute. " +
                    "For more information, see the 'Additional Details...' section of the XMLReader controller service's documentation.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(false)
            .build();

    public static final PropertyDescriptor PARSE_XML_ATTRIBUTES = new PropertyDescriptor.Builder()
            .name("parse_xml_attributes")
            .displayName("Parse XML Attributes")
            .description("When 'Schema Access Strategy' is 'Infer Schema' and this property is 'true' then XML attributes are parsed and " +
                    "added to the record as new fields. When the schema is inferred but this property is 'false', " +
                    "XML attributes and their values are ignored.")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues("true", "false")
            .defaultValue("true")
            .required(false)
            .dependsOn(SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, INFER_SCHEMA)
            .build();

    private volatile boolean parseXmlAttributes;
    private volatile String dateFormat;
    private volatile String timeFormat;
    private volatile String timestampFormat;

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.parseXmlAttributes = context.getProperty(PARSE_XML_ATTRIBUTES).asBoolean();
        this.dateFormat = context.getProperty(DateTimeUtils.DATE_FORMAT).getValue();
        this.timeFormat = context.getProperty(DateTimeUtils.TIME_FORMAT).getValue();
        this.timestampFormat = context.getProperty(DateTimeUtils.TIMESTAMP_FORMAT).getValue();
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PARSE_XML_ATTRIBUTES);
        properties.add(SchemaInferenceUtil.SCHEMA_CACHE);
        properties.add(RECORD_FORMAT);
        properties.add(ATTRIBUTE_PREFIX);
        properties.add(CONTENT_FIELD_NAME);
        properties.add(DateTimeUtils.DATE_FORMAT);
        properties.add(DateTimeUtils.TIME_FORMAT);
        properties.add(DateTimeUtils.TIMESTAMP_FORMAT);
        return properties;
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>(super.getSchemaAccessStrategyValues());
        allowableValues.add(INFER_SCHEMA);
        return allowableValues;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final PropertyContext context) {

        final RecordSourceFactory<XmlNode> sourceFactory = (variables, contentStream) -> {
            String contentFieldName = trim(context.getProperty(CONTENT_FIELD_NAME).evaluateAttributeExpressions(variables).getValue());
            contentFieldName = (contentFieldName == null) ? "value" : contentFieldName;
            final String attributePrefix = trim(context.getProperty(ATTRIBUTE_PREFIX).evaluateAttributeExpressions(variables).getValue());
            return new XmlRecordSource(contentStream, contentFieldName, isMultipleRecords(context, variables), parseXmlAttributes, attributePrefix);
        };
        final Supplier<SchemaInferenceEngine<XmlNode>> schemaInference = () -> new XmlSchemaInference(new TimeValueInference(dateFormat, timeFormat, timestampFormat));

        return SchemaInferenceUtil.getSchemaAccessStrategy(strategy, context, getLogger(), sourceFactory, schemaInference,
            () -> super.getSchemaAccessStrategy(strategy, schemaRegistry, context));
    }

    private boolean isMultipleRecords(final PropertyContext context, final Map<String, String> variables) {
        final String recordFormat = context.getProperty(RECORD_FORMAT).evaluateAttributeExpressions(variables).getValue().trim();
        if ("true".equalsIgnoreCase(recordFormat)) {
            return true;
        } else if ("false".equalsIgnoreCase(recordFormat)) {
            return false;
        } else {
            throw new ProcessException("Cannot parse XML Records because the '" + RECORD_FORMAT.getDisplayName() + "' property evaluates to '"
                + recordFormat + "', which is neither 'true' nor 'false'");
        }
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return INFER_SCHEMA;
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final long inputLength, final ComponentLog logger)
            throws IOException, SchemaNotFoundException, MalformedRecordException {
        final ConfigurationContext context = getConfigurationContext();

        final RecordSchema schema = getSchema(variables, in, null);

        final String attributePrefix = trim(context.getProperty(ATTRIBUTE_PREFIX).evaluateAttributeExpressions(variables).getValue());
        final String contentFieldName = trim(context.getProperty(CONTENT_FIELD_NAME).evaluateAttributeExpressions(variables).getValue());
        final boolean isArray = isMultipleRecords(context, variables);

        return new XMLRecordReader(in, schema, isArray, parseXmlAttributes, attributePrefix, contentFieldName, dateFormat, timeFormat, timestampFormat, logger);
    }

    private String trim(final String value) {
        return value == null ? null : value.trim();
    }
}
