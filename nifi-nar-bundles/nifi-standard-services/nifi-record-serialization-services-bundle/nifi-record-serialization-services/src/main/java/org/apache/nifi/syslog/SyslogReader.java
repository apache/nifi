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

package org.apache.nifi.syslog;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaAccessStrategy;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryService;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.apache.nifi.syslog.attributes.SyslogAttributes;
import org.apache.nifi.syslog.parsers.SyslogParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({"syslog", "logs", "logfiles", "parse", "text", "record", "reader"})
@CapabilityDescription("Attempts to parses the contents of a Syslog message in accordance to RFC5424 and RFC3164. In the " +
        "case of RFC5424 formatted messages, structured data is not supported, and will be returned as part of the message." +
        "Note: Be mindfull that RFC3164 is informational and a wide range of different implementations are present in" +
        " the wild.")
public class SyslogReader extends SchemaRegistryService implements RecordReaderFactory {
    public static final String GENERIC_SYSLOG_SCHEMA_NAME = "default-syslog-schema";
    static final AllowableValue GENERIC_SYSLOG_SCHEMA = new AllowableValue(GENERIC_SYSLOG_SCHEMA_NAME, "Use Generic Syslog Schema",
            "The schema will be the default Syslog schema.");
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies which character set of the Syslog messages")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    private volatile SyslogParser parser;
    private volatile RecordSchema recordSchema;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(1);
        properties.add(CHARSET);
        return properties;
    }


    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final String charsetName = context.getProperty(CHARSET).getValue();
        parser = new SyslogParser(Charset.forName(charsetName));
        recordSchema = createRecordSchema();
    }

    @Override
    protected List<AllowableValue> getSchemaAccessStrategyValues() {
        final List<AllowableValue> allowableValues = new ArrayList<>();
        allowableValues.add(GENERIC_SYSLOG_SCHEMA);
        return allowableValues;
    }

    @Override
    protected AllowableValue getDefaultSchemaAccessStrategy() {
        return GENERIC_SYSLOG_SCHEMA;
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final ConfigurationContext context) {
        return createAccessStrategy();
    }

    @Override
    protected SchemaAccessStrategy getSchemaAccessStrategy(final String strategy, final SchemaRegistry schemaRegistry, final ValidationContext context) {
        return createAccessStrategy();
    }

    static RecordSchema createRecordSchema() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField(SyslogAttributes.PRIORITY.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.SEVERITY.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.FACILITY.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.VERSION.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.TIMESTAMP.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.HOSTNAME.key(), RecordFieldType.STRING.getDataType(), true));
        fields.add(new RecordField(SyslogAttributes.BODY.key(), RecordFieldType.STRING.getDataType(), true));

        SchemaIdentifier schemaIdentifier = new StandardSchemaIdentifier.Builder().name(GENERIC_SYSLOG_SCHEMA_NAME).build();
        final RecordSchema schema = new SimpleRecordSchema(fields,schemaIdentifier);
        return schema;
    }

    private SchemaAccessStrategy createAccessStrategy() {
        return new SchemaAccessStrategy() {
            private final Set<SchemaField> schemaFields = EnumSet.noneOf(SchemaField.class);


            @Override
            public RecordSchema getSchema(Map<String, String> variables, InputStream contentStream, RecordSchema readSchema) throws SchemaNotFoundException {
                return recordSchema;
            }

            @Override
            public Set<SchemaField> getSuppliedSchemaFields() {
                return schemaFields;
            }
        };
    }

    @Override
    public RecordReader createRecordReader(final Map<String, String> variables, final InputStream in, final ComponentLog logger) throws IOException, SchemaNotFoundException {
        final RecordSchema schema = getSchema(variables, in, null);
        return new SyslogRecordReader(parser, in, schema);
    }
}
