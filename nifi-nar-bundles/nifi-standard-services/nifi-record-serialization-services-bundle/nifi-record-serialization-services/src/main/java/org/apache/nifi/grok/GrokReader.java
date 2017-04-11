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

package org.apache.nifi.grok;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RowRecordReaderFactory;
import org.apache.nifi.serialization.SchemaRegistryRecordReader;
import org.apache.nifi.serialization.record.RecordSchema;

import io.thekraken.grok.api.Grok;
import io.thekraken.grok.api.exception.GrokException;

@Tags({"grok", "logs", "logfiles", "parse", "unstructured", "text", "record", "reader", "regex", "pattern", "logstash"})
@CapabilityDescription("Provides a mechanism for reading unstructured text data, such as log files, and structuring the data "
    + "so that it can be processed. The service is configured using Grok patterns. "
    + "The service reads from a stream of data and splits each message that it finds into a separate Record, each containing the fields that are configured. "
    + "If a line in the input does not match the expected message pattern, the line of text is considered to be part of the previous "
    + "message, with the exception of stack traces. A stack trace that is found at the end of a log message is considered to be part "
    + "of the previous message but is added to the 'STACK_TRACE' field of the Record. If a record has no stack trace, it will have a NULL value "
    + "for the STACK_TRACE field. All fields that are parsed are considered to be of type String by default. If there is need to change the type of a field, "
    + "this can be accomplished by configuring the Schema Registry to use and adding the appropriate schema.")
public class GrokReader extends SchemaRegistryRecordReader implements RowRecordReaderFactory {
    private volatile Grok grok;
    private volatile boolean useSchemaRegistry;

    private static final String DEFAULT_PATTERN_NAME = "/default-grok-patterns.txt";

    static final PropertyDescriptor PATTERN_FILE = new PropertyDescriptor.Builder()
        .name("Grok Pattern File")
        .description("Path to a file that contains Grok Patterns to use for parsing logs. If not specified, a built-in default Pattern file "
            + "will be used. If specified, all patterns in the given pattern file will override the default patterns. See the Controller Service's "
            + "Additional Details for a list of pre-defined patterns.")
        .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
        .expressionLanguageSupported(true)
        .required(false)
        .build();

    static final PropertyDescriptor GROK_EXPRESSION = new PropertyDescriptor.Builder()
        .name("Grok Expression")
        .description("Specifies the format of a log line in Grok format. This allows the Record Reader to understand how to parse each log line. "
            + "If a line in the log file does not match this pattern, the line will be assumed to belong to the previous log message.")
        .addValidator(new GrokExpressionValidator())
        .required(true)
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(PATTERN_FILE);
        properties.add(GROK_EXPRESSION);
        return properties;
    }

    @OnEnabled
    public void preCompile(final ConfigurationContext context) throws GrokException, IOException {
        grok = new Grok();

        try (final InputStream in = getClass().getResourceAsStream(DEFAULT_PATTERN_NAME);
            final Reader reader = new InputStreamReader(in)) {
            grok.addPatternFromReader(reader);
        }

        if (context.getProperty(PATTERN_FILE).isSet()) {
            grok.addPatternFromFile(context.getProperty(PATTERN_FILE).evaluateAttributeExpressions().getValue());
        }

        grok.compile(context.getProperty(GROK_EXPRESSION).getValue());
        useSchemaRegistry = context.getProperty(OPTIONAL_SCHEMA_NAME).isSet() && context.getProperty(OPTIONAL_SCHEMA_REGISTRY).isSet();
    }

    @Override
    protected boolean isSchemaRequired() {
        return false;
    }

    @Override
    public RecordReader createRecordReader(final FlowFile flowFile, final InputStream in, final ComponentLog logger) throws IOException {
        final RecordSchema schema = useSchemaRegistry ? getSchema(flowFile) : null;
        return new GrokRecordReader(in, grok, schema);
    }
}
