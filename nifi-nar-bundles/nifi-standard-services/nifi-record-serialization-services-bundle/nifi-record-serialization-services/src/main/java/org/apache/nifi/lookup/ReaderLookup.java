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
package org.apache.nifi.lookup;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({"lookup", "parse", "record", "row", "reader"})
@SeeAlso({RecordSetWriterLookup.class})
@CapabilityDescription("Provides a RecordReaderFactory that can be used to dynamically select another RecordReaderFactory. This service " +
        "requires an variable named 'recordreader.name' to be passed in when asking for a record record, and will throw an exception " +
        "if the variable is missing. The value of 'recordreader.name' will be used to select the RecordReaderFactory that has been " +
        "registered with that name. This will allow multiple RecordReaderFactory's to be defined and registered, and then selected " +
        "dynamically at runtime by tagging flow files with the appropriate 'recordreader.name' variable.")
@DynamicProperty(name = "Name of the RecordReader", value = "A RecordReaderFactory controller service", expressionLanguageScope = ExpressionLanguageScope.NONE,
        description = "")
public class ReaderLookup extends AbstractControllerService implements RecordReaderFactory {

    public static final String RECORDREADER_NAME_VARIABLE = "recordreader.name";

    private volatile Map<String, RecordReaderFactory> recordReaderFactoryMap;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("The RecordReaderFactory to return when recordreader.name = '" + propertyDescriptorName + "'")
                .identifiesControllerService(RecordReaderFactory.class)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .build();
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();

        int numDefinedServices = 0;
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                numDefinedServices++;
            }

            final String referencedId = context.getProperty(descriptor).getValue();
            if (this.getIdentifier().equals(referencedId)) {
                results.add(new ValidationResult.Builder()
                        .subject(descriptor.getDisplayName())
                        .explanation("the current service cannot be registered as a RecordReaderFactory to lookup")
                        .valid(false)
                        .build());
            }
        }

        if (numDefinedServices == 0) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("at least one RecordReaderFactory must be defined via dynamic properties")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String,RecordReaderFactory> serviceMap = new HashMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final RecordReaderFactory recordReaderFactory = context.getProperty(descriptor).asControllerService(RecordReaderFactory.class);
                serviceMap.put(descriptor.getName(), recordReaderFactory);
            }
        }

        recordReaderFactoryMap = Collections.unmodifiableMap(serviceMap);
    }

    @OnDisabled
    public void onDisabled() {
        recordReaderFactoryMap = null;
    }


    @Override
    public RecordReader createRecordReader(FlowFile flowFile, InputStream in, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        if(flowFile == null) {
            throw new UnsupportedOperationException("Cannot lookup a RecordReaderFactory without variables.");
        }

        return createRecordReader(flowFile.getAttributes(), in, flowFile.getSize(), logger);
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {
        if(variables == null) {
            throw new UnsupportedOperationException("Cannot lookup a RecordReaderFactory without variables.");
        }

        if (!variables.containsKey(RECORDREADER_NAME_VARIABLE)) {
            throw new ProcessException("Variables must contain a variables name '" + RECORDREADER_NAME_VARIABLE + "'");
        }

        final String recordReaderName = variables.get(RECORDREADER_NAME_VARIABLE);
        if (StringUtils.isBlank(recordReaderName)) {
            throw new ProcessException(RECORDREADER_NAME_VARIABLE + " cannot be null or blank");
        }

        final RecordReaderFactory recordReaderFactory = recordReaderFactoryMap.get(recordReaderName);
        if (recordReaderFactory == null) {
            throw new ProcessException("No RecordReaderFactory was found for " + RECORDREADER_NAME_VARIABLE
                    + "'" + recordReaderName + "'");
        }

        return recordReaderFactory.createRecordReader(variables, in, inputLength, logger);
    }
}