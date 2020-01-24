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
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Tags({"lookup", "result", "set", "writer", "serializer", "record", "recordset", "row"})
@SeeAlso({ReaderLookup.class})
@CapabilityDescription("Provides a RecordSetWriterFactory that can be used to dynamically select another RecordSetWriterFactory. This service " +
        "requires a variable named 'recordsetwriter.name' to be passed in when asking for a schema or record set writer, and will throw an exception " +
        "if the variable is missing. The value of 'recordsetwriter.name' will be used to select the RecordSetWriterFactory that has been " +
        "registered with that name. This will allow multiple RecordSetWriterFactory's to be defined and registered, and then selected " +
        "dynamically at runtime by tagging flow files with the appropriate 'recordsetwriter.name' variable.")
@DynamicProperty(name = "Name of the RecordSetWriter", value = "A RecordSetWriterFactory controller service", expressionLanguageScope = ExpressionLanguageScope.NONE,
        description = "")
public class RecordSetWriterLookup extends AbstractControllerService implements RecordSetWriterFactory {

    public static final String RECORDWRITER_NAME_VARIABLE = "recordsetwriter.name";

    private volatile Map<String,RecordSetWriterFactory> recordSetWriterFactoryMap;

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("The RecordSetWriterFactory to return when recordwriter.name = '" + propertyDescriptorName + "'")
                .identifiesControllerService(RecordSetWriterFactory.class)
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
                        .explanation("the current service cannot be registered as a RecordSetWriterFactory to lookup")
                        .valid(false)
                        .build());
            }
        }

        if (numDefinedServices == 0) {
            results.add(new ValidationResult.Builder()
                    .subject(this.getClass().getSimpleName())
                    .explanation("at least one RecordSetWriterFactory must be defined via dynamic properties")
                    .valid(false)
                    .build());
        }

        return results;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        final Map<String,RecordSetWriterFactory> serviceMap = new HashMap<>();

        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
                final RecordSetWriterFactory recordSetWriterFactory = context.getProperty(descriptor).asControllerService(RecordSetWriterFactory.class);
                serviceMap.put(descriptor.getName(), recordSetWriterFactory);
            }
        }

        recordSetWriterFactoryMap = Collections.unmodifiableMap(serviceMap);
    }

    @OnDisabled
    public void onDisabled() {
        recordSetWriterFactoryMap = null;
    }


    @Override
    public RecordSchema getSchema(Map<String, String> variables, RecordSchema readSchema) throws SchemaNotFoundException, IOException {
        return getRecordSetWriterFactory(variables).getSchema(variables, readSchema);
    }

    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out) {
        throw new UnsupportedOperationException("Cannot lookup RecordSetWriterFactory without variables");
    }

    @Override
    public RecordSetWriter createWriter(ComponentLog logger, RecordSchema schema, OutputStream out, Map<String, String> variables) throws SchemaNotFoundException, IOException {
        return getRecordSetWriterFactory(variables).createWriter(logger, schema, out, variables);
    }

    private RecordSetWriterFactory getRecordSetWriterFactory(Map<String, String> variables){
        if (variables == null) {
            throw new UnsupportedOperationException("Cannot lookup RecordSetWriterFactory without variables");
        }

        if (!variables.containsKey(RECORDWRITER_NAME_VARIABLE)) {
            throw new ProcessException("Attributes must contain an variables name '" + RECORDWRITER_NAME_VARIABLE + "'");
        }

        final String recordSetWriterName = variables.get(RECORDWRITER_NAME_VARIABLE);
        if (StringUtils.isBlank(recordSetWriterName)) {
            throw new ProcessException(RECORDWRITER_NAME_VARIABLE + " cannot be null or blank");
        }

        final RecordSetWriterFactory recordSetWriterFactory = recordSetWriterFactoryMap.get(recordSetWriterName);
        if (recordSetWriterFactory == null) {
            throw new ProcessException("No RecordSetWriterFactory was found for " + RECORDWRITER_NAME_VARIABLE
                    + "'" + recordSetWriterName + "'");
        }

        return recordSetWriterFactory;
    }
}