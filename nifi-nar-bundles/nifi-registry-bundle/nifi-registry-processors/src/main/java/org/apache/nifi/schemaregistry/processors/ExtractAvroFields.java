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
package org.apache.nifi.schemaregistry.processors;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;

@Tags({ "registry", "schema", "avro", "extract", "evaluate" })
@CapabilityDescription("Extracts Avro field and assigns it to the FlowFile attribute")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@DynamicProperty(name = "Avro field name", value = "FlowFile attribute name to set the extracted field",
                 description = "The value of the Avro field specified by 'Avro field name' will be extracted and set as "
                         + "FlowFile attribute under name specified by the value of this property.")
public final class ExtractAvroFields extends AbstractTransformer {

    private static final List<PropertyDescriptor> DESCRIPTORS;

    static {
        List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.addAll(BASE_DESCRIPTORS);
        descriptors.add(SCHEMA_TYPE);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);
    }

    private volatile Map<String, String> dynamicProperties;

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     *
     */
    @Override
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        super.onScheduled(context);
        this.dynamicProperties = context.getProperties().entrySet().stream()
                .filter(p -> p.getKey().isDynamic())
                .collect(Collectors.toMap(p -> p.getKey().getName(), p -> p.getValue()));
    }

    /**
     *
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(false)
                .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
                .required(false)
                .dynamic(true)
                .build();
    }

    /**
     *
     */
    @Override
    protected Map<String, String> transform(InputStream in, InvocationContextProperties contextProperties, Schema schema) {
        GenericRecord avroRecord = AvroUtils.read(in, schema);
        Map<String, String> attributes = this.dynamicProperties.entrySet().stream().collect(
                Collectors.toMap(dProp -> dProp.getValue(), dProp -> String.valueOf(avroRecord.get(dProp.getKey()))));
        return Collections.unmodifiableMap(attributes);
    }
}
