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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

@SupportsBatching
@Tags({"test", "random", "generate"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("This processor creates FlowFiles with random data or custom content. GenerateFlowFile is useful" +
        " for load testing, configuration, and simulation.")
@DynamicProperty(name = "Generated FlowFile attribute name", value = "Generated FlowFile attribute value",
        expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY,
        description = "Specifies an attribute on generated FlowFiles defined by the Dynamic Property's key and value." +
        " If Expression Language is used, evaluation will be performed only once per batch of generated FlowFiles.")
public class GenerateFlowFile extends AbstractProcessor {

    private final AtomicReference<byte[]> data = new AtomicReference<>();

    public static final String DATA_FORMAT_BINARY = "Binary";
    public static final String DATA_FORMAT_TEXT = "Text";

    public static final PropertyDescriptor FILE_SIZE = new PropertyDescriptor.Builder()
            .name("File Size")
            .description("The size of the file that will be used")
            .required(true)
            .defaultValue("0B")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The number of FlowFiles to be transferred in each invocation")
            .required(true)
            .defaultValue("1")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    public static final PropertyDescriptor DATA_FORMAT = new PropertyDescriptor.Builder()
            .name("Data Format")
            .description("Specifies whether the data should be Text or Binary")
            .required(true)
            .defaultValue(DATA_FORMAT_TEXT)
            .allowableValues(DATA_FORMAT_BINARY, DATA_FORMAT_TEXT)
            .build();
    public static final PropertyDescriptor UNIQUE_FLOWFILES = new PropertyDescriptor.Builder()
            .name("Unique FlowFiles")
            .description("If true, each FlowFile that is generated will be unique. If false, a random value will be generated and all FlowFiles "
                    + "will get the same content but this offers much higher throughput")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor CUSTOM_TEXT = new PropertyDescriptor.Builder()
            .displayName("Custom Text")
            .name("generate-ff-custom-text")
            .description("If Data Format is text and if Unique FlowFiles is false, then this custom text will be used as content of the generated "
                    + "FlowFiles and the File Size will be ignored. Finally, if Expression Language is used, evaluation will be performed only once "
                    + "per batch of generated FlowFiles")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("character-set")
            .displayName("Character Set")
            .description("Specifies the character set to use when writing the bytes of Custom Text to a flow file.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    private static final char[] TEXT_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()-_=+/?.,';:\"?<>\n\t ".toCharArray();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(FILE_SIZE);
        descriptors.add(BATCH_SIZE);
        descriptors.add(DATA_FORMAT);
        descriptors.add(UNIQUE_FLOWFILES);
        descriptors.add(CUSTOM_TEXT);
        descriptors.add(CHARSET);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .dynamic(true)
            .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (context.getProperty(UNIQUE_FLOWFILES).asBoolean()) {
            this.data.set(null);
        } else if(!context.getProperty(CUSTOM_TEXT).isSet()) {
            this.data.set(generateData(context));
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        final boolean isUnique = validationContext.getProperty(UNIQUE_FLOWFILES).asBoolean();
        final boolean isText = validationContext.getProperty(DATA_FORMAT).getValue().equals(DATA_FORMAT_TEXT);
        final boolean isCustom = validationContext.getProperty(CUSTOM_TEXT).isSet();

        if(isCustom && (isUnique || !isText)) {
            results.add(new ValidationResult.Builder().subject("Custom Text").valid(false).explanation("If Custom Text is set, then Data Format must be "
                    + "text and Unique FlowFiles must be false.").build());
        }

        return results;
    }

    private byte[] generateData(final ProcessContext context) {
        final int byteCount = context.getProperty(FILE_SIZE).asDataSize(DataUnit.B).intValue();

        final Random random = new Random();
        final byte[] array = new byte[byteCount];
        if (context.getProperty(DATA_FORMAT).getValue().equals(DATA_FORMAT_BINARY)) {
            random.nextBytes(array);
        } else {
            for (int i = 0; i < array.length; i++) {
                final int index = random.nextInt(TEXT_CHARS.length);
                array[i] = (byte) TEXT_CHARS[index];
            }
        }

        return array;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final byte[] data;
        if (context.getProperty(UNIQUE_FLOWFILES).asBoolean()) {
            data = generateData(context);
        } else if(context.getProperty(CUSTOM_TEXT).isSet()) {
            final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
            data = context.getProperty(CUSTOM_TEXT).evaluateAttributeExpressions().getValue().getBytes(charset);
        } else {
            data = this.data.get();
        }

        Map<PropertyDescriptor, String> processorProperties = context.getProperties();
        Map<String, String> generatedAttributes = new HashMap<String, String>();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String dynamicValue = context.getProperty(property).evaluateAttributeExpressions().getValue();
                generatedAttributes.put(property.getName(), dynamicValue);
            }
        }

        for (int i = 0; i < context.getProperty(BATCH_SIZE).asInteger(); i++) {
            FlowFile flowFile = session.create();
            if (data.length > 0) {
                flowFile = session.write(flowFile, new OutputStreamCallback() {
                    @Override
                    public void process(final OutputStream out) throws IOException {
                        out.write(data);
                    }
                });
            }
            flowFile = session.putAllAttributes(flowFile, generatedAttributes);

            session.getProvenanceReporter().create(flowFile);
            session.transfer(flowFile, SUCCESS);
        }
    }
}
