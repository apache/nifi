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

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.migration.PropertyConfiguration;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.scheduling.SchedulingStrategy;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

@SupportsBatching
@Tags({"test", "random", "generate", "load"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("This processor creates FlowFiles with random data or custom content. GenerateFlowFile is useful" +
        " for load testing, configuration, and simulation." + " Also see DuplicateFlowFile for additional load testing.")
@DynamicProperty(name = "Generated FlowFile attribute name", value = "Generated FlowFile attribute value",
        expressionLanguageScope = ExpressionLanguageScope.ENVIRONMENT,
        description = "Specifies an attribute on generated FlowFiles defined by the Dynamic Property's key and value." +
        " If Expression Language is used, evaluation will be performed only once per batch of generated FlowFiles.")
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "Sets the MIME type of the output if the 'Mime Type' property is set"),
})
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class GenerateFlowFile extends AbstractProcessor {

    private static final int BUFFER_SIZE = 8192;

    private final AtomicReference<GeneratedData> generatedData = new AtomicReference<>();

    public static final String DATA_FORMAT_BINARY = "Binary";
    public static final String DATA_FORMAT_TEXT = "Text";

    public static final PropertyDescriptor FILE_SIZE = new PropertyDescriptor.Builder()
            .name("File Size")
            .description("The size of the file that will be used")
            .required(true)
            .defaultValue("0B")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(0, Integer.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
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
            .description("If true, each FlowFile that is generated will be unique. If false, all generated FlowFiles will have the same content. "
                    + "When Unique FlowFiles is false, the first FlowFile in a batch is written and additional FlowFiles clone that content.")
            .required(true)
            .allowableValues("true", "false")
            .defaultValue("false")
            .build();
    public static final PropertyDescriptor CUSTOM_TEXT = new PropertyDescriptor.Builder()
            .name("Custom Text")
            .description("If Data Format is text and if Unique FlowFiles is false, then this custom text will be used as content of the generated "
                    + "FlowFiles and the File Size will be ignored. Finally, if Expression Language is used, evaluation will be performed only once "
                    + "per batch of generated FlowFiles")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor CHARSET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("Specifies the character set to use when writing the bytes of Custom Text to a FlowFile.")
            .required(true)
            .defaultValue("UTF-8")
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .build();
    public static final PropertyDescriptor MIME_TYPE = new PropertyDescriptor.Builder()
            .name("Mime Type")
            .description("Specifies the value to set for the \"mime.type\" attribute.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            FILE_SIZE,
            BATCH_SIZE,
            DATA_FORMAT,
            UNIQUE_FLOWFILES,
            CUSTOM_TEXT,
            CHARSET,
            MIME_TYPE
    );

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(
            SUCCESS
    );

    private static final char[] TEXT_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()-_=+/?.,';:\"?<>\n\t ".toCharArray();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .required(false)
            .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
            .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .dynamic(true)
            .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        if (context.getProperty(UNIQUE_FLOWFILES).asBoolean() || context.getProperty(CUSTOM_TEXT).isSet()) {
            generatedData.set(null);
        } else {
            generatedData.set(getGeneratedData(context));
        }
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(1);
        final boolean isUnique = validationContext.getProperty(UNIQUE_FLOWFILES).asBoolean();
        final boolean isText = validationContext.getProperty(DATA_FORMAT).getValue().equals(DATA_FORMAT_TEXT);
        final boolean isCustom = validationContext.getProperty(CUSTOM_TEXT).isSet();

        if (isCustom && (isUnique || !isText)) {
            results.add(new ValidationResult.Builder().subject("Custom Text").valid(false).explanation("If Custom Text is set, then Data Format must be "
                    + "text and Unique FlowFiles must be false.").build());
        }

        return results;
    }

    private GeneratedData getGeneratedData(final ProcessContext context) {
        final long byteCount = context.getProperty(FILE_SIZE).evaluateAttributeExpressions().asDataSize(DataUnit.B).longValue();
        final boolean binary = context.getProperty(DATA_FORMAT).getValue().equals(DATA_FORMAT_BINARY);
        return new GeneratedData(byteCount, binary, new Random().nextLong());
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final boolean uniqueData = context.getProperty(UNIQUE_FLOWFILES).asBoolean();
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();
        final Map<String, String> generatedAttributes = getGeneratedAttributes(context);

        if (uniqueData) {
            for (int i = 0; i < batchSize; i++) {
                session.transfer(createGeneratedFlowFile(session, getGeneratedData(context), generatedAttributes), SUCCESS);
            }
            return;
        }

        final FlowFile first;
        if (context.getProperty(CUSTOM_TEXT).isSet()) {
            first = createCustomTextFlowFile(context, session, generatedAttributes);
        } else {
            first = createGeneratedFlowFile(session, generatedData.get(), generatedAttributes);
        }

        for (int i = 1; i < batchSize; i++) {
            session.transfer(session.clone(first), SUCCESS);
        }
        session.transfer(first, SUCCESS);
    }

    private Map<String, String> getGeneratedAttributes(final ProcessContext context) {
        final Map<String, String> generatedAttributes = new HashMap<>();
        for (final Map.Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            final PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                generatedAttributes.put(property.getName(), context.getProperty(property).evaluateAttributeExpressions().getValue());
            }
        }

        if (context.getProperty(MIME_TYPE).isSet()) {
            generatedAttributes.put(CoreAttributes.MIME_TYPE.key(), context.getProperty(MIME_TYPE).getValue());
        }

        return generatedAttributes;
    }

    private FlowFile createCustomTextFlowFile(final ProcessContext context, final ProcessSession session, final Map<String, String> generatedAttributes) {
        final Charset charset = Charset.forName(context.getProperty(CHARSET).getValue());
        final byte[] customData = context.getProperty(CUSTOM_TEXT).evaluateAttributeExpressions().getValue().getBytes(charset);
        FlowFile flowFile = session.create();
        if (customData.length > 0) {
            flowFile = session.write(flowFile, out -> out.write(customData));
        }
        return finishCreatedFlowFile(session, flowFile, generatedAttributes);
    }

    private FlowFile createGeneratedFlowFile(final ProcessSession session, final GeneratedData data, final Map<String, String> generatedAttributes) {
        FlowFile flowFile = session.create();
        if (data.byteCount() > 0) {
            flowFile = session.write(flowFile, out -> writeGeneratedData(out, data));
        }
        return finishCreatedFlowFile(session, flowFile, generatedAttributes);
    }

    private FlowFile finishCreatedFlowFile(final ProcessSession session, FlowFile flowFile, final Map<String, String> generatedAttributes) {
        flowFile = session.putAllAttributes(flowFile, generatedAttributes);
        session.getProvenanceReporter().create(flowFile);
        return flowFile;
    }

    private static void writeGeneratedData(final OutputStream outputStream, final GeneratedData generatedData) throws IOException {
        final Random random = new Random(generatedData.seed());
        final byte[] buffer = new byte[(int) Math.min(BUFFER_SIZE, generatedData.byteCount())];
        long remaining = generatedData.byteCount();

        while (remaining > 0) {
            final int length = (int) Math.min(buffer.length, remaining);
            if (generatedData.binary()) {
                random.nextBytes(buffer);
            } else {
                for (int i = 0; i < length; i++) {
                    buffer[i] = (byte) TEXT_CHARS[random.nextInt(TEXT_CHARS.length)];
                }
            }
            outputStream.write(buffer, 0, length);
            remaining -= length;
        }
    }

    private record GeneratedData(long byteCount, boolean binary, long seed) {
    }

    @Override
    public void migrateProperties(PropertyConfiguration config) {
        config.renameProperty("generate-ff-custom-text", CUSTOM_TEXT.getName());
        config.renameProperty("character-set", CHARSET.getName());
        config.renameProperty("mime-type", MIME_TYPE.getName());
    }
}
