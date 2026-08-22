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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.documentation.UseCase;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"base64", "detect", "identify", "encoding", "content"})
@CapabilityDescription("""
        Checks whether FlowFile content is Base64 encoded and writes either true or false to a configurable attribute. The content is inspected rather \
        than decoded, so content that is not Base64 is reported instead of causing a decoding failure. Every FlowFile is routed to success, allowing \
        downstream processors such as RouteOnAttribute to branch on the attribute.""")
@WritesAttribute(attribute = "<Base64 Attribute Name>",
        description = "Set to true when the evaluated content is Base64 encoded, otherwise set to false. The attribute name is configured using the "
                + "Base64 Attribute Name property")
@UseCase(
        description = "Route FlowFiles based on whether the content is Base64 encoded",
        configuration = """
                Leave "Base64 Attribute Name" set to the default of "content.base64", or set it to the attribute name of your choosing.
                Set "Detection Scope" to "Entire Content" when the result must be certain, or to "Sample" for a faster check on large content.

                Connect the "success" relationship to a RouteOnAttribute processor and add a property such as "base64" with the value \
                "${content.base64:equals('true')}" to send Base64 encoded content down its own branch.
                """
)
public class DetectBase64Content extends AbstractProcessor {

    public static final PropertyDescriptor BASE64_ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
            .name("Base64 Attribute Name")
            .description("Name of the FlowFile attribute to which the detection result of either true or false will be written.")
            .required(true)
            .defaultValue("content.base64")
            .addValidator(StandardValidators.ATTRIBUTE_KEY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .build();

    public static final PropertyDescriptor DETECTION_SCOPE = new PropertyDescriptor.Builder()
            .name("Detection Scope")
            .description("Amount of FlowFile content to evaluate when determining whether the content is Base64 encoded.")
            .required(true)
            .allowableValues(DetectionScope.class)
            .defaultValue(DetectionScope.ENTIRE_CONTENT)
            .build();

    public static final PropertyDescriptor SAMPLE_SIZE = new PropertyDescriptor.Builder()
            .name("Sample Size")
            .description("Maximum number of bytes read from the beginning of the FlowFile content when Detection Scope is set to Sample.")
            .required(true)
            .defaultValue("4 KB")
            .addValidator(StandardValidators.createDataSizeBoundsValidator(1, Long.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .dependsOn(DETECTION_SCOPE, DetectionScope.SAMPLE)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            BASE64_ATTRIBUTE_NAME,
            DETECTION_SCOPE,
            SAMPLE_SIZE
    );

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Every FlowFile is routed to success after the detection result has been written to the configured attribute")
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(REL_SUCCESS);

    private static final int BUFFER_SIZE = 8192;

    /** Number of Base64 characters that encode three bytes of input */
    private static final int ENCODING_QUANTUM = 4;

    private static final int MAXIMUM_PADDING_CHARACTERS = 2;

    private static final byte PADDING = '=';

    private static final byte CARRIAGE_RETURN = '\r';

    private static final byte LINE_FEED = '\n';

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final String attributeName = context.getProperty(BASE64_ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final long scanLimit = getScanLimit(context, flowFile);

        final boolean base64Encoded;
        try (InputStream inputStream = session.read(flowFile)) {
            base64Encoded = isBase64Encoded(inputStream, scanLimit);
        } catch (final IOException e) {
            throw new ProcessException("Base64 detection failed reading content of %s".formatted(flowFile), e);
        }

        getLogger().debug("Base64 detection completed with result [{}] for {}", base64Encoded, flowFile);

        flowFile = session.putAttribute(flowFile, attributeName, Boolean.toString(base64Encoded));

        session.getProvenanceReporter().modifyAttributes(flowFile);
        session.transfer(flowFile, REL_SUCCESS);
    }

    private long getScanLimit(final ProcessContext context, final FlowFile flowFile) {
        final DetectionScope detectionScope = context.getProperty(DETECTION_SCOPE).asAllowableValue(DetectionScope.class);
        if (detectionScope == DetectionScope.ENTIRE_CONTENT) {
            return Long.MAX_VALUE;
        }
        return context.getProperty(SAMPLE_SIZE).evaluateAttributeExpressions(flowFile).asDataSize(DataUnit.B).longValue();
    }

    /**
     * Evaluate content for Base64 encoding without decoding, reading no more than the requested number of bytes.
     *
     * @param inputStream Content to be evaluated
     * @param scanLimit Maximum number of bytes to read
     * @return Detection result
     * @throws IOException Thrown on failures reading content
     */
    private static boolean isBase64Encoded(final InputStream inputStream, final long scanLimit) throws IOException {
        final byte[] buffer = new byte[BUFFER_SIZE];
        long scanned = 0;
        long alphabetCharacters = 0;
        int paddingCharacters = 0;
        boolean contentCompleted = false;

        while (scanned < scanLimit) {
            final int requested = (int) Math.min(buffer.length, scanLimit - scanned);
            final int read = inputStream.read(buffer, 0, requested);
            if (read == -1) {
                contentCompleted = true;
                break;
            }
            scanned += read;

            for (int i = 0; i < read; i++) {
                final byte character = buffer[i];

                if (character == CARRIAGE_RETURN || character == LINE_FEED) {
                    continue;
                }

                if (character == PADDING) {
                    paddingCharacters++;
                    if (paddingCharacters > MAXIMUM_PADDING_CHARACTERS) {
                        return false;
                    }
                } else if (isAlphabetCharacter(character)) {
                    // Base64 characters cannot follow padding characters
                    if (paddingCharacters > 0) {
                        return false;
                    }
                    alphabetCharacters++;
                } else {
                    return false;
                }
            }
        }

        if (alphabetCharacters == 0) {
            return false;
        }

        if (!contentCompleted) {
            // The scan limit was reached before the end of the content, so read one more byte to determine whether content remains
            contentCompleted = inputStream.read() == -1;
        }

        // The number of characters can only be evaluated when the entire content has been read
        return !contentCompleted || (alphabetCharacters + paddingCharacters) % ENCODING_QUANTUM == 0;
    }

    private static boolean isAlphabetCharacter(final byte character) {
        return (character >= 'A' && character <= 'Z')
                || (character >= 'a' && character <= 'z')
                || (character >= '0' && character <= '9')
                || character == '+'
                || character == '/';
    }

    enum DetectionScope implements DescribedValue {
        SAMPLE(
                "Sample",
                "Evaluates the number of bytes configured in Sample Size read from the beginning of the content. This avoids reading large content in "
                        + "full, but content that is not Base64 encoded beyond the sample will not be detected."
        ),
        ENTIRE_CONTENT(
                "Entire Content",
                "Evaluates the entire content, streamed instead of buffered in memory. This provides a certain result at the cost of reading all content."
        );

        private final String value;

        private final String description;

        DetectionScope(final String value, final String description) {
            this.value = value;
            this.description = description;
        }

        @Override
        public String getValue() {
            return value;
        }

        @Override
        public String getDisplayName() {
            return value;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }
}
