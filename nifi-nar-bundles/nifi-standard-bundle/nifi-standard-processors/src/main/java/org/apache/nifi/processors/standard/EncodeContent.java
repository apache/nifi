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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base32InputStream;
import org.apache.commons.codec.binary.Base32OutputStream;
import org.apache.commons.codec.binary.Base64InputStream;
import org.apache.commons.codec.binary.Base64OutputStream;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.standard.util.ValidatingBase32InputStream;
import org.apache.nifi.processors.standard.util.ValidatingBase64InputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"encode", "decode", "base64", "base32", "hex"})
@CapabilityDescription("Encode or decode the contents of a FlowFile using Base64, Base32, or hex encoding schemes")
public class EncodeContent extends AbstractProcessor {

    public static final AllowableValue ENCODE_MODE = new AllowableValue("encode", "Encode", "Sets the operation mode to 'encode'.");
    public static final AllowableValue DECODE_MODE = new AllowableValue("decode", "Decode", "Sets the operation mode to 'decoding'.");

    public static final AllowableValue BASE64_ENCODING = new AllowableValue("base64", "Base64", "Sets the encoding type to 'Base64'.");
    public static final AllowableValue BASE32_ENCODING = new AllowableValue("base32", "Base32", "Sets the encoding type to 'Base32'.");
    public static final AllowableValue HEX_ENCODING = new AllowableValue("hex", "Hexadecimal", "Sets the encoding type to 'Hexadecimal'.");

    /**
     * Represents an allowable value that indicates the encoded FlowFile content should be output as a single line.
     * When this value is set, the encoded content will not contain any line breaks or newlines, ensuring
     * that the entire content is presented in a single, continuous line.
     * <p>
     * This value is particularly useful in scenarios where multiline output is not desired or could lead to issues,
     * such as in certain data formats or when interfacing with systems that expect single-line input.
     * </p>
     */
    static final AllowableValue SINGLE_LINE_OUTPUT_TRUE = new AllowableValue("true", "True", "The encoded FlowFile content will be output as a single line.");


    /**
     * Represents an allowable value that indicates the encoded FlowFile content should be output as multiple lines.
     * When this value is set, the encoded content will include appropriate line breaks or newlines, breaking
     * the content into separate lines as required.
     * <p>
     * This setting is useful in scenarios where multi-line formatting is necessary or preferred, such as for
     * improved readability, adherence to specific data formats, or compatibility with systems and tools that
     * process multi-line input.
     * </p>
     */
    static final AllowableValue SINGLE_LINE_OUTPUT_FALSE = new AllowableValue("false", "False", "The encoded FlowFile content will be output as a multiple lines.");

    public static final PropertyDescriptor MODE = new PropertyDescriptor.Builder()
            .name("Mode")
            .description("Specifies whether the content should be encoded or decoded")
            .required(true)
            .allowableValues(ENCODE_MODE, DECODE_MODE)
            .defaultValue(ENCODE_MODE.getValue())
            .build();

    public static final PropertyDescriptor ENCODING = new PropertyDescriptor.Builder()
            .name("Encoding")
            .description("Specifies the type of encoding used")
            .required(true)
            .allowableValues(BASE64_ENCODING, BASE32_ENCODING, HEX_ENCODING)
            .defaultValue(BASE64_ENCODING.getValue())
            .build();

    // A Boolean property descriptor that allows the user
    // to choose if the output to encoded FlowFile content should be to a single line or multiple lines.
    static final PropertyDescriptor SINGLE_LINE_OUTPUT = new PropertyDescriptor.Builder()
            .name("single-line-output")
            .displayName("Output Content to Single Line")
            .description("If set to 'true', the encoded FlowFile content will output as a single line. If set to 'false', "
                + "it will output as multiple lines. This property is only applicable when Base64 or Base32 encoding is selected.")
            .required(false)
            .defaultValue("false")
            .allowableValues(SINGLE_LINE_OUTPUT_TRUE, SINGLE_LINE_OUTPUT_FALSE)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .dependsOn(MODE, ENCODE_MODE)
            .dependsOn(ENCODING, BASE64_ENCODING, BASE32_ENCODING)
            .build();

    static final PropertyDescriptor ENCODED_LINE_SEPARATOR = new PropertyDescriptor.Builder()
        .name("line-separator")
        .displayName("Encoded Content Line Separator")
        .description("Each line of encoded data will be terminated with this byte sequence (e.g. \\r\\n"
                + "). This property defaults to the system-dependent line separator string.  If `line-length` <= 0, "
                + "the `line-separator` property is not used. This property is not used for `hex` encoding.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .defaultValue(System.lineSeparator())
        .addValidator(Validator.VALID)
        .dependsOn(MODE, ENCODE_MODE)
        .dependsOn(ENCODING, BASE64_ENCODING, BASE32_ENCODING)
        .build();

    static final PropertyDescriptor ENCODED_LINE_LENGTH = new PropertyDescriptor.Builder()
        .name("encoded-line-length")
        .displayName("Encoded Content Line Length")
        .description("Each line of encoded data will contain `encoded-line-length` characters (rounded down to the nearest multiple of 4). "
            + "If `encoded-line-length` <= 0, the encoded data is not divided into lines. This property is "
            + "ignored if `single-line-output` is set to True.")
        .required(false)
        .defaultValue("76")
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(StandardValidators.INTEGER_VALIDATOR)
        .dependsOn(MODE, ENCODE_MODE)
        .dependsOn(ENCODING, BASE64_ENCODING, BASE32_ENCODING)
        .dependsOn(SINGLE_LINE_OUTPUT, SINGLE_LINE_OUTPUT_FALSE)
        .build();

    static final PropertyDescriptor FAIL_ON_ZERO_LENGTH_CONTENT = new PropertyDescriptor.Builder()
            .name("fail-on-zero-length-content")
            .displayName("Fail On 0-Length Content")
            .description("If set to 'true', FlowFiles with 0-length (zero) content will be routed to 'failure', else they will be routed to 'success'.")
            .required(true)
            .defaultValue("false")
            .allowableValues("true", "false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Any FlowFile that is successfully encoded or decoded will be routed to success")
            .build();
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be encoded or decoded will be routed to failure")
            .build();

    private static final int BUFFER_SIZE = 8192;

    private static final List<PropertyDescriptor> properties = List.of(MODE,
        ENCODING, SINGLE_LINE_OUTPUT, ENCODED_LINE_SEPARATOR, ENCODED_LINE_LENGTH, FAIL_ON_ZERO_LENGTH_CONTENT);

    private static final Set<Relationship> relationships = Set.of(REL_SUCCESS,
        REL_FAILURE);

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        // Decide if we should fail on 0-length content
        final Boolean failOnZeroLengthContent = context.getProperty(FAIL_ON_ZERO_LENGTH_CONTENT).asBoolean();

        final boolean encode = context.getProperty(MODE).getValue().equalsIgnoreCase(ENCODE_MODE.getValue());
        final String encoding = context.getProperty(ENCODING).getValue();

        final Boolean singleLineOutput = context.getProperty(SINGLE_LINE_OUTPUT).asBoolean();
        final Integer lineLength = context.getProperty(ENCODED_LINE_LENGTH).evaluateAttributeExpressions(flowFile).asInteger();
        final String lineSeparator = context.getProperty(ENCODED_LINE_SEPARATOR).evaluateAttributeExpressions(flowFile).getValue();

        final StreamCallback callback = getStreamCallback(encode, encoding, Boolean.TRUE.equals(singleLineOutput) ? -1 : lineLength, lineSeparator);

        try {
            // If flowFile content is null, transfer based on FAIL_ON_ZERO_LENGTH_CONTENT property
            if (flowFile.getSize() == 0) {
                if (Boolean.TRUE.equals(failOnZeroLengthContent)) session.transfer(flowFile, REL_FAILURE);
                else session.transfer(flowFile, REL_SUCCESS);

                return;
            }

            final StopWatch stopWatch = new StopWatch(true);
            flowFile = session.write(flowFile, callback);

            getLogger().info("{} completed {}", encode ? "Encoding" : "Decoding", flowFile);
            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final Exception e) {
            getLogger().error("{} failed {}", encode ? "Encoding" : "Decoding", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    private static StreamCallback getStreamCallback(final Boolean encode, final String encoding,
        final Integer lineLength, final String lineSeparator) {
        if (Boolean.TRUE.equals(encode)) {
            if (encoding.equalsIgnoreCase(BASE64_ENCODING.getValue())) {
                return new EncodeBase64(lineLength, lineSeparator);
            } else if (encoding.equalsIgnoreCase(BASE32_ENCODING.getValue())) {
                return new EncodeBase32(lineLength, lineSeparator);
            } else {
                return new EncodeHex();
            }
        } else {
            if (encoding.equalsIgnoreCase(BASE64_ENCODING.getValue())) {
                return new DecodeBase64();
            } else if (encoding.equalsIgnoreCase(BASE32_ENCODING.getValue())) {
                return new DecodeBase32();
            } else {
                return new DecodeHex();
            }
        }
    }

    private static class EncodeBase64 implements StreamCallback {

        private int lineLength;
        private String lineSeparator;

        public EncodeBase64(final Integer lineLength,
            final String lineSeparator) {
            this.lineLength = lineLength;
            this.lineSeparator = lineSeparator;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base64OutputStream bos = new Base64OutputStream(out,
                true,
                this.lineLength,
                this.lineSeparator.getBytes())) {
                StreamUtils.copy(in, bos);
            }
        }
    }

    private static class DecodeBase64 implements StreamCallback {

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base64InputStream bis = new Base64InputStream(new ValidatingBase64InputStream(in))) {
                StreamUtils.copy(bis, out);
            }
        }
    }

    private static class EncodeBase32 implements StreamCallback {

        private int lineLength;
        private String lineSeparator;

        public EncodeBase32(final Integer lineLength,
            final String lineSeparator) {

            this.lineLength = lineLength;
            this.lineSeparator = lineSeparator;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base32OutputStream bos = new Base32OutputStream(out,
                true,
                this.lineLength,
                this.lineSeparator.getBytes())) {
                StreamUtils.copy(in, bos);
            }
        }
    }

    private static class DecodeBase32 implements StreamCallback {

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            try (Base32InputStream bis = new Base32InputStream(new ValidatingBase32InputStream(in))) {
                StreamUtils.copy(bis, out);
            }
        }
    }

    private static class EncodeHex implements StreamCallback {

        private static final byte[] HEX_CHARS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            int len;
            byte[] inBuf = new byte[8192];
            byte[] outBuf = new byte[inBuf.length * 2];
            while ((len = in.read(inBuf)) > 0) {
                for (int i = 0; i < len; i++) {
                    outBuf[i * 2] = HEX_CHARS[(inBuf[i] & 0xF0) >>> 4];
                    outBuf[i * 2 + 1] = HEX_CHARS[inBuf[i] & 0x0F];
                }
                out.write(outBuf, 0, len * 2);
            }
            out.flush();
        }
    }

    private static class DecodeHex implements StreamCallback {

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            int len;
            byte[] inBuf = new byte[BUFFER_SIZE];
            Hex h = new Hex();
            while ((len = in.read(inBuf)) > 0) {
                // If the input buffer is of odd length, try to get another byte
                if (len % 2 != 0) {
                    int b = in.read();
                    if (b != -1) {
                        inBuf[len] = (byte) b;
                        len++;
                    }
                }

                // Construct a new buffer bounded to len
                byte[] slice = Arrays.copyOfRange(inBuf, 0, len);
                try {
                    out.write(h.decode(slice));
                } catch (final DecoderException e) {
                    throw new IOException("Hexadecimal decoding failed", e);
                }
            }
            out.flush();
        }
    }
}
