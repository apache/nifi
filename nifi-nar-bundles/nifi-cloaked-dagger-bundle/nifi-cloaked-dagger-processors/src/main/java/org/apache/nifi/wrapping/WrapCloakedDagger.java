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
package org.apache.nifi.wrapping;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.wrapping.wrapper.Wrapper;
import org.apache.nifi.wrapping.wrapper.WrapperFactory;
import org.apache.nifi.wrapping.wrapper.WrapperProperties;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.CheckSum;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.MaskLength;
import org.apache.nifi.wrapping.wrapper.WrapperFactory.TechniqueType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

//CHECKSTYLE.ON: CustomImportOrderCheck

/**
 * This processor provides access to the wrapping algorithm supplied by the Inspection Pit team.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({ "wrap", "cloaked", "dagger", "technique", "mask", "checksum" })
@CapabilityDescription("Wraps potentially malicious content using the Cloaked Dagger technique to prevent either accidental or deliberate processing of that content on unintended systems."
        + " It is often necessary to transport or store potentially malicious content through a system ensuring that the content is not processed by anything other than the intended destination."
        + " Unintentional processing includes both accidental and deliberate user activity, and parsing by system processes (such as indexers and anti-virus software)"
        + " Trivial solutions to this problem exist in simple obfuscations of the data; e.g. bit-shifting or bit-flipping,"
        + " however if this is performed in a deterministic way then an attacker could craft his data to ensure that it is unmasked by the unwrapping process."
        + " It is therefore evident that some kind of unpredictability needs to be introduced into the system."
        + " Cloaked Dagger allows for a range of encapsulation techniques that can be used to obfuscate the data."
        + " It should be noted that, despite the inclusion of cryptographic primitives it is not designed to provide Confidentiality, Availability or Integrity of the underlying data,"
        + " but of the system which is transporting or storing it. As such, further protections should be sought if transmitting wrapped content across untrusted bearers.")
@SeeAlso(classNames={"glib.app.fc.wrapping.nifi.UnwrapCloakedDagger"})
public class WrapCloakedDagger extends AbstractProcessor {

    /**
     * The properties.
     */

    /**
     * The Constant MASK_LENGTH_PROP. The MASK LENGTH of the encryption
     */
    public static final PropertyDescriptor MASK_LENGTH_PROP = new PropertyDescriptor.Builder()
                    .name("Mask Length")
                    .description("The Mask Length of the encryption - one of 16, 32, 64 or 128.")
                    .required(true)
                    .allowableValues(Integer.toString(MaskLength.MASK_16_BIT.intValue()),
                                    Integer.toString(MaskLength.MASK_32_BIT.intValue()),
                                    Integer.toString(MaskLength.MASK_64_BIT.intValue()),
                                    Integer.toString(MaskLength.MASK_128_BIT.intValue()))
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE).build();

    /**
     * The Constant TECH_TYPE_PROP. The technique type to use for the masking : Technique1 or Technique2
     */
    public static final PropertyDescriptor TECH_TYPE_PROP = new PropertyDescriptor.Builder()
                    .name("Technique Type")
                    .description("The technique type to use for the masking - Technique1 or Technique2")
                    .required(true)
                    .allowableValues(TechniqueType.TECHNIQUE_1.stringValue(), TechniqueType.TECHNIQUE_2.stringValue())
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE).addValidator(new Validator() {

                        /*
                         * (non-Javadoc)
                         *
                         * @see org.apache.nifi.components.Validator#validate(java.lang.String, java.lang.String,
                         * org.apache.nifi.components.ValidationContext)
                         */
                        @Override
                        public ValidationResult validate(String subject, String input, ValidationContext context) {
                            String maskLen = context.getProperty(MASK_LENGTH_PROP).getValue();
                            if ((!Integer.toString(MaskLength.MASK_128_BIT.intValue()).equals(maskLen))
                                            && (TechniqueType.TECHNIQUE_2.stringValue().equals(input))) {
                                // Technique2 only supports Mask Length of 128, so give a false result.
                                return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                                                .explanation("Technique2 only supports a Mask Length of 128").build();
                            }
                            // The input can only be one of the allowed values, so does not require further validation.
                            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
                        }
                    }).build();

    /**
     * The Constant HEADER_CS_PROP. The Header Checksum to be used for the masking e.g. CRC32, SHA256 or NONE
     */
    public static final PropertyDescriptor HEADER_CS_PROP = new PropertyDescriptor.Builder()
                    .name("Header Checksum Type")
                    .description("The Header checksum to be used for the masking e.g. CRC32, SHA256 or NONE")
                    .required(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .allowableValues(CheckSum.CRC_32_CHECKSUM.stringValue(), CheckSum.SHA_256_CHECKSUM.stringValue(),
                                    "NONE").build();

    /**
     * The Constant BODY_CS_PROP. The Body Checksum to be used for the masking e.g. CRC32, SHA256 or NONE
     */
    public static final PropertyDescriptor BODY_CS_PROP = new PropertyDescriptor.Builder()
                    .name("Body Checksum Type")
                    .description("The Body checksum to be used for the masking e.g. CRC32, SHA256 or NONE")
                    .required(true)
                    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
                    .allowableValues(CheckSum.CRC_32_CHECKSUM.stringValue(), CheckSum.SHA_256_CHECKSUM.stringValue(),
                                    "NONE").build();

    /**
     * The relationships.
     */

    /**
     * The Constant SUCCESS_RELATIONSHIP. If the processor worked it will pass the flow file via this relationship
     */
    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder().name("success")
                    .description("If the wrapping succeeds it will pass the flow file via this relationship.")
                    .build();

    /**
     * The relationship ERROR_FAILURE.
     */
    public static final Relationship ERROR_RELATIONSHIP = new Relationship.Builder().name("error")
                    .description("If the wrapping fails then it will pass the flow file via this relationship")
                    .build();

    /**
     * The properties.
     */
    private volatile List<PropertyDescriptor> properties;

    /**
     * The relationships.
     */
    private volatile Set<Relationship> relationships;

    /**
     * @see org.apache.nifi.components.AbstractConfigurableComponent#getSupportedPropertyDescriptors()
     *
     * @return The list of property descriptors.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.properties;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.nifi.processor.AbstractSessionFactoryProcessor#getRelationships()
     */
    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.nifi.processor.AbstractSessionFactoryProcessor#init(org.apache.nifi
     * .processor.ProcessorInitializationContext)
     */
    protected void init(final ProcessorInitializationContext context) {

        // Create property list and add the properties
        final List<PropertyDescriptor> descriptor = new ArrayList<PropertyDescriptor>();
        descriptor.add(BODY_CS_PROP);
        descriptor.add(HEADER_CS_PROP);
        descriptor.add(MASK_LENGTH_PROP);
        descriptor.add(TECH_TYPE_PROP);
        this.properties = Collections.unmodifiableList(descriptor);

        // Create relationship set and add relations
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS_RELATIONSHIP);
        relationships.add(ERROR_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    /**
     * Attempts to process a flowfile.
     *
     * @param context
     *            - The process context to allow access to processor instance.
     * @param session
     *            - The session (task) running this method - so provides access to Flow Files.
     * @throws ProcessException
     *             - Indicates an issue occurred while access flow file connection which can be standard IO exception or
     *             some issue that occurred in user code.
     * @see org.apache.nifi.processor.AbstractProcessor#onTrigger(org.apache.nifi.processor.ProcessContext,
     *      org.apache.nifi.processor.ProcessSession)
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return; // fail-fast if there is no work to do
        }

        // Get the values from the processor properties
        WrapperProperties wrappingProperties = getPropertyValues(context);

        final StreamCallback safeWrapperCallback = new WrappingProcessorCallback(flowFile, wrappingProperties);

        try {
            flowFile = session.write(flowFile, safeWrapperCallback);
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, SUCCESS_RELATIONSHIP);
        } catch (Throwable ex) {
            getLogger().error(this + " unable to process " + flowFile + " due to " + ex.getLocalizedMessage(), ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, ERROR_RELATIONSHIP);
        }
    }

    /**
     * Method to determine the set of Wrapping Properties to use for Wrapping.
     *
     * @param context
     *            - The process context to allow access to processor instance.
     * @return the set of wrapping properties in a class object.
     */
    private WrapperProperties getPropertyValues(final ProcessContext context) {
        final WrapperProperties wrappingProperties = new WrapperProperties();

        final String maskLengthProp = context.getProperty(MASK_LENGTH_PROP).getValue();
        for (MaskLength maskValue : MaskLength.values()) {
            if (Integer.parseInt(maskLengthProp) == maskValue.intValue()) {
                wrappingProperties.setFinalMaskLength(maskValue);
            }
        }

        final String techTypeProp = context.getProperty(TECH_TYPE_PROP).getValue();
        for (TechniqueType techniqueType : TechniqueType.values()) {
            if (techTypeProp.equals(techniqueType.stringValue())) {
                wrappingProperties.setFinalTechType(techniqueType);
            }
        }

        final String bodyCsProp = context.getProperty(BODY_CS_PROP).getValue();
        if ("NONE".equals(bodyCsProp)) {
            wrappingProperties.setFinalBodyCs(CheckSum.NO_CHECKSUM);
            wrappingProperties.setNoBodyCs(true);
        } else {
            for (CheckSum bodyCheckSum : CheckSum.values()) {
                if (bodyCsProp.equals(bodyCheckSum.stringValue())) {
                    wrappingProperties.setFinalBodyCs(bodyCheckSum);
                }
            }
        }

        final String headerCsProp = context.getProperty(HEADER_CS_PROP).getValue();
        if ("NONE".equals(headerCsProp)) {
            wrappingProperties.setFinalHeaderCs(CheckSum.NO_CHECKSUM);
            wrappingProperties.setNoHeaderCs(true);
        } else {
            for (CheckSum headerCheckSum : CheckSum.values()) {
                if (headerCsProp.equals(headerCheckSum.stringValue())) {
                    wrappingProperties.setFinalHeaderCs(headerCheckSum);
                }
            }
        }

        return wrappingProperties;
    }

    /**
     * This processor wraps the the algorithm so that it can be used within Nifi.
     */
    public static class WrappingProcessorCallback implements StreamCallback {
        final FlowFile flowFile;
        final WrapperProperties wrappingProperties;

        /**
         * Constructor.
         *
         * @param flowFile
         *            - to be processed.
         * @param wrappingProperties
         *            - set of properties to use for wrapping.
         */
        public WrappingProcessorCallback(FlowFile flowFile, WrapperProperties wrappingProperties) {

            this.flowFile = flowFile;
            this.wrappingProperties = wrappingProperties;

        }

        /*
         * Passes the input and output streams along with the file length to the @see ShiftAllProtector. (non-Javadoc)
         *
         * @see org.apache.nifi.processor.io.StreamCallback#process(java.io.InputStream, java.io.OutputStream)
         */
        @Override
        public void process(InputStream in, OutputStream os) throws IOException {
            // Make sure that we wrap the input/output streams in a buffer to speed things up.
            try (BufferedInputStream bufferedInputStream = new BufferedInputStream(in, 20480);
                            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(os, 20480)) {

                if (wrappingProperties.isNoHeaderCs()) {
                    wrappingProperties.setFinalHeaderCs(WrapperFactory.CheckSum.NO_CHECKSUM);
                }
                if (wrappingProperties.isNoBodyCs()) {
                    wrappingProperties.setFinalBodyCs(WrapperFactory.CheckSum.NO_CHECKSUM);
                }

                // Get the wrapper factory.
                final WrapperFactory wrapperFactory = new WrapperFactory();

                // Get the wrapper from the factory, providing technique type and stream to write to.
                final Wrapper wrapper = wrapperFactory.getWrapper(bufferedOutputStream,
                                wrappingProperties.getFinalMaskLength(), wrappingProperties.getFinalTechType(),
                                wrappingProperties.getFinalHeaderCs(), wrappingProperties.getFinalBodyCs());

                // Wrap the data.

                final byte[] buffer = new byte[4096];

                int got = bufferedInputStream.read(buffer);
                while (got > 0) {
                    if (got != buffer.length) {
                        wrapper.write(buffer, 0, got);
                    } else {
                        wrapper.write(buffer);
                    }
                    got = bufferedInputStream.read(buffer);
                }
                wrapper.close();
            } // End Try-Release
        }
    }
}
