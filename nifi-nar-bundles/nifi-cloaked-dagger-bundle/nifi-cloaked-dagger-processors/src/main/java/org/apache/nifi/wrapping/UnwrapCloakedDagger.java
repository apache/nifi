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
import org.apache.nifi.wrapping.unwrapper.Unwrapper;

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
 * This processor provides access to the unwrapping algorithm supplied by the Inspection Pit team.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@SupportsBatching
@Tags({ "unwrap", "cloaked", "dagger", "technique", "mask", "checksum" })
@CapabilityDescription("Unwraps potentially malicious content using the Cloaked Dagger technique to prevent either accidental or deliberate processing of that content on unintended systems."
        + " It is often necessary to transport or store potentially malicious content through a system ensuring that the content is not processed by anything other than the intended destination."
        + " Unintentional processing includes both accidental and deliberate user activity, and parsing by system processes (such as indexers and anti-virus software)"
        + " Trivial solutions to this problem exist in simple obfuscations of the data; e.g. bit-shifting or bit-flipping,"
        + " however if this is performed in a deterministic way then an attacker could craft his data to ensure that it is unmasked by the unwrapping process."
        + " It is therefore evident that some kind of unpredictability needs to be introduced into the system.")
@SeeAlso(classNames={"glib.app.fc.wrapping.nifi.WrapCloakedDagger"})
public class UnwrapCloakedDagger extends AbstractProcessor {

    /**
     * The properties.
     */

    // Properties would normally be declared here but there are none for this processor

    /**
     * The relationships.
     */

    /**
     * The Constant SUCCESS_RELATIONSHIP. If the processor worked it will pass the flow file via this relationship
     */
    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder().name("success")
                    .description("If the unwrapping succeeds it will pass the flow file via this relationship.")
                    .build();

    /**
     * The relationship ERROR_FAILURE.
     */
    public static final Relationship ERROR_RELATIONSHIP = new Relationship.Builder().name("error")
                    .description("If the unwrapping fails then it will pass the flow file via this relationship")
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
    @Override
    protected void init(final ProcessorInitializationContext context) {

        // Create property list and add the properties
        final List<PropertyDescriptor> descriptor = new ArrayList<PropertyDescriptor>();
        // No Properties here.
        this.properties = Collections.unmodifiableList(descriptor);

        // Create relationship set and add relations
        final Set<Relationship> relationships = new HashSet<Relationship>();
        // Normally properties would be added to the Hashset here
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

        final StreamCallback safeUnwrapperCallback = new Technique2UnwrapperProcessorCallback();

        try {
            flowFile = session.write(flowFile, safeUnwrapperCallback);
            session.getProvenanceReporter().modifyContent(flowFile);
            session.transfer(flowFile, SUCCESS_RELATIONSHIP);
        } catch (Throwable ex) {
            getLogger().error(this + " unable to process " + flowFile + " due to " + ex.getLocalizedMessage(), ex);
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, ERROR_RELATIONSHIP);
        }
    }

    /**
     * Unwraps the supplied file.
     */
    public static class Technique2UnwrapperProcessorCallback implements StreamCallback {

        /*
         * (non-Javadoc)
         *
         * @see org.apache.nifi.processor.io.StreamCallback#process(java.io.InputStream, java.io.OutputStream)
         */
        @Override
        public void process(InputStream in, OutputStream os) throws IOException {
            // Make sure that we wrap the input/output streams in a buffer to speed things up.
            try (BufferedInputStream bufferedInputStream = new BufferedInputStream(in);
                            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(os)) {

                // Set up the unwrapper.
                final Unwrapper unwrapper = new Unwrapper(bufferedInputStream, bufferedOutputStream);

                // Unwrap the data.
                final byte[] buffer = new byte[4096];

                int got = unwrapper.read(buffer);
                while (got != -1) {
                    got = unwrapper.read(buffer);
                }
                unwrapper.close();
            } // End Try-Release
        }
    }
}
