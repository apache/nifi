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
package org.apache.nifi.processors.standard.pgp;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.bouncycastle.openpgp.PGPException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * The VerifyContentAttributePGPProcessor processor attempts to verify a flow file signature when triggered.  The processor uses a
 * {@link PGPControllerService} to provide verification keys.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"verify", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Verifies a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class VerifyContentAttributePGPProcessor extends AbstractPGPProcessor {
    public static final PropertyDescriptor PGP_KEY_SERVICE =
            AbstractPGPProcessor.buildControllerServiceProperty("PGP Key Material Controller Service that provides the public key for verification.");

    public static final PropertyDescriptor SIGNATURE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("signature-attribute")
            .displayName("Signature Attribute")
            .description("The name of the FlowFile Attribute that should have the signature to verify.")
            .defaultValue(AbstractPGPProcessor.DEFAULT_SIGNATURE_ATTRIBUTE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final PGPService service = context.getProperty(PGP_KEY_SERVICE).asControllerService(PGPService.class);
        final StopWatch stopWatch = new StopWatch(true);

        try {
            if (!service.verify(session.read(flowFile), getSignature(context, flowFile), service.optionsForVerify())) {
                throw new ProcessException("FlowFile unverified.");
            }
            long elapsed = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            getLogger().debug("Called to verify flow {} completed in {}ms", new Object[]{flowFile, elapsed});
            session.getProvenanceReporter().modifyAttributes(flowFile, elapsed);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException | DecoderException | PGPException | IOException e) {
            getLogger().debug("Exception in verify flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(PGP_KEY_SERVICE, SIGNATURE_ATTRIBUTE));
    }

    private InputStream getSignature(ProcessContext context, FlowFile flowFile) throws DecoderException {
        String attribute = context.getProperty(SIGNATURE_ATTRIBUTE).getValue();
        String signature = flowFile.getAttribute(attribute);
        return new ByteArrayInputStream(Hex.decodeHex(signature));
    }
}

