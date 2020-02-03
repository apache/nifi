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
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.bouncycastle.openpgp.PGPPublicKey;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;


/**
 * The VerifyPGP processor attempts to verify a flow file signature when triggered.  The processor uses a
 * {@link PGPKeyMaterialControllerService} to provide verification keys.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"verify", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Verifies a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class VerifyPGP extends AbstractProcessorPGP {
    public static final PropertyDescriptor PGP_KEY_SERVICE =
            AbstractProcessorPGP.buildKeyServiceProperty("PGP Key Material Controller Service that provides the public key for verification.");

    public static final PropertyDescriptor SIGNATURE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("signature-attribute")
            .displayName("Signature Attribute")
            .description("The name of the FlowFile Attribute that should have the signature to verify.")
            .defaultValue(AbstractProcessorPGP.DEFAULT_SIGNATURE_ATTRIBUTE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final ExtendedStreamCallback callback;

        try {
            callback = new VerifyStreamCallback(buildVerifySession(context, flowFile));
        } catch (DecoderException e) {
            logger.error("Exception constructing stream callback", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            final StopWatch stopWatch = new StopWatch(true);
            final FlowFile finalFlow = session.write(flowFile, callback);
            callback.postProcess(session, finalFlow);
            logger.debug("Called to verify flow {}", new Object[]{flowFile});
            session.getProvenanceReporter().modifyContent(finalFlow, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(finalFlow, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Exception in verify flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PGP_KEY_SERVICE);
        properties.add(SIGNATURE_ATTRIBUTE);
        return properties;
    }

    private VerifyStreamSession buildVerifySession(ProcessContext context, FlowFile flowFile) throws DecoderException {
        final PGPKeyMaterialService service = context.getProperty(PGP_KEY_SERVICE).asControllerService(PGPKeyMaterialService.class);
        final PGPPublicKey publicKey = service.getPublicKey();
        InputStream signature = getSignature(context, flowFile);
        return new VerifyStreamSession(getLogger(), publicKey, signature);
    }

    private InputStream getSignature(ProcessContext context, FlowFile flowFile) throws DecoderException {
        String attribute = context.getProperty(SIGNATURE_ATTRIBUTE).getValue();
        String signature = flowFile.getAttribute(attribute);
        return new ByteArrayInputStream(Hex.decodeHex(signature));
    }
}

