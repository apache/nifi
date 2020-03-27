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
package org.apache.nifi.pgp.processors;

import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.pgp.controllerservices.PGPService;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.bouncycastle.openpgp.PGPException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The SignContentAttributePGPProcessor processor attempts to create a signature of flow file contents when triggered.  The processor uses a
 * {@link PGPControllerService} to provide signature keys.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sign", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Signs a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class SignContentAttributePGPProcessor extends AbstractPGPProcessor {
    public static final PropertyDescriptor PGP_KEY_SERVICE =
            AbstractPGPProcessor.buildControllerServiceProperty("PGP Key Material Controller Service that provides the private key for signing.");

    public static final PropertyDescriptor SIGNATURE_HASH_ALGORITHM = new PropertyDescriptor.Builder()
            .name("signature-hash-algorithm")
            .displayName("Signature Hash Function")
            .description("The hash function used when signing data.")
            .allowableValues(getSignatureHashAllowableValues())
            .defaultValue(getSignatureHashDefaultValue())
            .build();

    public static final PropertyDescriptor SIGNATURE_ATTRIBUTE = new PropertyDescriptor.Builder()
            .name("signature-attribute")
            .displayName("Signature Attribute")
            .description("The name of the FlowFile Attribute for the signature.")
            .defaultValue(AbstractPGPProcessor.DEFAULT_SIGNATURE_ATTRIBUTE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static String getSignatureHashDefaultValue() {
        return String.valueOf(org.bouncycastle.openpgp.PGPUtil.SHA256);
    }

    // Values match integer values in org.bouncycastle.bcpg.HashAlgorithmTags
    private static AllowableValue[] getSignatureHashAllowableValues() {
        return new AllowableValue[]{
                new AllowableValue("1", "MD5"),
                new AllowableValue("2", "SHA1"),
                new AllowableValue("6", "TIGER 192"),
                new AllowableValue("8", "SHA 256"),
                new AllowableValue("9", "SHA 384"),
                new AllowableValue("10", "SHA 512"),
        };
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final PGPService service = context.getProperty(PGP_KEY_SERVICE).asControllerService(PGPService.class);
        final ByteArrayOutputStream signature = new ByteArrayOutputStream();
        final StopWatch stopWatch = new StopWatch(true);

        try {
            service.sign(session.read(flowFile), signature, service.optionsForSign(context.getProperty(SIGNATURE_HASH_ALGORITHM).asInteger()));
            session.putAttribute(flowFile, context.getProperty(SIGNATURE_ATTRIBUTE).getValue(), Hex.encodeHexString(signature.toByteArray()));
            long elapsed = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            getLogger().debug("Called to sign flow {} completed in {}ms", new Object[]{flowFile, elapsed});
            session.getProvenanceReporter().modifyAttributes(flowFile, elapsed);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException | PGPException | IOException e) {
            getLogger().debug("Exception in sign flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(PGP_KEY_SERVICE, SIGNATURE_ATTRIBUTE, SIGNATURE_HASH_ALGORITHM));
    }
}
