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

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
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
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The SignPGP processor attempts to create a signature of flow file contents when triggered.  The processor uses a
 * {@link PGPKeyMaterialControllerService} to provide signature keys.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"sign", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Signs a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class SignPGP extends AbstractProcessorPGP {
    public static final PropertyDescriptor PGP_KEY_SERVICE =
            AbstractProcessorPGP.buildKeyServiceProperty("PGP Key Material Controller Service that provides the private key for signing.");

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
            .defaultValue(AbstractProcessorPGP.DEFAULT_SIGNATURE_ATTRIBUTE)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static String getSignatureHashDefaultValue() {
        return String.valueOf(PGPUtil.SHA256);
    }

    private static AllowableValue[] getSignatureHashAllowableValues() {

        return new AllowableValue[]{
                // Values match integer values in org.bouncycastle.bcpg.HashAlgorithmTags
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

        final ComponentLog logger = getLogger();
        final ExtendedStreamCallback callback;

        try {
            callback = new SignStreamCallback(buildSignSession(context, flowFile, session));
        } catch (final IOException | PGPException e) {
            logger.error("Exception constructing stream callback", e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            final StopWatch stopWatch = new StopWatch(true);
            final FlowFile finalFlow = session.write(flowFile, callback);
            callback.postProcess(session, finalFlow);
            logger.debug("Called to sign flow {}", new Object[]{flowFile});
            session.getProvenanceReporter().modifyContent(finalFlow, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(finalFlow, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Exception in sign flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PGP_KEY_SERVICE);
        properties.add(SIGNATURE_ATTRIBUTE);
        properties.add(SIGNATURE_HASH_ALGORITHM);
        return properties;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> validationResults = new ArrayList<>(super.customValidate(context));
        return null;
    }

    private SignStreamSession buildSignSession(ProcessContext context, FlowFile flowFile, ProcessSession session) throws PGPException, IOException {
        final PGPKeyMaterialService service = context.getProperty(PGP_KEY_SERVICE).asControllerService(PGPKeyMaterialService.class);
        PGPPrivateKey privateKey = service.getPrivateKey();
        String attribute = context.getProperty(SIGNATURE_ATTRIBUTE).getValue();
        int algo = getSignHashAlgorithm(context);
        return new SignStreamSession(getLogger(), privateKey, algo, attribute, session, flowFile);
    }

    private int getSignHashAlgorithm(ProcessContext context) {
        return context.getProperty(SIGNATURE_HASH_ALGORITHM).asInteger();
    }
}
