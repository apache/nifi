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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SystemResource;
import org.apache.nifi.annotation.behavior.SystemResourceConsideration;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * The DecryptPGP processor attempts to decrypt flow file contents when triggered.  The processor uses a
 * {@link PGPKeyMaterialControllerService} to provide decryption keys.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"decryption", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Decrypts a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class DecryptPGP extends AbstractProcessorPGP {
    public static final PropertyDescriptor PGP_KEY_SERVICE =
            AbstractProcessorPGP.buildKeyServiceProperty("PGP Key Material Controller Service that provides the private key for decryption.");

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final ComponentLog logger = getLogger();
        final ExtendedStreamCallback callback = new DecryptStreamCallback(buildDecryptSession(context));

        try {
            final StopWatch stopWatch = new StopWatch(true);
            final FlowFile finalFlow = session.write(flowFile, callback);
            callback.postProcess(session, finalFlow);
            logger.debug("Called to decrypt flow {}", new Object[]{flowFile});
            session.getProvenanceReporter().modifyContent(finalFlow, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(finalFlow, REL_SUCCESS);
        } catch (final ProcessException e) {
            logger.error("Exception in decrypt flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PGP_KEY_SERVICE);
        return properties;
    }

    private DecryptStreamSession buildDecryptSession(ProcessContext context) {
        final PGPKeyMaterialService service = context.getProperty(PGP_KEY_SERVICE).asControllerService(PGPKeyMaterialService.class);

        char[] passphrase = service.getPBEPassPhrase();
        if (passphrase != null && passphrase.length != 0) {
            return new PBEDecryptStreamSession(getLogger(), passphrase);
        }

        return new PrivateKeyDecryptStreamSession(getLogger(), service.getPrivateKey());
    }
}
