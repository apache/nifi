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
import org.apache.nifi.security.pgp.StandardPGPOperator;
import org.apache.nifi.pgp.controllerservices.PGPKeyMaterialService;
import org.apache.nifi.util.StopWatch;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The EncryptContentPGP processor attempts to encrypt flow file contents when triggered.  The processor uses a
 * {@link PGPKeyMaterialService} to provide encryption operations.
 *
 * This processor exposes the encryption algorithm selection to the user, and also exposes a property that controls
 * encrypted content encoding.
 *
 */

@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"encryption", "decryption", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Encrypts a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class EncryptContentPGP extends AbstractPGPProcessor {
    private final List<PropertyDescriptor> properties = Stream.concat(
            super.getSupportedPropertyDescriptors().stream(),
            Collections.unmodifiableList(Arrays.asList(StandardPGPOperator.ENCRYPT_ALGORITHM, StandardPGPOperator.ENCRYPT_ENCODING)).stream()
    ).collect(Collectors.toList());

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        try {
            final FlowFile finalFlow = getPGPKeyMaterialService(context).encrypt(flowFile, context, session);
            final long elapsed = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            getLogger().debug("Called to encrypt flow {} completed in {}ms", new Object[]{flowFile, elapsed});
            session.getProvenanceReporter().modifyContent(finalFlow, elapsed);
            session.transfer(finalFlow, REL_SUCCESS);
        } catch (final ProcessException e) {
            getLogger().debug("Exception in encrypt flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
