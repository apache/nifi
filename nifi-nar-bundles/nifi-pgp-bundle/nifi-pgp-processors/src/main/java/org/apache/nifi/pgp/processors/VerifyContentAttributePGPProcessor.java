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
import org.apache.nifi.security.pgp.PGPKeyMaterialService;
import org.apache.nifi.security.pgp.StandardPGPOperator;
import org.apache.nifi.util.StopWatch;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * The VerifyContentAttributePGPProcessor processor attempts to verify a flow file signature when triggered.  The processor uses a
 * {@link PGPKeyMaterialService} to provide verification keys.
 */
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@Tags({"verify", "OpenPGP", "PGP", "GPG"})
@CapabilityDescription("Verifies a FlowFile using a PGP key.")
@SystemResourceConsideration(resource = SystemResource.CPU)

public class VerifyContentAttributePGPProcessor extends AbstractPGPProcessor {
    private final List<PropertyDescriptor> properties = Stream.concat(
            super.getSupportedPropertyDescriptors().stream(),
            Collections.unmodifiableList(Arrays.asList(StandardPGPOperator.SIGNATURE_ATTRIBUTE)).stream()
    ).collect(Collectors.toList());


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final StopWatch stopWatch = new StopWatch(true);
        try {
            if (!getPGPKeyMaterialService(context).verify(flowFile, context, session)) {
                throw new ProcessException("Unable to verify flow.");
            }
            final long elapsed = stopWatch.getElapsed(TimeUnit.MILLISECONDS);
            getLogger().debug("Called to verify flow {} completed in {}ms", new Object[]{flowFile, elapsed});
            session.getProvenanceReporter().modifyAttributes(flowFile, elapsed);
            session.transfer(flowFile, REL_SUCCESS);
        } catch (final ProcessException e) {
            getLogger().debug("Exception in verify flow {} ", new Object[]{flowFile});
            session.transfer(flowFile, REL_FAILURE);
        }
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
