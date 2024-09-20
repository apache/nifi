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

package org.apache.nifi.python.processor;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import py4j.Py4JNetworkException;

import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

@InputRequirement(Requirement.INPUT_REQUIRED)
public class FlowFileTransformProxy extends PythonProcessorProxy<FlowFileTransform> {


    public FlowFileTransformProxy(final String processorType, final Supplier<PythonProcessorBridge> bridgeFactory, final boolean initialize) {
        super(processorType, bridgeFactory, initialize);
    }


    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final FlowFileTransformResult result;
        try (final StandardInputFlowFile inputFlowFile = new StandardInputFlowFile(session, flowFile)) {
            result = getTransform().transformFlowFile(inputFlowFile);
        } catch (final Py4JNetworkException e) {
            throw new ProcessException("Failed to communicate with Python Process", e);
        } catch (final Exception e) {
            getLogger().error("Failed to transform {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        try {
            final String relationshipName = result.getRelationship();
            final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
            final Map<String, String> attributes = result.getAttributes();

            if (REL_FAILURE.getName().equals(relationshipName)) {
                if (attributes != null) {
                    flowFile = session.putAllAttributes(flowFile, attributes);
                }

                session.transfer(flowFile, REL_FAILURE);
                return;
            }

            //Clone before making modifications if needed for original relationship
            final Optional<FlowFile> clone;
            if (context.isAutoTerminated(REL_ORIGINAL)) {
                clone = Optional.empty();
            } else {
                clone = Optional.of(session.clone(flowFile));
            }

            if (attributes != null) {
                flowFile = session.putAllAttributes(flowFile, attributes);
            }

            final byte[] contents = result.getContents();
            if (contents != null) {
                flowFile = session.write(flowFile, out -> out.write(contents));
            }

            session.transfer(flowFile, relationship);

            if (clone.isPresent()) {
                session.transfer(clone.get(), REL_ORIGINAL);
            }

        } finally {
            result.free();
        }
    }

}
