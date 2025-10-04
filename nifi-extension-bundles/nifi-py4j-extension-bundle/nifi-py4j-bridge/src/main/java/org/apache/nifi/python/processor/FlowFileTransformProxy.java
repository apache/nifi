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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

        final List<FlowFileTransformResult> results;
        try (final StandardInputFlowFile inputFlowFile = new StandardInputFlowFile(session, flowFile)) {
            final Object resultObject = getTransform().transformFlowFile(inputFlowFile);
            results = normalizeResults(resultObject);
        } catch (final Py4JNetworkException e) {
            throw new ProcessException("Failed to communicate with Python Process", e);
        } catch (final Exception e) {
            getLogger().error("Failed to transform {}", flowFile, e);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (results.isEmpty()) {
            getLogger().warn("Python processor {} returned no FlowFileTransformResults; routing {} to failure", this, flowFile);
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        if (results.size() == 1) {
            processSingleResult(results.get(0), context, session, flowFile);
            return;
        }

        processMultipleResults(results, context, session, flowFile);
    }

    private void processSingleResult(final FlowFileTransformResult result, final ProcessContext context, final ProcessSession session, FlowFile flowFile) {
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

            clone.ifPresent(cloned -> session.transfer(cloned, REL_ORIGINAL));
        } finally {
            result.free();
        }
    }

    private void processMultipleResults(final List<FlowFileTransformResult> results, final ProcessContext context, final ProcessSession session, final FlowFile original) {
        for (final FlowFileTransformResult result : results) {
            try {
                final String relationshipName = result.getRelationship();
                if (REL_FAILURE.getName().equals(relationshipName)) {
                    throw new ProcessException("Python processor returned multiple results including the failure relationship, which is not supported");
                }

                final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
                FlowFile child = session.clone(original);

                final Map<String, String> attributes = result.getAttributes();
                if (attributes != null) {
                    child = session.putAllAttributes(child, attributes);
                }

                final byte[] contents = result.getContents();
                if (contents != null) {
                    child = session.write(child, out -> out.write(contents));
                }

                session.transfer(child, relationship);
            } finally {
                result.free();
            }
        }

        if (context.isAutoTerminated(REL_ORIGINAL)) {
            session.remove(original);
        } else {
            session.transfer(original, REL_ORIGINAL);
        }
    }

    private List<FlowFileTransformResult> normalizeResults(final Object resultObject) {
        if (resultObject == null) {
            return List.of();
        }

        if (resultObject instanceof FlowFileTransformResult transformResult) {
            return List.of(transformResult);
        }

        final List<FlowFileTransformResult> results = new ArrayList<>();

        if (resultObject instanceof Iterable<?> iterable) {
            for (Object element : iterable) {
                addTransformResult(results, element);
            }
            return results;
        }

        if (resultObject.getClass().isArray()) {
            if (resultObject.getClass().getComponentType().isPrimitive()) {
                throw new ProcessException("Python processor returned primitive array when FlowFileTransformResult was expected");
            }

            final Object[] array = (Object[]) resultObject;
            Arrays.stream(array).forEach(element -> addTransformResult(results, element));
            return results;
        }

        throw new ProcessException("Python processor returned unsupported result type " + resultObject.getClass());
    }

    private void addTransformResult(final List<FlowFileTransformResult> results, final Object element) {
        if (element == null) {
            getLogger().warn("Python processor {} returned null FlowFileTransformResult which will be ignored", this);
            return;
        }

        if (!(element instanceof FlowFileTransformResult transformResult)) {
            throw new ProcessException("Python processor returned unsupported element type " + element.getClass());
        }

        results.add(transformResult);
    }

}
