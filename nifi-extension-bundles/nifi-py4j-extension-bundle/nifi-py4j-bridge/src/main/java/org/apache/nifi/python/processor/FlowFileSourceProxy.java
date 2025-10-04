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

import org.apache.nifi.annotation.behavior.DefaultRunDuration;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.scheduling.SchedulingStrategy;
import py4j.Py4JNetworkException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

@InputRequirement(Requirement.INPUT_FORBIDDEN)
@SupportsBatching(defaultDuration = DefaultRunDuration.NO_BATCHING)
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class FlowFileSourceProxy extends PythonProcessorProxy<FlowFileSource> {

    protected static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles created by this processor can be routed to this relationship.")
            .build();

    private static final Set<Relationship> implicitRelationships = Set.of(REL_SUCCESS);

    public FlowFileSourceProxy(final String processorType, final Supplier<PythonProcessorBridge> bridgeFactory, final boolean initialize) {
        super(processorType, bridgeFactory, initialize);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Object resultObject;
        try {
            resultObject = getTransform().createFlowFile();
        } catch (final Py4JNetworkException e) {
            throw new ProcessException("Failed to communicate with Python Process", e);
        } catch (final Exception e) {
            getLogger().error("Failed to create FlowFile", e);
            return;
        }

        final List<FlowFileSourceResult> results = normalizeResults(resultObject);
        if (results.isEmpty()) {
            return;
        }

        for (final FlowFileSourceResult result : results) {
            try {
                final String relationshipName = result.getRelationship();
                final Relationship relationship = new Relationship.Builder().name(relationshipName).build();
                final Map<String, String> attributes = result.getAttributes();
                final byte[] contents = result.getContents();

                FlowFile output = createFlowFile(session, attributes, contents);

                if (REL_SUCCESS.getName().equals(relationshipName)) {
                    session.transfer(output, REL_SUCCESS);
                } else {
                    session.transfer(output, relationship);
                }
            } finally {
                result.free();
            }
        }
    }

    protected FlowFile createFlowFile(final ProcessSession session, final Map<String, String> attributes, final byte[] contents) {
        FlowFile flowFile = session.create();
        if (attributes != null) {
            flowFile = session.putAllAttributes(flowFile, attributes);
        }
        if (contents != null) {
            flowFile = session.write(flowFile, out -> out.write(contents));
        }
        return flowFile;
    }

    @Override
    protected Set<Relationship> getImplicitRelationships() {
        return implicitRelationships;
    }

    private List<FlowFileSourceResult> normalizeResults(final Object resultObject) {
        if (resultObject == null) {
            return List.of();
        }

        if (resultObject instanceof FlowFileSourceResult flowFileSourceResult) {
            return List.of(flowFileSourceResult);
        }

        final List<FlowFileSourceResult> results = new ArrayList<>();

        if (resultObject instanceof Iterable<?> iterable) {
            for (Object element : iterable) {
                addResult(results, element);
            }
            return results;
        }

        if (resultObject.getClass().isArray()) {
            if (resultObject.getClass().getComponentType().isPrimitive()) {
                throw new ProcessException("Python processor returned primitive array when FlowFileSourceResult was expected");
            }

            final Object[] array = (Object[]) resultObject;
            Arrays.stream(array).forEach(element -> addResult(results, element));
            return results;
        }

        throw new ProcessException("Python processor returned unsupported result type " + resultObject.getClass());
    }

    private void addResult(final List<FlowFileSourceResult> results, final Object element) {
        if (element == null) {
            getLogger().warn("Python processor {} returned null FlowFileSourceResult which will be ignored", this);
            return;
        }

        if (!(element instanceof FlowFileSourceResult flowFileSourceResult)) {
            throw new ProcessException("Python processor returned unsupported element type " + element.getClass());
        }

        results.add(flowFileSourceResult);
    }
}
