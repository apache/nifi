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
package org.apache.nifi.processors.tests.system;

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

@DefaultSchedule(period = "10 mins")
@Stateful(scopes = Scope.LOCAL, description = "Stores the number of the most recent state update along with a fixed-size value")
public class AppendLocalState extends AbstractProcessor {
    static final PropertyDescriptor UPDATE_COUNT = new PropertyDescriptor.Builder()
        .name("Update Count")
        .description("The number of state updates to perform each time the Processor is triggered.")
        .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
        .defaultValue("500")
        .required(true)
        .build();

    static final PropertyDescriptor VALUE_SIZE = new PropertyDescriptor.Builder()
        .name("Value Size")
        .description("The size of the value that is stored in state alongside the update number.")
        .addValidator(StandardValidators.createDataSizeBoundsValidator(1, 1024 * 1024))
        .defaultValue("64 B")
        .required(true)
        .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .build();

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return List.of(UPDATE_COUNT, VALUE_SIZE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        final int updateCount = context.getProperty(UPDATE_COUNT).asInteger();
        final int valueSize = context.getProperty(VALUE_SIZE).asDataSize(DataUnit.B).intValue();
        final String value = "A".repeat(valueSize);

        final StateManager stateManager = context.getStateManager();
        for (int i = 0; i < updateCount; i++) {
            try {
                stateManager.setState(Map.of("update", String.valueOf(i), "value", value), Scope.LOCAL);
            } catch (final IOException e) {
                throw new ProcessException("Failed to update local state", e);
            }
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
