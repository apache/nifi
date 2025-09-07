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

import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@PrimaryNodeOnly
@DefaultSchedule(period = "10 mins")
@Stateful(scopes = Scope.CLUSTER, description = "Stores three counters in state", dropStateKeySupported = true)
public class MultiKeyState extends AbstractProcessor {
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .autoTerminateDefault(true)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return Set.of(REL_SUCCESS);
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final Map<String, String> currentState;
        try {
            currentState = session.getState(Scope.CLUSTER).toMap();
        } catch (IOException e) {
            throw new ProcessException(e);
        }

        final int a = Integer.parseInt(currentState.getOrDefault("a", "0")) + 1;
        final int b = Integer.parseInt(currentState.getOrDefault("b", "0")) + 1;
        final int c = Integer.parseInt(currentState.getOrDefault("c", "0")) + 1;

        final Map<String, String> newState = new HashMap<>();
        newState.put("a", String.valueOf(a));
        newState.put("b", String.valueOf(b));
        newState.put("c", String.valueOf(c));
        try {
            session.setState(newState, Scope.CLUSTER);
        } catch (IOException e) {
            throw new ProcessException(e);
        }

        FlowFile flowFile = session.create();
        session.transfer(flowFile, REL_SUCCESS);
    }
}
