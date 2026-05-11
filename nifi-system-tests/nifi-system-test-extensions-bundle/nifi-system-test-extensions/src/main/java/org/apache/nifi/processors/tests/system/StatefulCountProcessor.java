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
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

@DefaultSchedule(period = "100 ms")
@Stateful(scopes = {Scope.CLUSTER, Scope.LOCAL}, description = "Stores a counter in both cluster and local state")
public class StatefulCountProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

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

        try {
            incrementState(session, Scope.CLUSTER);
            incrementState(session, Scope.LOCAL);
        } catch (final IOException e) {
            throw new ProcessException(e);
        }

        session.transfer(flowFile, REL_SUCCESS);
    }

    private void incrementState(final ProcessSession session, final Scope scope) throws IOException {
        final StateMap stateMap = session.getState(scope);
        final String countValue = stateMap.toMap().get("count");
        final int count = countValue == null ? 0 : Integer.parseInt(countValue);
        session.setState(Map.of("count", String.valueOf(count + 1)), scope);
    }
}
