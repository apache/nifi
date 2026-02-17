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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.metrics.CommitTiming;

import java.util.Set;

public class UpdateMetric extends AbstractProcessor {

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .autoTerminateDefault(true)
            .build();

    private static final Set<Relationship> RELATIONSHIPS = Set.of(SUCCESS);

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        session.adjustCounter("onTrigger", 1, true);

        final Runtime runtime = Runtime.getRuntime();
        final long freeMemory = runtime.freeMemory();
        session.recordGauge("freeMemory", freeMemory, CommitTiming.NOW);

        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        session.transfer(flowFile, SUCCESS);
    }
}
