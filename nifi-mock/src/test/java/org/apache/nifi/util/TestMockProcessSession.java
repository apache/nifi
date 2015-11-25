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
package org.apache.nifi.util;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public class TestMockProcessSession {

    @Test(expected = AssertionError.class)
    public void testPenalizeFlowFileFromProcessor() {
        TestRunners.newTestRunner(PoorlyBehavedProcessor.class).run();
    }

    protected static class PoorlyBehavedProcessor extends AbstractProcessor {

        private static final Relationship REL_FAILURE = new Relationship.Builder()
                .name("failure")
                .build();

        private final Set<Relationship> relationships = Collections.singleton(REL_FAILURE);

        @Override
        public Set<Relationship> getRelationships() {
            return relationships;
        }

        @Override
        public void onTrigger(final ProcessContext ctx, final ProcessSession session) throws ProcessException {
            final FlowFile file = session.create();
            session.penalize(file);
            session.transfer(file, REL_FAILURE);
        }

    }
}
