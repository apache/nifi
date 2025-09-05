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
package org.apache.nifi.processors.aws.kinesis;

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.PROCESSING_STRATEGY;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.ProcessingStrategy.FLOW_FILE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.ProcessingStrategy.RECORD;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.REL_PARSE_FAILURE;
import static org.apache.nifi.processors.aws.kinesis.ConsumeKinesis.REL_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ConsumeKinesisTest {

    private TestRunner testRunner;

    @BeforeEach
    void setUp() {
        testRunner = TestRunners.newTestRunner(ConsumeKinesis.class);
    }

    @Test
    void getRelationshipsForFlowFileProcessingStrategy() {
        testRunner.setProperty(PROCESSING_STRATEGY, FLOW_FILE);

        final Set<Relationship> relationships = testRunner.getProcessor().getRelationships();

        assertEquals(Set.of(REL_SUCCESS), relationships);
    }

    @Test
    void getRelationshipsForRecordProcessingStrategy() {
        testRunner.setProperty(PROCESSING_STRATEGY, RECORD);

        final Set<Relationship> relationships = testRunner.getProcessor().getRelationships();

        assertEquals(Set.of(REL_SUCCESS, REL_PARSE_FAILURE), relationships);
    }
}
