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
package org.apache.nifi.processors.cybersecurity;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestFuzzyHashContent {

    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(FuzzyHashContent.class);
    }

    @Test
    public void testFuzzyHashContentProcessor() {
        runner.setProperty(FuzzyHashContent.HASH_ALGORITHM, "ssdeep");
        runner.enqueue("This is the a short and meaningless sample taste of 'test test test chocolate' " +
                "an odd test string that is used within some of the NiFi test units. Once day the author of " +
                "such strings may decide to tell the history behind this sentence and its true meaning...\n");
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(FuzzyHashContent.REL_SUCCESS, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(FuzzyHashContent.REL_SUCCESS).get(0);

        Assert.assertEquals("6:hERjIfhRrlB63J0FDw1NBQmEH68xwMSELN:hZrlB62IwMS",outFile.getAttribute("fuzzyhash.value") );

    }

}
