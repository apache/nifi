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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestFuzzyHashContent {

    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(FuzzyHashContent.class);
    }

    @After
    public void stop() {
        runner.shutdown();
    }

    @Test
    public void testSsdeepFuzzyHashContent() {
        runner.setProperty(FuzzyHashContent.HASH_ALGORITHM, FuzzyHashContent.allowableValueSSDEEP.getValue());
        runner.enqueue("This is the a short and meaningless sample taste of 'test test test chocolate' " +
                "an odd test string that is used within some of the NiFi test units. Once day the author of " +
                "such strings may decide to tell the history behind this sentence and its true meaning...\n");
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(FuzzyHashContent.REL_SUCCESS, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(FuzzyHashContent.REL_SUCCESS).get(0);

        Assert.assertEquals("6:hERjIfhRrlB63J0FDw1NBQmEH68xwMSELN:hZrlB62IwMS",outFile.getAttribute("fuzzyhash.value") );
    }

    @Test
    public void testTLSHFuzzyHashInvalidContent() {
        runner.setProperty(FuzzyHashContent.HASH_ALGORITHM, FuzzyHashContent.allowableValueTLSH.getValue());
        runner.enqueue("This is the a short and meaningless sample taste of 'test test test chocolate' " +
                "an odd test string that is used within some of the NiFi test units. Once day the author of " +
                "such strings may decide to tell the history behind this sentence and its true meaning...\n");
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(FuzzyHashContent.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(FuzzyHashContent.REL_FAILURE).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value");
    }

    @Test
    public void testTLSHFuzzyHashValidContent() {
        runner.setProperty(FuzzyHashContent.HASH_ALGORITHM, FuzzyHashContent.allowableValueTLSH.getValue());
        runner.enqueue("This is the a short and meaningless sample taste of 'test test test chocolate' " +
                "an odd test string that is used within some of the NiFi test units. Once day the author of " +
                "such strings may decide to tell the history behind this sentence and its true meaning...\n" +
                "What is certain however, is that this section of the test unit requires at least 512 " +
                "characters of data to produce the expect results.\n " +
                "And yet... despite all the senseless verbosity, I still have to continue writing these somewhat " +
                "meaningless words that do nothing but to remind all of us of Ipsum Lorem..." );
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(FuzzyHashContent.REL_SUCCESS, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(FuzzyHashContent.REL_SUCCESS).get(0);

        Assert.assertEquals("E2F0818B7AE7173906A72221570E30979B11C0FC47B518A1E89D257E2343CEC02381ED",
                outFile.getAttribute("fuzzyhash.value"));
    }

}
