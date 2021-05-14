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

package org.apache.nifi.tests.system.repositories;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

/**
 * This test is intended to verify that Processors are able to access the content that their FlowFiles represent in several different situations.
 * We test for things like splitting a FlowFile by creating multiple children and writing to them, as well as creating children via processSession.clone(FlowFile flowFile, long offset, long length);
 * We also test against Run Duration of 0 ms vs. 25 milliseconds in order to test when a processor writes the contents to multiple FlowFiles in the same session (which will result in writing to the
 * same Content Claim) as well as writing to multiple FlowFiles in multiple sessions (which may result in writing to multiple Content Claims).
 */
public class ContentAccessIT extends NiFiSystemIT {

    @Test
    public void testCorrectContentReadWhenMultipleFlowFilesInClaimWithBatchAndWrite() throws NiFiClientException, IOException, InterruptedException {
        testCorrectContentReadWhenMultipleFlowFilesInClaim(true, false);
    }

    @Test
    public void testCorrectContentReadWhenMultipleFlowFilesInClaimWithoutBatchAndWrite() throws NiFiClientException, IOException, InterruptedException {
        testCorrectContentReadWhenMultipleFlowFilesInClaim(false, false);
    }

    @Test
    public void testCorrectContentReadWhenMultipleFlowFilesInClaimWithBatchAndClone() throws NiFiClientException, IOException, InterruptedException {
        testCorrectContentReadWhenMultipleFlowFilesInClaim(true, true);
    }

    @Test
    public void testCorrectContentReadWhenMultipleFlowFilesInClaimWithoutBatchAndClone() throws NiFiClientException, IOException, InterruptedException {
        testCorrectContentReadWhenMultipleFlowFilesInClaim(false, true);
    }

    public void testCorrectContentReadWhenMultipleFlowFilesInClaim(final boolean useBatch, final boolean clone) throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        final ProcessorEntity split = getClientUtil().createProcessor("SplitByLine");
        final ProcessorEntity reverse = getClientUtil().createProcessor("ReverseContents");
        final ProcessorEntity verify = getClientUtil().createProcessor("VerifyContents");
        final ProcessorEntity terminateAa = getClientUtil().createProcessor("TerminateFlowFile");
        final ProcessorEntity terminateBa = getClientUtil().createProcessor("TerminateFlowFile");
        final ProcessorEntity terminateCa = getClientUtil().createProcessor("TerminateFlowFile");
        final ProcessorEntity terminateUnmatched = getClientUtil().createProcessor("TerminateFlowFile");

        // Configure Generate
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 mins");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("Text", "{ a : a }\n{ a : b }\n{ a : c }"));

        // Configure split
        getClientUtil().updateProcessorProperties(split, Collections.singletonMap("Use Clone", String.valueOf(clone)));

        // Configure Verify
        final Map<String, String> verifyProperties = new HashMap<>();
        verifyProperties.put("aa", "} a : a {");
        verifyProperties.put("ba", "} b : a {");
        verifyProperties.put("ca", "} c : a {");
        getClientUtil().updateProcessorProperties(verify, verifyProperties);

        // Configure batching for reverse
        final int runDuration = useBatch ? 25 : 0;
        getClientUtil().updateProcessorRunDuration(reverse, runDuration);

        final ConnectionEntity generateToSplit = getClientUtil().createConnection(generate, split, "success");
        final ConnectionEntity splitToReverse = getClientUtil().createConnection(split, reverse, "success");
        final ConnectionEntity reverseToVerify = getClientUtil().createConnection(reverse, verify, "success");
        final ConnectionEntity verifyToTerminateAa = getClientUtil().createConnection(verify, terminateAa, "aa");
        final ConnectionEntity verifyToTerminateBa = getClientUtil().createConnection(verify, terminateBa, "ba");
        final ConnectionEntity verifyToTerminateCa = getClientUtil().createConnection(verify, terminateCa, "ca");
        final ConnectionEntity verifyToTerminateUnmatched = getClientUtil().createConnection(verify, terminateAa, "unmatched");

        // Run Generate processor, wait for its output
        getNifiClient().getProcessorClient().startProcessor(generate);
        waitForQueueCount(generateToSplit.getId(), 1);

        // Run split processor, wait for its output
        getNifiClient().getProcessorClient().startProcessor(split);
        waitForQueueCount(splitToReverse.getId(), 3);

        // Verify output of the Split processor
        final String firstSplitContents = getClientUtil().getFlowFileContentAsUtf8(splitToReverse.getId(), 0);
        final String secondSplitContents = getClientUtil().getFlowFileContentAsUtf8(splitToReverse.getId(), 1);
        final String thirdSplitContents = getClientUtil().getFlowFileContentAsUtf8(splitToReverse.getId(), 2);

        // Verify that we get both expected outputs. We put them in a set and ensure that the set contains both because we don't know the order
        // that they will be in. The reason we don't know the order is because if we are using batching, the contents will be in the same output
        // Content Claim, otherwise they won't be. If they are not, the order can change.
        final Set<String> splitContents = new HashSet<>();
        splitContents.add(firstSplitContents);
        splitContents.add(secondSplitContents);
        splitContents.add(thirdSplitContents);

        assertTrue(splitContents.contains("{ a : a }"));
        assertTrue(splitContents.contains("{ a : b }"));
        assertTrue(splitContents.contains("{ a : c }"));

        // Start the reverse processor, wait for its output
        getNifiClient().getProcessorClient().startProcessor(reverse);
        waitForQueueCount(reverseToVerify.getId(), 3);

        final String firstReversedContents = getClientUtil().getFlowFileContentAsUtf8(reverseToVerify.getId(), 0);
        final String secondReversedContents = getClientUtil().getFlowFileContentAsUtf8(reverseToVerify.getId(), 1);
        final String thirdReversedContents = getClientUtil().getFlowFileContentAsUtf8(reverseToVerify.getId(), 2);

        final Set<String> reversedContents = new HashSet<>();
        reversedContents.add(firstReversedContents);
        reversedContents.add(secondReversedContents);
        reversedContents.add(thirdReversedContents);

        assertTrue(reversedContents.contains("} a : a {"));
        assertTrue(reversedContents.contains("} b : a {"));
        assertTrue(reversedContents.contains("} c : a {"));

        // Start verify processor. This is different than verify the contents above because doing so above is handled by making a REST call, which does not make use
        // of the ProcessSession. Using the VerifyContents processor ensures that the Processors see the same contents.
        getNifiClient().getProcessorClient().startProcessor(verify);

        waitForQueueCount(verifyToTerminateAa.getId(), 1);
        waitForQueueCount(verifyToTerminateBa.getId(), 1);
        waitForQueueCount(verifyToTerminateCa.getId(), 1);
        waitForQueueCount(verifyToTerminateUnmatched.getId(), 0);
    }
}
