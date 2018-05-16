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


import org.apache.nifi.processors.cybersecurity.matchers.FuzzyHashMatcher;
import org.apache.nifi.processors.cybersecurity.matchers.SSDeepHashMatcher;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestCompareFuzzyHash {
    String ssdeepInput = "48:c1xs8Z/m6H0eRH31S8p8bHENANkPrNy4tkPytwPyh2jTytxPythPytNdPytDgYyF:OuO/mg3HFSRHEb44RNMi6uHU2hcq3";
    String tlshInput = "EB519EA4A8F95171A2A409C1DEEB9872AF55C137E00A5289F1CCD0CE4F6CCD784BB4B7";

    final CompareFuzzyHash proc = new CompareFuzzyHash();
    final private TestRunner runner = TestRunners.newTestRunner(proc);

    @After
    public void stop() {
        runner.shutdown();
    }

    @Test
    public void testSsdeepCompareFuzzyHash() {
        double matchingSimilarity = 80;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueSSDEEP.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/ssdeep.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));
        runner.setProperty(CompareFuzzyHash.MATCHING_MODE, CompareFuzzyHash.singleMatch.getValue());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", ssdeepInput);

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FOUND).get(0);


        outFile.assertAttributeEquals(
                "fuzzyhash.value.0.match",
                "\"nifi/nifi-nar-bundles/nifi-beats-bundle/nifi-beats-processors/pom.xml\""
        );
        double similarity = Double.valueOf(outFile.getAttribute("fuzzyhash.value.0.similarity"));
        Assert.assertTrue(similarity >= matchingSimilarity);

        outFile.assertAttributeNotExists("fuzzyhash.value.1.match");
    }

    @Test
    public void testSsdeepCompareFuzzyHashMultipleMatches() {
        double matchingSimilarity = 80;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueSSDEEP.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/ssdeep.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));
        runner.setProperty(CompareFuzzyHash.MATCHING_MODE, CompareFuzzyHash.multiMatch.getValue());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", ssdeepInput );

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FOUND).get(0);


        outFile.assertAttributeEquals("fuzzyhash.value.0.match",
                "\"nifi/nifi-nar-bundles/nifi-lumberjack-bundle/nifi-lumberjack-processors/pom.xml\""
        );

        double similarity = Double.valueOf(outFile.getAttribute("fuzzyhash.value.0.similarity"));
        Assert.assertTrue(similarity >= matchingSimilarity);

        outFile.assertAttributeEquals("fuzzyhash.value.1.match",
                "\"nifi/nifi-nar-bundles/nifi-beats-bundle/nifi-beats-processors/pom.xml\""
        );
        similarity = Double.valueOf(outFile.getAttribute("fuzzyhash.value.1.similarity"));
        Assert.assertTrue(similarity >= matchingSimilarity);
    }

    @Test
    public void testSsdeepCompareFuzzyHashWithBlankHashList() {
        double matchingSimilarity = 80;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueSSDEEP.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/blank_ssdeep.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", "6:hERjIfhRrlB63J0FDw1NBQmEH68xwMSELN:hZrlB62IwMS");

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_NOT_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_NOT_FOUND).get(0);
    }

    @Test
    public void testSsdeepCompareFuzzyHashWithInvalidHashList() {
        // This is different from "BlankHashList series of tests in that the file lacks headers and as such is totally
        // invalid
        double matchingSimilarity = 80;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueSSDEEP.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/empty.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", "6:hERjIfhRrlB63J0FDw1NBQmEH68xwMSELN:hZrlB62IwMS");

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_NOT_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_NOT_FOUND).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value.0.match");
    }

    @Test
    public void testSsdeepCompareFuzzyHashWithInvalidHash() {
        double matchingSimilarity = 80;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueSSDEEP.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/ssdeep.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));
        runner.setProperty(CompareFuzzyHash.MATCHING_MODE, CompareFuzzyHash.singleMatch.getValue());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", "Test test test chocolate!");

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FAILURE).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value.0.match");
    }


    @Test
    public void testTLSHCompareFuzzyHash() {
        double matchingSimilarity = 200;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueTLSH.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/tlsh.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));
        runner.setProperty(CompareFuzzyHash.MATCHING_MODE, CompareFuzzyHash.singleMatch.getValue());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", tlshInput);

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FOUND).get(0);

        outFile.assertAttributeEquals(
                "fuzzyhash.value.0.match",
                "nifi-nar-bundles/nifi-lumberjack-bundle/nifi-lumberjack-processors/pom.xml"
        );
        double similarity = Double.valueOf(outFile.getAttribute("fuzzyhash.value.0.similarity"));
        Assert.assertTrue(similarity <= matchingSimilarity);

        outFile.assertAttributeNotExists("fuzzyhash.value.1.match");
    }

    @Test
    public void testTLSHCompareFuzzyHashMultipleMatches() {
        double matchingSimilarity = 200;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueTLSH.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/tlsh.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));
        runner.setProperty(CompareFuzzyHash.MATCHING_MODE, CompareFuzzyHash.multiMatch.getValue());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", tlshInput);

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FOUND).get(0);

        outFile.assertAttributeEquals(
                "fuzzyhash.value.0.match",
                "nifi-nar-bundles/nifi-lumberjack-bundle/nifi-lumberjack-processors/pom.xml"
        );
        double similarity = Double.valueOf(outFile.getAttribute("fuzzyhash.value.0.similarity"));
        Assert.assertTrue(similarity <= matchingSimilarity);

        outFile.assertAttributeEquals(
                "fuzzyhash.value.1.match",
                "nifi-nar-bundles/nifi-beats-bundle/nifi-beats-processors/pom.xml"
        );
        similarity = Double.valueOf(outFile.getAttribute("fuzzyhash.value.1.similarity"));
        Assert.assertTrue(similarity <= matchingSimilarity);
    }


    @Test
    public void testTLSHCompareFuzzyHashWithBlankFile() {
        // This is different from "BlankHashList series of tests in that the file lacks headers and as such is totally
        // invalid
        double matchingSimilarity = 200;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueTLSH.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/empty.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", "E2F0818B7AE7173906A72221570E30979B11C0FC47B518A1E89D257E2343CEC02381ED");

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_NOT_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_NOT_FOUND).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value.0.match");
    }

    @Test
    public void testTLSHCompareFuzzyHashWithEmptyHashList() {
        double matchingSimilarity = 200;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueTLSH.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/empty.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", "E2F0818B7AE7173906A72221570E30979B11C0FC47B518A1E89D257E2343CEC02381ED");

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_NOT_FOUND, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_NOT_FOUND).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value.0.match");
    }

    @Test
    public void testTLSHCompareFuzzyHashWithInvalidHash() {
        double matchingSimilarity = 200;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueTLSH.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/empty.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", "Test test test chocolate");

        runner.enqueue("bogus".getBytes(), attributes);
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FAILURE).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value.0.match");

    }

    @Test
    public void testMissingAttribute() {
        double matchingSimilarity = 200;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueTLSH.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/empty.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));
        runner.setProperty(CompareFuzzyHash.MATCHING_MODE, CompareFuzzyHash.multiMatch.getValue());

        runner.enqueue("bogus".getBytes());
        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FAILURE).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value.0.match");
    }

    @Test
    public void testAttributeIsEmptyString() {
        double matchingSimilarity = 200;
        runner.setProperty(CompareFuzzyHash.HASH_ALGORITHM, CompareFuzzyHash.allowableValueTLSH.getValue());
        runner.setProperty(CompareFuzzyHash.ATTRIBUTE_NAME, "fuzzyhash.value");
        runner.setProperty(CompareFuzzyHash.HASH_LIST_FILE, "src/test/resources/empty.list");
        runner.setProperty(CompareFuzzyHash.MATCH_THRESHOLD, String.valueOf(matchingSimilarity));
        runner.setProperty(CompareFuzzyHash.MATCHING_MODE, CompareFuzzyHash.multiMatch.getValue());

        Map<String, String> attributes = new HashMap<>();
        attributes.put("fuzzyhash.value", "");
        runner.enqueue("bogus".getBytes(), attributes);

        runner.run();

        runner.assertQueueEmpty();
        runner.assertAllFlowFilesTransferred(CompareFuzzyHash.REL_FAILURE, 1);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(CompareFuzzyHash.REL_FAILURE).get(0);

        outFile.assertAttributeNotExists("fuzzyhash.value.0.match");
    }

    @Test
    public void testlooksLikeSpamSum() {
        FuzzyHashMatcher matcher = new SSDeepHashMatcher();

        List<String> invalidPayloads = Arrays.asList(
                "4AD:c1xs8Z/m6H0eRH31S8p8bHENANkPrNy4tkPytwPyh2jTytxPythPytNdPytDgYyF:OuO/mg3HFSRHEb44RNMi6uHU2hcq3", // invalidFirstField
                ":c1xs8Z/m6H0eRH31S8p8bHENANkPrNy4tkPytwPyh2jTytxPythPytNdPytDgYyF:OuO/mg3HFSRHEb44RNMi6uHU2hcq3", // emptyFirstField
                "48::OuO/mg3HFSRHEb44RNMi6uHU2hcq3", // emptySecondField
                "48:c1xs8Z/m6H0eRH31S8p8bHENANkPrNy4tkPytwPyh2jTytxPythPytNdPytDgYyF:", //         emptyThirdField
                "48:c1xs8Z/m6H0eRH31S8p8bHENANkPrNy4tkPytwPyh2jTytxPythPytNdPytDgYyF", //         withoutThirdField
                "c1xs8Z/m6H0eRH31S8p8bHENANkPrNy4tkPytwPyh2jTytxPythPytNdPytDgYyF" //         Just a simple string
        );

        for (String item : invalidPayloads) {
            Assert.assertTrue("item '" + item + "' should have failed validation",  !matcher.isValidHash(item));
        }

        // Now test with a valid string
        Assert.assertTrue(matcher.isValidHash(ssdeepInput));

    }
}
