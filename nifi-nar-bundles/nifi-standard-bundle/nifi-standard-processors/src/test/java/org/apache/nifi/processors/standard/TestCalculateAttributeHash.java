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
package org.apache.nifi.processors.standard;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCalculateAttributeHash {

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    @Test
    public void testMD2() throws Exception {
        testAllAlgorithm("MD2");
        testParitalAlgorithm("MD2");
        testMissingAlgorithm("MD2");
    }

    @Test
    public void testMD5() throws Exception {
        testAllAlgorithm("MD5");
        testParitalAlgorithm("MD5");
        testMissingAlgorithm("MD5");
    }

    @Test
    public void testSHA1() throws Exception {
        testAllAlgorithm("SHA-1");
        testParitalAlgorithm("SHA-1");
        testMissingAlgorithm("SHA-1");
    }

    @Test
    public void testSHA256() throws Exception {
        testAllAlgorithm("SHA-256");
        testParitalAlgorithm("SHA-256");
        testMissingAlgorithm("SHA-256");
    }

    @Test
    public void testSHA384() throws Exception {
        testAllAlgorithm("SHA-384");
        testParitalAlgorithm("SHA-384");
        testMissingAlgorithm("SHA-384");
    }

    @Test
    public void testSHA512() throws Exception {
        testAllAlgorithm("SHA-512");
        testParitalAlgorithm("SHA-512");
        testMissingAlgorithm("SHA-512");
    }

    public void testAllAlgorithm(String algorithm) {
        final TestRunner runner = TestRunners.newTestRunner(new CalculateAttributeHash());
        runner.setProperty(CalculateAttributeHash.HASH_ALGORITHM.getName(), algorithm);
        runner.setProperty("name", String.format("%s_%s", "name", algorithm));
        runner.setProperty("value", String.format("%s_%s", "value", algorithm));

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("name", "abcdefg");
        attributeMap.put("value", "hijklmnop");
        runner.enqueue(new byte[0], attributeMap);

        runner.run(1);

        runner.assertTransferCount(HashAttribute.REL_FAILURE, 0);
        runner.assertTransferCount(HashAttribute.REL_SUCCESS, 1);

        final List<MockFlowFile> success = runner.getFlowFilesForRelationship(HashAttribute.REL_SUCCESS);

        for (final MockFlowFile flowFile : success) {
            Assert.assertEquals(Hex.encodeHexString(DigestUtils.getDigest(algorithm).digest("abcdefg".getBytes(UTF8))),
                    flowFile.getAttribute(String.format("%s_%s", "name", algorithm)));
            Assert.assertEquals(Hex.encodeHexString(DigestUtils.getDigest(algorithm).digest("hijklmnop".getBytes(UTF8))),
                    flowFile.getAttribute(String.format("%s_%s", "value", algorithm)));
        }
    }

    public void testParitalAlgorithm(String algorithm) {
        final TestRunner runner = TestRunners.newTestRunner(new CalculateAttributeHash());
        runner.setProperty(CalculateAttributeHash.HASH_ALGORITHM.getName(), algorithm);
        runner.setProperty("name", String.format("%s_%s", "name", algorithm));
        runner.setProperty("value", String.format("%s_%s", "value", algorithm));

        // test default ALLOW

        final Map<String, String> paritalAttributeMap = new HashMap<>();
        paritalAttributeMap.put("name", "abcdefg");
        runner.enqueue(new byte[0], paritalAttributeMap);

        runner.run(1);

        runner.assertTransferCount(HashAttribute.REL_FAILURE, 0);
        runner.assertTransferCount(HashAttribute.REL_SUCCESS, 1);

        final List<MockFlowFile> success = runner.getFlowFilesForRelationship(HashAttribute.REL_SUCCESS);

        for (final MockFlowFile flowFile : success) {
            Assert.assertEquals(Hex.encodeHexString(DigestUtils.getDigest(algorithm).digest("abcdefg".getBytes(UTF8))),
                    flowFile.getAttribute(String.format("%s_%s", "name", algorithm)));
                   }

        runner.clearTransferState();

        // test PROHIBIT
        runner.setProperty(CalculateAttributeHash.PARTIAL_ATTR_ROUTE_POLICY, CalculateAttributeHash.PartialAttributePolicy.PROHIBIT.name());
        runner.enqueue(new byte[0], paritalAttributeMap);

        runner.run(1);

        runner.assertTransferCount(HashAttribute.REL_FAILURE, 1);
        runner.assertTransferCount(HashAttribute.REL_SUCCESS, 0);
    }

    public void testMissingAlgorithm(String algorithm) {
        final TestRunner runner = TestRunners.newTestRunner(new CalculateAttributeHash());
        runner.setProperty(CalculateAttributeHash.HASH_ALGORITHM.getName(), algorithm);
        runner.setProperty("name", String.format("%s_%s", "name", algorithm));
        runner.setProperty("value", String.format("%s_%s", "value", algorithm));

        // test default of true
        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("not_name", "abcdefg");
        attributeMap.put("not_value", "hijklmnop");
        runner.enqueue(new byte[0], attributeMap);

        runner.run(1);

        runner.assertTransferCount(HashAttribute.REL_FAILURE, 1);
        runner.assertTransferCount(HashAttribute.REL_SUCCESS, 0);

        runner.clearTransferState();

        runner.setProperty(CalculateAttributeHash.FAIL_WHEN_EMPTY,"false");

        runner.enqueue(new byte[0], attributeMap);

        runner.run(1);

        runner.assertTransferCount(HashAttribute.REL_FAILURE, 0);
        runner.assertTransferCount(HashAttribute.REL_SUCCESS, 1);

        final List<MockFlowFile> success = runner.getFlowFilesForRelationship(HashAttribute.REL_SUCCESS);
        final Map<String, Integer> correlationCount = new HashMap<>();

        for (final MockFlowFile flowFile : success) {
            flowFile.assertAttributeNotExists(String.format("%s_%s", "name", algorithm));
            flowFile.assertAttributeNotExists(String.format("%s_%s", "value", algorithm));
            Assert.assertEquals("abcdefg",flowFile.getAttribute("not_name"));
            Assert.assertEquals("hijklmnop",flowFile.getAttribute("not_value"));
        }
    }
}
