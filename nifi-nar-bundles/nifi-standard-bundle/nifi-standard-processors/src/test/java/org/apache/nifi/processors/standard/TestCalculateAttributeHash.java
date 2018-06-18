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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestCalculateAttributeHash {

    @Test
    public void testMD2() throws Exception {
        testAlgorithm("MD2");
    }

    @Test
    public void testMD5() throws Exception {
        testAlgorithm("MD5");
    }

    @Test
    public void testSHA1() throws Exception {
        testAlgorithm("SHA-1");
    }

    @Test
    public void testSHA256() throws Exception {
        testAlgorithm("SHA-256");
    }

    @Test
    public void testSHA384() throws Exception {
        testAlgorithm("SHA-384");
    }

    @Test
    public void testSHA512() throws Exception {
        testAlgorithm("SHA-512");
    }

    public void testAlgorithm(String algorithm) {
        final TestRunner runner = TestRunners.newTestRunner(new CalculateAttributeHash());
        runner.setProperty(CalculateAttributeHash.HASH_ALGORITHM.getName(), algorithm);
        runner.setProperty("name", String.format("%s_%s", "name", algorithm));
        runner.setProperty("value", String.format("%s_%s", "value", algorithm));

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("name", "abcdefg");
        attributeMap.put("value", "hijklmnop");
        runner.enqueue(new byte[0], attributeMap);

        final Map<String, String> missingAttributeMap = new HashMap<>();
        missingAttributeMap.put("name", "foo");
        runner.enqueue(new byte[0], missingAttributeMap);
        runner.run(2);

        runner.assertTransferCount(HashAttribute.REL_FAILURE, 1);
        runner.assertTransferCount(HashAttribute.REL_SUCCESS, 1);

        final List<MockFlowFile> success = runner.getFlowFilesForRelationship(HashAttribute.REL_SUCCESS);
        final Map<String, Integer> correlationCount = new HashMap<>();

        for (final MockFlowFile flowFile : success) {
            Assert.assertEquals(flowFile.getAttribute(String.format("%s_%s", "name", algorithm)),
                    Hex.encodeHexString(DigestUtils.getDigest(algorithm).digest("abcdefg".getBytes(CalculateAttributeHash.UTF8))));
            Assert.assertEquals(flowFile.getAttribute(String.format("%s_%s", "value", algorithm)),
                    Hex.encodeHexString(DigestUtils.getDigest(algorithm).digest("hijklmnop".getBytes(CalculateAttributeHash.UTF8))));
        }
    }

}
