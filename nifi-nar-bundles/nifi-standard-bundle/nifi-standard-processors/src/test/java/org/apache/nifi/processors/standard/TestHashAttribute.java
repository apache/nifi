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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

public class TestHashAttribute {

    @Test
    public void test() {
        final TestRunner runner = TestRunners.newTestRunner(new HashAttribute());
        runner.setProperty(HashAttribute.HASH_VALUE_ATTRIBUTE.getName(), "hashValue");
        runner.setProperty("MDKey1", ".*");
        runner.setProperty("MDKey2", "(.).*");

        final Map<String, String> attributeMap = new HashMap<>();
        attributeMap.put("MDKey1", "a");
        attributeMap.put("MDKey2", "b");
        runner.enqueue(new byte[0], attributeMap);

        attributeMap.put("MDKey1", "1");
        attributeMap.put("MDKey2", "2");
        runner.enqueue(new byte[0], attributeMap);

        attributeMap.put("MDKey1", "a");
        attributeMap.put("MDKey2", "z");
        runner.enqueue(new byte[0], attributeMap);

        attributeMap.put("MDKey1", "a");
        attributeMap.put("MDKey2", "bad");
        runner.enqueue(new byte[0], attributeMap);

        attributeMap.put("MDKey1", "a");
        attributeMap.remove("MDKey2");
        runner.enqueue(new byte[0], attributeMap);

        runner.run(5);

        runner.assertTransferCount(HashAttribute.REL_FAILURE, 1);
        runner.assertTransferCount(HashAttribute.REL_SUCCESS, 4);

        final List<MockFlowFile> success = runner.getFlowFilesForRelationship(HashAttribute.REL_SUCCESS);
        final Map<String, Integer> correlationCount = new HashMap<>();
        for (final MockFlowFile flowFile : success) {
            final String correlationId = flowFile.getAttribute("hashValue");
            assertNotNull(correlationId);

            Integer cur = correlationCount.get(correlationId);
            if (cur == null) {
                cur = 0;
            }

            correlationCount.put(correlationId, cur + 1);
        }

        int twoCount = 0;
        int oneCount = 0;
        for (final Integer i : correlationCount.values()) {
            if (i == 1) {
                oneCount++;
            } else if (i == 2) {
                twoCount++;
            } else {
                fail("Got count of " + i);
            }
        }

        assertEquals(1, twoCount);
        assertEquals(2, oneCount);
    }

}
