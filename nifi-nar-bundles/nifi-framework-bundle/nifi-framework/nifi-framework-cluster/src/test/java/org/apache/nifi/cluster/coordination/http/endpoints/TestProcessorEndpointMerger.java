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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.manager.ErrorMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestProcessorEndpointMerger {

    @Test
    public void testMergeValidationErrors() {
        final ProcessorEndpointMerger merger = new ProcessorEndpointMerger();
        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();

        final NodeIdentifier nodeId1234 = new NodeIdentifier("1234", "localhost", 9000, "localhost", 9001, "localhost", 9002, 9003, false);
        final List<String> nodeValidationErrors1234 = new ArrayList<>();
        nodeValidationErrors1234.add("error 1");
        nodeValidationErrors1234.add("error 2");

        ErrorMerger.mergeErrors(validationErrorMap, nodeId1234, nodeValidationErrors1234);

        final NodeIdentifier nodeXyz = new NodeIdentifier("xyz", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false);
        final List<String> nodeValidationErrorsXyz = new ArrayList<>();
        nodeValidationErrorsXyz.add("error 1");

        ErrorMerger.mergeErrors(validationErrorMap, nodeXyz, nodeValidationErrorsXyz);

        assertEquals(2, validationErrorMap.size());

        final Set<NodeIdentifier> idsError1 = validationErrorMap.get("error 1");
        assertEquals(2, idsError1.size());
        assertTrue(idsError1.contains(nodeId1234));
        assertTrue(idsError1.contains(nodeXyz));

        final Set<NodeIdentifier> idsError2 = validationErrorMap.get("error 2");
        assertEquals(1, idsError2.size());
        assertTrue(idsError2.contains(nodeId1234));
    }

}
