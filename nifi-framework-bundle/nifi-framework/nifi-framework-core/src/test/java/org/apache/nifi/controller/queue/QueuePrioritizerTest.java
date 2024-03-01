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
package org.apache.nifi.controller.queue;

import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("EqualsWithItself")
class QueuePrioritizerTest {

    private static final String SORT_ATTRIBUTE = "my-sort-attribute";

    private final FlowFilePrioritizer flowFilePrioritizer = (o1, o2) -> {
        Comparator<FlowFile> comparing = Comparator.comparing(
                (flowFile) -> flowFile.getAttribute(SORT_ATTRIBUTE),
                Comparator.nullsFirst(Comparator.naturalOrder())
        );
        return comparing.compare(o1, o2);
    };
    private final ResourceClaimManager claimManager = new StandardResourceClaimManager();

    private final QueuePrioritizer prioritizer = new QueuePrioritizer(List.of(flowFilePrioritizer));

    @Test
    void deprioritizesFlowFilesWithPenalty() {
        MockFlowFileRecord nonPenalizedFlowFile = new MockFlowFileRecord(Map.of("penalized", "no", SORT_ATTRIBUTE, "later"), 0);
        MockFlowFileRecord expiredPenaltyFlowFile = new MockFlowFileRecord(Map.of("penalized", "no longer"), 0);
        expiredPenaltyFlowFile.setPenaltyExpiration(System.currentTimeMillis() - 9_001);
        MockFlowFileRecord penalizedFlowFile = new MockFlowFileRecord(Map.of("penalized", "short"), 0);
        penalizedFlowFile.setPenaltyExpiration(System.currentTimeMillis() + 123_456);
        MockFlowFileRecord longerPenalizedFlowFile = new MockFlowFileRecord(Map.of("penalized", "long"), 0);
        longerPenalizedFlowFile.setPenaltyExpiration(System.currentTimeMillis() + 456_789);

        assertEquals(0, prioritizer.compare(nonPenalizedFlowFile, nonPenalizedFlowFile));
        assertEquals(1, prioritizer.compare(nonPenalizedFlowFile, expiredPenaltyFlowFile));
        assertEquals(-1, prioritizer.compare(nonPenalizedFlowFile, penalizedFlowFile));
        assertEquals(-1, prioritizer.compare(nonPenalizedFlowFile, longerPenalizedFlowFile));
        assertEquals(1, prioritizer.compare(longerPenalizedFlowFile, nonPenalizedFlowFile));
        assertEquals(-1, prioritizer.compare(expiredPenaltyFlowFile, nonPenalizedFlowFile));
        assertEquals(0, prioritizer.compare(expiredPenaltyFlowFile, expiredPenaltyFlowFile));
        assertEquals(-1, prioritizer.compare(expiredPenaltyFlowFile, penalizedFlowFile));
        assertEquals(-1, prioritizer.compare(expiredPenaltyFlowFile, longerPenalizedFlowFile));
        assertEquals(1, prioritizer.compare(penalizedFlowFile, nonPenalizedFlowFile));
        assertEquals(1, prioritizer.compare(penalizedFlowFile, expiredPenaltyFlowFile));
        assertEquals(0, prioritizer.compare(penalizedFlowFile, penalizedFlowFile));
        assertEquals(-1, prioritizer.compare(penalizedFlowFile, longerPenalizedFlowFile));
        assertEquals(1, prioritizer.compare(longerPenalizedFlowFile, nonPenalizedFlowFile));
        assertEquals(1, prioritizer.compare(longerPenalizedFlowFile, expiredPenaltyFlowFile));
        assertEquals(1, prioritizer.compare(longerPenalizedFlowFile, penalizedFlowFile));
        assertEquals(0, prioritizer.compare(longerPenalizedFlowFile, longerPenalizedFlowFile));
    }

    @Test
    void prioritizesNonPenalizedFlowFilesByProvidedPrioritizers() {
        MockFlowFileRecord flowFileC = new MockFlowFileRecord(Map.of(SORT_ATTRIBUTE, "C"), 0);
        MockFlowFileRecord flowFileA = new MockFlowFileRecord(Map.of(SORT_ATTRIBUTE, "A"), 0);
        MockFlowFileRecord flowFileB = new MockFlowFileRecord(Map.of(SORT_ATTRIBUTE, "B"), 0);

        assertEquals(0, prioritizer.compare(flowFileC, flowFileC));
        assertTrue(prioritizer.compare(flowFileC, flowFileA) >= 1);
        assertTrue(prioritizer.compare(flowFileC, flowFileB) >= 1);
        assertTrue(prioritizer.compare(flowFileA, flowFileC) <= -1);
        assertEquals(0, prioritizer.compare(flowFileA, flowFileA));
        assertTrue(prioritizer.compare(flowFileA, flowFileB) <= -1);
        assertTrue(prioritizer.compare(flowFileB, flowFileC) <= -1);
        assertTrue(prioritizer.compare(flowFileB, flowFileA) >= 1);
        assertEquals(0, prioritizer.compare(flowFileB, flowFileB));
    }

    @Test
    void prioritizesNonPenalizedFlowFilesByClaimWhenNoPrioritizersAreProvided() {
        final ResourceClaim resourceClaim = claimManager
                .newResourceClaim("container", "section", "rc-id", false, false);
        claimManager.incrementClaimantCount(resourceClaim);
        MockFlowFileRecord flowFileWithClaimAndClaimOffset =
                new MockFlowFileRecord(Map.of(), 0, new StandardContentClaim(resourceClaim, 9L));
        MockFlowFileRecord flowFileWithClaimButNoOffset =
                new MockFlowFileRecord(Map.of(), 0, new StandardContentClaim(resourceClaim, 0L));
        MockFlowFileRecord flowFileWithoutClaim =
                new MockFlowFileRecord(Map.of(), 0, null);

        assertEquals(0, prioritizer.compare(flowFileWithClaimAndClaimOffset, flowFileWithClaimAndClaimOffset));
        assertTrue(prioritizer.compare(flowFileWithClaimAndClaimOffset, flowFileWithoutClaim) >= 1);
        assertTrue(prioritizer.compare(flowFileWithClaimAndClaimOffset, flowFileWithClaimButNoOffset) >= 1);
        assertTrue(prioritizer.compare(flowFileWithoutClaim, flowFileWithClaimAndClaimOffset) <= -1);
        assertEquals(0, prioritizer.compare(flowFileWithoutClaim, flowFileWithoutClaim));
        assertTrue(prioritizer.compare(flowFileWithoutClaim, flowFileWithClaimButNoOffset) <= -1);
        assertTrue(prioritizer.compare(flowFileWithClaimButNoOffset, flowFileWithClaimAndClaimOffset) <= -1);
        assertTrue(prioritizer.compare(flowFileWithClaimButNoOffset, flowFileWithoutClaim) >= 1);
        assertEquals(0, prioritizer.compare(flowFileWithClaimButNoOffset, flowFileWithClaimButNoOffset));
    }

    @Test
    void prioritizesByIdAsLastMeans() {
        MockFlowFileRecord flowFileFirstId = new MockFlowFileRecord();
        MockFlowFileRecord flowFileSecondId = new MockFlowFileRecord();
        MockFlowFileRecord flowFileThirdId = new MockFlowFileRecord();

        assertEquals(0, prioritizer.compare(flowFileThirdId, flowFileThirdId));
        assertTrue(prioritizer.compare(flowFileThirdId, flowFileFirstId) >= 1);
        assertTrue(prioritizer.compare(flowFileThirdId, flowFileSecondId) >= 1);
        assertTrue(prioritizer.compare(flowFileFirstId, flowFileThirdId) <= -1);
        assertEquals(0, prioritizer.compare(flowFileFirstId, flowFileFirstId));
        assertTrue(prioritizer.compare(flowFileFirstId, flowFileSecondId) <= -1);
        assertTrue(prioritizer.compare(flowFileSecondId, flowFileThirdId) <= -1);
        assertTrue(prioritizer.compare(flowFileSecondId, flowFileFirstId) >= 1);
        assertEquals(0, prioritizer.compare(flowFileSecondId, flowFileSecondId));
    }
}