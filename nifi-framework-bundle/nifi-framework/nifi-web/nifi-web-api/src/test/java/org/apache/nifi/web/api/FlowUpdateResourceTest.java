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
package org.apache.nifi.web.api;

import org.apache.nifi.web.api.concurrent.UpdateStep;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlowUpdateResourceTest {
    private static final List<String> STANDARD_STEPS = List.of(
            "Stopping Affected Processors",
            "Disabling Affected Controller Services",
            "Updating Flow",
            "Re-Enabling Controller Services",
            "Restarting Affected Processors");

    @Test
    void testRegistryUpdateHasRemovedConnectionDrainStep() {
        assertEquals(List.of(
                "Draining Removed Connections",
                "Stopping Affected Processors",
                "Disabling Affected Controller Services",
                "Updating Flow",
                "Re-Enabling Controller Services",
                "Restarting Affected Processors"), getStepDescriptions("update-requests"));
    }

    @Test
    void testNonUpdateRequestsRetainStandardSteps() {
        assertEquals(STANDARD_STEPS, getStepDescriptions("revert-requests"));
        assertEquals(STANDARD_STEPS, getStepDescriptions("rebase-requests"));
        assertEquals(STANDARD_STEPS, getStepDescriptions("replace-requests"));
    }

    private List<String> getStepDescriptions(final String requestType) {
        return FlowUpdateResource.getUpdateFlowSteps(requestType).stream()
                .map(UpdateStep::getDescription)
                .toList();
    }
}
