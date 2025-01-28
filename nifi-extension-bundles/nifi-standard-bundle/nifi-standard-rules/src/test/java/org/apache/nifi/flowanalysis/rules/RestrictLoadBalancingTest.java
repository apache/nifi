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
package org.apache.nifi.flowanalysis.rules;

import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RestrictLoadBalancingTest extends AbstractFlowAnalaysisRuleTest<RestrictLoadBalancing> {
    @Override
    protected RestrictLoadBalancing initializeRule() {
        return new RestrictLoadBalancing();
    }

    @BeforeEach
    @Override
    public void setup() {
        super.setup();
        // Default settings
        setProperty(RestrictLoadBalancing.ALLOW_DO_NOT_LOAD_BALANCE, "true");
        setProperty(RestrictLoadBalancing.ALLOW_PARTITION, "false");
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, "false");
        setProperty(RestrictLoadBalancing.ALLOW_SINGLE_NODE, "false");
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, "true");
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, "true");
    }

    @Test
    public void testBadConfiguration() {
        // Set all load balancing strategies to 'false'
        setProperty(RestrictLoadBalancing.ALLOW_DO_NOT_LOAD_BALANCE, "false");
        ArrayList<ValidationResult> validationErrors = new ArrayList<>(rule.customValidate(validationContext));
        assertEquals(1, validationErrors.size());
        assertTrue(validationErrors.getFirst().getExplanation().contains(RestrictLoadBalancing.CONFIGURE_STRATEGY_ERROR_MESSAGE));

        // Set attribute compression to false while content compression is true (and at least one of the load balancing strategies is allowed)
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, "true");
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, "false");
        validationErrors = new ArrayList<>(rule.customValidate(validationContext));
        assertEquals(1, validationErrors.size());
        assertTrue(validationErrors.getFirst().getExplanation().contains(RestrictLoadBalancing.COMPRESSION_CONFIGURATION_ERROR_MESSAGE));
    }

    @Test
    public void testGoodConfiguration() {
        // Set attribute compression to false while content compression is true (and no load balancing strategy is allowed)
        // This is a violation only if some form of load balancing is allowed
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, "false");
        ArrayList<ValidationResult> validationErrors = new ArrayList<>(rule.customValidate(validationContext));
        assertEquals(0, validationErrors.size());
    }

    @Test
    public void testNoViolations() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing_noViolation.json",
                List.of()
        );
    }

    @Test
    public void testViolationRoundRobin() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing_roundRobin.json",
                // Upstream processor UUID : aa0e758e-0194-1000-a703-6b124a357ce2
                // Connection UUID         : aa0ea254-0194-1000-c64b-45b5c17f2429
                // Connection configuration: Round robin load balancing
                List.of(
                        "aa0e758e-0194-1000-a703-6b124a357ce2"
                )
        );
    }

    @Test
    public void testViolationSingleNode() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing_singleNode.json",
                // Upstream processor UUID : aa0e758e-0194-1000-a703-6b124a357ce2
                // Connection UUID         : aa0ea254-0194-1000-c64b-45b5c17f2429
                // Connection configuration: Single node load balancing
                List.of(
                        "aa0e758e-0194-1000-a703-6b124a357ce2"
                )
        );
    }

    @Test
    public void testViolationPartition() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing_partition.json",
                // Upstream processor UUID : aa0e758e-0194-1000-a703-6b124a357ce2
                // Connection UUID         : aa0ea254-0194-1000-c64b-45b5c17f2429
                // Connection configuration: Partition by attribute load balancing
                List.of(
                        "aa0e758e-0194-1000-a703-6b124a357ce2"
                )
        );
    }

    @Test
    public void testViolationAttributeCompression() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, "true");
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, "false");
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, "false");
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing_roundRobin_compressAttributes.json",
                // Upstream processor UUID : aa0e758e-0194-1000-a703-6b124a357ce2
                // Connection UUID         : aa0ea254-0194-1000-c64b-45b5c17f2429
                // Connection configuration: Round robin load balancing, Compress attributes only
                List.of(
                        "aa0e758e-0194-1000-a703-6b124a357ce2"
                )
        );
    }

    @Test
    public void testViolationContentCompression() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, "true");
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, "true");
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, "false");
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing_roundRobin_compressAttributesAndContent.json",
                // Upstream processor UUID : aa0e758e-0194-1000-a703-6b124a357ce2
                // Connection UUID         : aa0ea254-0194-1000-c64b-45b5c17f2429
                // Connection configuration: Round robin load balancing, Compress attributes and content
                List.of(
                        "aa0e758e-0194-1000-a703-6b124a357ce2"
                )
        );
    }

    @Test
    public void testNoViolationContentCompression() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, "true");
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, "true");
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, "true");
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing_roundRobin_compressAttributesAndContent.json",
                // Upstream processor UUID : aa0e758e-0194-1000-a703-6b124a357ce2
                // Connection UUID         : aa0ea254-0194-1000-c64b-45b5c17f2429
                // Connection configuration: Round robin load balancing, Compress attributes and content
                List.of()
        );
    }
}