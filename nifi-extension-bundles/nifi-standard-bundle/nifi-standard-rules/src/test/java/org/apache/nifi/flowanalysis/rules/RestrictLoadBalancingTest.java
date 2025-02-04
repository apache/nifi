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
    // The flow consists of six upstream GenerateFlowFile processors each having one connection to an UpdateAttribute processor
    // The UUID of the upstream processor is specified in the following variables which describe the properties of the connection
    // The sixth upstream processor has a connection with 'Do not load balance'; it can never generate a violation
    private static final String UPSTREAM_PROCESSOR_PARTITION_BY_ATTRIBUTE_UUID = "2f3f4270-9703-331a-659c-1c583972547b";
    private static final String UPSTREAM_PROCESSOR_ROUND_ROBIN_UUID = "930d2d12-fefe-39d2-4884-fb9d9330d892";
    private static final String UPSTREAM_PROCESSOR_SINGLE_NODE_UUID = "aa0e758e-0194-1000-a703-6b124a357ce2";
    private static final String UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_UUID = "78a971d6-3015-3fc1-17c5-68b7be982aa2";
    private static final String UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_AND_CONTENT_UUID = "8792e315-8937-300c-ccbd-3130ceca40fc";

    @Override
    protected RestrictLoadBalancing initializeRule() {
        return new RestrictLoadBalancing();
    }

    @BeforeEach
    @Override
    public void setup() {
        super.setup();
        // Non-default setting, but appropriate for unit tests
        setProperty(RestrictLoadBalancing.LOAD_BALANCING_POLICY, RestrictLoadBalancing.ALLOWED);
        // Default settings
        setProperty(RestrictLoadBalancing.ALLOW_PARTITION, RestrictLoadBalancing.ALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, RestrictLoadBalancing.ALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_SINGLE_NODE, RestrictLoadBalancing.ALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, RestrictLoadBalancing.ALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, RestrictLoadBalancing.ALLOWED);
    }

    @Test
    public void testBadConfiguration() {
        // Set all load balancing strategies to 'Disallowed'
        setProperty(RestrictLoadBalancing.ALLOW_PARTITION, RestrictLoadBalancing.DISALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, RestrictLoadBalancing.DISALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_SINGLE_NODE, RestrictLoadBalancing.DISALLOWED);
        ArrayList<ValidationResult> validationErrors = new ArrayList<>(rule.customValidate(validationContext));
        assertEquals(1, validationErrors.size());
        assertTrue(validationErrors.getFirst().getExplanation().contains(RestrictLoadBalancing.CONFIGURE_STRATEGY_ERROR_MESSAGE));
    }

    @Test
    public void testGoodConfiguration() {
        // Set load balancing policy to disallowed while strategies and compression are also allowed
        setProperty(RestrictLoadBalancing.LOAD_BALANCING_POLICY, RestrictLoadBalancing.DISALLOWED);
        ArrayList<ValidationResult> validationErrors = new ArrayList<>(rule.customValidate(validationContext));
        assertEquals(0, validationErrors.size());
    }

    @Test
    public void testNoViolations() throws Exception {
        // Using unit test defaults, all load balancing configurations are allowed
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of()
        );
    }

    @Test
    public void testViolationsLoadBalancePolicy() throws Exception {
        setProperty(RestrictLoadBalancing.LOAD_BALANCING_POLICY, RestrictLoadBalancing.DISALLOWED);
        // Using unit test defaults, all load balancing configurations are allowed
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of(
                        UPSTREAM_PROCESSOR_PARTITION_BY_ATTRIBUTE_UUID,
                        UPSTREAM_PROCESSOR_ROUND_ROBIN_UUID,
                        UPSTREAM_PROCESSOR_SINGLE_NODE_UUID,
                        UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_UUID,
                        UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_AND_CONTENT_UUID
                )
        );
    }

    @Test
    public void testViolationRoundRobin() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_ROUND_ROBIN, RestrictLoadBalancing.DISALLOWED);
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of(
                        // Compression connections are also configured with Round robin strategy
                        UPSTREAM_PROCESSOR_ROUND_ROBIN_UUID,
                        UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_UUID,
                        UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_AND_CONTENT_UUID
                )
        );
    }

    @Test
    public void testViolationSingleNode() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_SINGLE_NODE, RestrictLoadBalancing.DISALLOWED);
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of(
                        UPSTREAM_PROCESSOR_SINGLE_NODE_UUID
                )
        );
    }

    @Test
    public void testViolationPartition() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_PARTITION, RestrictLoadBalancing.DISALLOWED);
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of(
                        UPSTREAM_PROCESSOR_PARTITION_BY_ATTRIBUTE_UUID
                )
        );
    }

    @Test
    public void testViolationAttributeCompression() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, RestrictLoadBalancing.DISALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, RestrictLoadBalancing.DISALLOWED);
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of(
                        UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_UUID,
                        UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_AND_CONTENT_UUID
                )
        );
    }

    @Test
    public void testViolationContentCompression() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, RestrictLoadBalancing.DISALLOWED);
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of(
                        UPSTREAM_PROCESSOR_RR_COMPRESS_ATTRIBUTES_AND_CONTENT_UUID
                )
        );
    }

    @Test
    public void testNoViolationContentCompression() throws Exception {
        setProperty(RestrictLoadBalancing.ALLOW_ATTRIBUTE_COMPRESSION, RestrictLoadBalancing.ALLOWED);
        setProperty(RestrictLoadBalancing.ALLOW_CONTENT_COMPRESSION, RestrictLoadBalancing.ALLOWED);
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictLoadBalancing/RestrictLoadBalancing.json",
                List.of()
        );
    }
}