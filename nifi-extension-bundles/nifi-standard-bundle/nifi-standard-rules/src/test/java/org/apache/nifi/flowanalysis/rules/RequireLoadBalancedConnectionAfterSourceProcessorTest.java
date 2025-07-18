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

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;


public class RequireLoadBalancedConnectionAfterSourceProcessorTest extends AbstractFlowAnalaysisRuleTest<RequireLoadBalancedConnectionAfterSourceProcessor> {

    @Test
    public void testViolations() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RequireLoadBalancedConnectionAfterSourceProcessor/RequireLoadBalancedConnectionAfterSourceProcessor_Violation.json",
                List.of(
                        "f54fbff5-0197-1000-1c04-924d2ac6cc8e" // processor GenerateFlowFile connecting to a funnel with no load-balancing strategy
                )
        );
    }

    @Test
    public void testNoViolations() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RequireLoadBalancedConnectionAfterSourceProcessor/RequireLoadBalancedConnectionAfterSourceProcessor_No_Violation.json",
                Collections.emptyList()
        );
    }

    @Override
    protected RequireLoadBalancedConnectionAfterSourceProcessor initializeRule() {
        return new RequireLoadBalancedConnectionAfterSourceProcessor();
    }
}