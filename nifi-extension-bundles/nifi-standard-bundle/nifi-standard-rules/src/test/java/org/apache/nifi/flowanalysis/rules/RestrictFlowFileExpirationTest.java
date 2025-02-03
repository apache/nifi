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

import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RestrictFlowFileExpirationTest extends AbstractFlowAnalaysisRuleTest<RestrictFlowFileExpiration> {
    @Override
    protected RestrictFlowFileExpiration initializeRule() {
        return new RestrictFlowFileExpiration();
    }

    @BeforeEach
    @Override
    public void setup() {
        super.setup();
        setProperty(RestrictFlowFileExpiration.ALLOW_ZERO, "true");
        setProperty(RestrictFlowFileExpiration.MIN_FLOWFILE_EXPIRATION, RestrictFlowFileExpiration.MIN_FLOWFILE_EXPIRATION.getDefaultValue());
        setProperty(RestrictFlowFileExpiration.MAX_FLOWFILE_EXPIRATION, RestrictFlowFileExpiration.MAX_FLOWFILE_EXPIRATION.getDefaultValue());
    }

    @Test
    public void testBadConfiguration() {
        setProperty(RestrictFlowFileExpiration.MIN_FLOWFILE_EXPIRATION, "2 days");
        setProperty(RestrictFlowFileExpiration.MAX_FLOWFILE_EXPIRATION, "1 day");
        Collection<ValidationResult> results = rule.customValidate(validationContext);
        assertEquals(1, results.size());
        results.forEach(result -> {
            assertFalse(result.isValid());
            assertEquals(RestrictFlowFileExpiration.MIN_FLOWFILE_EXPIRATION.getName(), result.getSubject());
            assertTrue(result.getExplanation().contains("cannot be greater than"));
        });
    }

    @Test
    public void testNoViolations() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictFlowFileExpiration/RestrictFlowFileExpiration_noViolation.json",
                List.of()
        );
    }

    @Test
    public void testViolationsAllowZeroTrue() throws Exception {
        testAnalyzeProcessGroup(
                "src/test/resources/RestrictFlowFileExpiration/RestrictFlowFileExpiration.json",
                List.of(
                        "e26f3857-0192-1000-776a-3d62e15f75dc", // connection from UpdateAttribute to funnel, 1 sec
                        "e27073f8-0192-1000-cf43-9c41e69eadd2" // connection from output port to funnel, 45 days
                )
        );
    }

    @Test
    public void testViolationsAllowZeroFalse() throws Exception {
        setProperty(RestrictFlowFileExpiration.ALLOW_ZERO, "false");

        testAnalyzeProcessGroup(
                "src/test/resources/RestrictFlowFileExpiration/RestrictFlowFileExpiration.json",
                List.of(
                        "e26f20c1-0192-1000-ff8b-bcf395c02076", // GenerateFlowFile, GenerateFlowFile connected to UpdateAttribute, 0 sec
                        "e26f3857-0192-1000-776a-3d62e15f75dc", // UpdateAttribute, UpdateAttribute connected to Funnel, 1 sec
                        "e26f3857-0192-1000-776a-3d62e15f75dc", // UpdateAttribute, Funnel connected to UpdateAttribute, 0 sec
                        "e26fd0d5-0192-1000-ee3d-f90141590475", // connection from Funnel to Funnel, 0 sec
                        "e27073f8-0192-1000-cf43-9c41e69eadd2"  // connection from Process Group output port to Funnel, 45 days
                )
        );
    }
}
