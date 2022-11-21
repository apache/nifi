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
package org.apache.nifi.processors.email;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ConsumeEWSTest {

    private static final String LOCALHOST_URL = "http://localhost:65000";

    TestRunner runner;

    @BeforeEach
    void setRunner() {
        runner = TestRunners.newTestRunner(ConsumeEWS.class);
    }

    @Test
    void testRequiredProperties() {
        runner.setProperty(ConsumeEWS.USER, String.class.getSimpleName());
        runner.setProperty(ConsumeEWS.PASSWORD, String.class.getName());

        runner.assertValid();
    }

    @Test
    void testRunFailure() {
        runner.setProperty(ConsumeEWS.USER, String.class.getSimpleName());
        runner.setProperty(ConsumeEWS.PASSWORD, String.class.getName());
        runner.setProperty(ConsumeEWS.USE_AUTODISCOVER, Boolean.FALSE.toString());
        runner.setProperty(ConsumeEWS.EWS_URL, LOCALHOST_URL);

        assertThrows(AssertionError.class, runner::run);
    }
}
