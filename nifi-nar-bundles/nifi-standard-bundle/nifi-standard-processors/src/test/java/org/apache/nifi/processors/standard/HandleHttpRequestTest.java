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

import org.apache.nifi.http.HttpContextMap;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HandleHttpRequestTest {

    private static final String CONTEXT_MAP_ID = HttpContextMap.class.getSimpleName();

    private static final String MINIMUM_THREADS = "8";

    @Mock
    HttpContextMap httpContextMap;

    TestRunner runner;

    @BeforeEach
    void setRunner() throws InitializationException {
        runner = TestRunners.newTestRunner(HandleHttpRequest.class);

        when(httpContextMap.getIdentifier()).thenReturn(CONTEXT_MAP_ID);
        runner.addControllerService(CONTEXT_MAP_ID, httpContextMap);
        runner.enableControllerService(httpContextMap);
    }

    @AfterEach
    void shutdown() {
        runner.shutdown();
    }

    @Test
    void testSetRequiredProperties() {
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);

        runner.assertValid();
    }

    @Test
    void testRun() {
        runner.setProperty(HandleHttpRequest.HTTP_CONTEXT_MAP, CONTEXT_MAP_ID);
        runner.setProperty(HandleHttpRequest.MAXIMUM_THREADS, MINIMUM_THREADS);
        runner.setProperty(HandleHttpRequest.PORT, "0");

        runner.run();

        runner.assertTransferCount(HandleHttpRequest.REL_SUCCESS, 0);
    }
}
