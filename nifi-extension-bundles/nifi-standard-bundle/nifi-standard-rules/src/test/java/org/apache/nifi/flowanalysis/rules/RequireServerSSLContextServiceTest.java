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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowanalysis.ComponentAnalysisResult;
import org.apache.nifi.ssl.SSLContextProvider;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RequireServerSSLContextServiceTest extends AbstractFlowAnalaysisRuleTest<RequireServerSSLContextService> {

    public static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("SSL Context Service enables support for HTTPS")
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();
    @Override
    protected RequireServerSSLContextService initializeRule() {
        return new RequireServerSSLContextService();
    }

    @Test
    public void testNoViolations() throws Exception {
        setProperty(SSL_CONTEXT_SERVICE, "9c50e433-c2aa-3d19-aae6-20299f4ac38c");
        testAnalyzeProcessors(
                "src/test/resources/RequireServerSSLContextService/RequireServerSSLContextService_noViolation.json",
                List.of()
        );
    }

    @Test
    public void testViolations() throws Exception {
        setProperty(SSL_CONTEXT_SERVICE, null);

        ComponentAnalysisResult expectedResult = new ComponentAnalysisResult("b5734be4-0195-1000-0e75-bc0f150d06bd", "'ListenHTTP' is not allowed");
        testAnalyzeProcessors(
                "src/test/resources/RequireServerSSLContextService/RequireServerSSLContextService.json",
                List.of(
                        expectedResult  // processor ListenHttp with no SSLContextService set
                )
        );
    }
}
