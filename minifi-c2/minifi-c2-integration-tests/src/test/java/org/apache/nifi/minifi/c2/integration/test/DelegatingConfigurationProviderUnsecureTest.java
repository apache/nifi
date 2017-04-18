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

package org.apache.nifi.minifi.c2.integration.test;

import com.palantir.docker.compose.DockerComposeRule;
import org.apache.nifi.minifi.c2.integration.test.health.HttpStatusCodeHealthCheck;
import org.junit.Before;
import org.junit.ClassRule;

public class DelegatingConfigurationProviderUnsecureTest extends AbstractTestUnsecure {
    @ClassRule
    public static DockerComposeRule docker = DockerComposeRule.builder()
            .file("target/test-classes/docker-compose-DelegatingProviderUnsecureTest.yml")
            .waitingForService("c2-upstream", new HttpStatusCodeHealthCheck(DelegatingConfigurationProviderUnsecureTest::getUnsecureConfigUrl, 400))
            .waitingForService("c2", new HttpStatusCodeHealthCheck(DelegatingConfigurationProviderUnsecureTest::getUnsecureConfigUrl, 400))
            .build();

    @Before
    public void setup() {
        super.setup(docker);
    }
}
