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
package org.apache.nifi.kafka.shared.login;

import org.junit.jupiter.api.Test;

import static javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LoginConfigBuilderTest {

    @Test
    void createExampleJaasConfigTest() {
        String expectedConfig = "test.class.name required booleanFlag=true numberFlag=1 stringFlag=\"string-flag\";";

        LoginConfigBuilder builder = new LoginConfigBuilder("test.class.name", REQUIRED);
        builder.append("booleanFlag", Boolean.TRUE);
        builder.append("numberFlag", 1);
        builder.append("stringFlag", "string-flag");

        assertEquals(expectedConfig, builder.build());
    }
}