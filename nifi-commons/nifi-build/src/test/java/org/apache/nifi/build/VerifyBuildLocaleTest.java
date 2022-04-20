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
package org.apache.nifi.build;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.platform.commons.util.StringUtils;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class VerifyBuildLocaleTest {

    /**
     * Verify that the intended system locale is active for the surefire invocation.
     *
     * NiFi Surefire invocations may be executed in the context of a particular locale, in order to verify the locale
     * independence of the code base.
     *
     * If "NIFI_CI_LOCALE" is defined, then it must specify each system property enumerated in this test case.
     */
    @Test
    @EnabledIfEnvironmentVariable(named = "NIFI_CI_LOCALE", matches = ".+",
            disabledReason = "NiFi CI build sets this environment variable; it will not normally be set")
    public void testEnvironmentLocaleMatchesSystemProperties() {
        final String ciLocale = System.getenv("NIFI_CI_LOCALE");
        // if the flag variable is set, verify the system locale of the surefire process against NIFI_CI_LOCALE
        final String[] systemPropertyKeys = {
                "user.language",
                "user.country",
        };
        for (String key : systemPropertyKeys) {
            assertTrue(ciLocale.contains(key));
            final String value = System.getProperty(key);
            final String message = String.format(
                    "system property - CI_LOCALE:[%s] ACTUAL:[%s=%s]", ciLocale, key, value);
            assertTrue(StringUtils.isNotBlank(value), message);
            final String expected = String.format("%s=%s", key, value);
            assertTrue(ciLocale.contains(expected), message);
        }
    }
}
