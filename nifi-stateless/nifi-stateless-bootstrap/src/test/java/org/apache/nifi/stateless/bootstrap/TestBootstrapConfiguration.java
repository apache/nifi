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

package org.apache.nifi.stateless.bootstrap;

import org.apache.nifi.stateless.config.ParameterOverride;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

public class TestBootstrapConfiguration {

    private final String engineConfigPropertiesFilename = "src/test/resources/nifi-stateless.properties";

    @Test
    public void testParseParameterOverride() {
        final BootstrapConfiguration configuration = BootstrapConfiguration.fromCommandLineArgs(new String[] {"-e", engineConfigPropertiesFilename, "-f", engineConfigPropertiesFilename});

        testOverride(configuration, "a:b=c", "a", "b", "c"); // simple case, context name, param name, param value, no special chars
        testOverride(configuration, "a=b", null, "a", "b"); // test no context name
        testOverride(configuration, "a\\:b:c=d", "a:b", "c", "d"); // test escaped colon in context name
        testOverride(configuration, "a:b:c=d", "a", "b:c", "d"); // test colon in param name
        testOverride(configuration, "a=b:c", null, "a", "b:c"); // test colon in param value
        testOverride(configuration, "a=b=c", null, "a", "b=c"); // test equals in param value
        testOverride(configuration, "a b:c d=e f g", "a b", "c d", "e f g"); // test spaces

        // Any input that doesn't contain an equals should fail
        testParseFailure(configuration, "a");
        testParseFailure(configuration, "a:b");
        testParseFailure(configuration, "a:b:c");

        testParseFailure(configuration, "=c");
    }

    private void testOverride(final BootstrapConfiguration configuration, final String argument, final String contextName, final String parameterName, final String parameterValue) {
        final ParameterOverride override = configuration.parseOverride(argument);
        assertEquals(contextName, override.getContextName());
        assertEquals(parameterName, override.getParameterName());
        assertEquals(parameterValue, override.getParameterValue());
    }

    private void testParseFailure(final BootstrapConfiguration configuration, final String argument) {
        try {
            configuration.parseOverride(argument);
            Assert.fail("Expected an IllegalArgumentException");
        } catch (final IllegalArgumentException expected) {
        }
    }
}
