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
package org.apache.nifi.security.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TlsConfigurationTest {

    @ParameterizedTest
    @ValueSource(strings = {"1.5.0", "1.6.0", "1.7.0.123", "1.8.0.231", "9.0.1", "10.1.2", "11.2.3", "12.3.456"})
    public void testParseJavaVersion(String version) {
        int javaVersion = TlsConfiguration.parseJavaVersion(version);

        assertTrue(javaVersion >= 5 && javaVersion <= 12);
    }

    @Test
    @EnabledForJreRange(min = JRE.JAVA_11)
    public void testGeSupportedTlsProtocolVersionsForJava11AndHigher() {
        assertArrayEquals(new String[]{"TLSv1.3", "TLSv1.2"},
                TlsConfiguration.getCurrentSupportedTlsProtocolVersions());
    }

    @Test
    @EnabledForJreRange(max = JRE.JAVA_10)
    public void testGeSupportedTlsProtocolVersionsForJava10AndLower() {
        assertArrayEquals(new String[]{"TLSv1.2"},
                TlsConfiguration.getCurrentSupportedTlsProtocolVersions());
    }

    @Test
    @EnabledForJreRange(min = JRE.JAVA_11)
    public void testGetHighestCurrentSupportedTlsProtocolVersionForJava11AndHigher() {
        assertEquals("TLSv1.3", TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
    }

    @Test
    @EnabledForJreRange(max = JRE.JAVA_10)
    public void testGetHighestCurrentSupportedTlsProtocolVersionForJava10AndLower() {
        assertEquals("TLSv1.2", TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion());
    }
}
