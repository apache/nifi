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

package org.apache.nifi.minifi.c2.command;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.stream.Stream;
import org.apache.nifi.util.NiFiProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TransferDebugCommandHelperTest {

    private TransferDebugCommandHelper transferDebugCommandHelper;

    @BeforeEach
    public void setUp() {
        NiFiProperties niFiProperties = new NiFiProperties();
        transferDebugCommandHelper = new TransferDebugCommandHelper(niFiProperties);
    }

    private static Stream<Arguments> textAndExpectedResult() {
        return Stream.of(
            Arguments.of("nifi.minifi.sensitive.props.key", false),
            Arguments.of("nifi.minifi.sensitive.props.algorithm", false),
            Arguments.of("key:", false),
            Arguments.of("algorithm:", false),
            Arguments.of("secret.key", false),
            Arguments.of("c2.security.truststore.password", false),
            Arguments.of("c2.security.keystore.password", false),
            Arguments.of("nifi.minifi.security.keystorePasswd", false),
            Arguments.of("nifi.minifi.security.truststorePasswd", false),
            Arguments.of("nifi.minifi.security.keystore", true),
            Arguments.of("nifi.minifi.security.truststore", true),
            Arguments.of("nifi.minifi.flow.use.parent.ssl", true),
            Arguments.of("nifi.minifi.status.reporter", true),
            Arguments.of("nifi.minifi.security.ssl.protocol", true),
            Arguments.of("", true),
            Arguments.of(null, true),
            Arguments.of("nifi", true)
        );
    }

    @ParameterizedTest
    @MethodSource("textAndExpectedResult")
    public void testSensitiveTextIsExcluded(String propertyName, boolean expectedResult) {
        assertEquals(expectedResult, transferDebugCommandHelper.excludeSensitiveText(propertyName));
    }
}
