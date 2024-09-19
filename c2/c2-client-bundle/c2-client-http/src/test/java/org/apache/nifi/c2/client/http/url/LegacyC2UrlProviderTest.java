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

package org.apache.nifi.c2.client.http.url;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class LegacyC2UrlProviderTest {

    private static final String C2_HEARTBEAT_URL = "https://host:8080/c2/api/heartbeat";
    private static final String C2_ACKNOWLEDGE_URL = "https://host:8080/c2/api/acknowledge";
    private static final String ABSOLUTE_URL = "http://c2/api/callback";
    private static final String RELATIVE_URL = "any_url";
    private static final String EXPECTED_URL = "http://c2/api/callback";

    @Test
    public void testProviderIsCreatedAndReturnsProperHeartbeatAndAcknowledgeUrls() {
        LegacyC2UrlProvider testProvider = new LegacyC2UrlProvider(C2_HEARTBEAT_URL, C2_ACKNOWLEDGE_URL);

        assertEquals(C2_HEARTBEAT_URL, testProvider.getHeartbeatUrl());
        assertEquals(C2_ACKNOWLEDGE_URL, testProvider.getAcknowledgeUrl());
    }

    @MethodSource("testCallbackUrlProvidedArguments")
    @ParameterizedTest(name = "{index} => absoluteUrl={0}, relativeUrl={1}, expectedCallbackUrl={2}")
    public void testCallbackUrlProvidedForInvalidInputs(String absoluteUrl, String relativeUrl) {
        LegacyC2UrlProvider testProvider = new LegacyC2UrlProvider(C2_HEARTBEAT_URL, C2_ACKNOWLEDGE_URL);
        assertThrows(IllegalArgumentException.class, () -> testProvider.getCallbackUrl(absoluteUrl, relativeUrl));
    }

    @Test
    public void testCallbackUrlProvidedFor() {
        LegacyC2UrlProvider testProvider = new LegacyC2UrlProvider(C2_HEARTBEAT_URL, C2_ACKNOWLEDGE_URL);
        assertEquals(EXPECTED_URL, testProvider.getCallbackUrl(ABSOLUTE_URL, RELATIVE_URL));
    }

    private static Stream<Arguments> testCallbackUrlProvidedArguments() {
        return Stream.of(
            Arguments.of(null, null, Optional.empty()),
            Arguments.of(null, "any_url", Optional.empty()),
            Arguments.of("", "", Optional.empty()),
            Arguments.of("", "any_url", Optional.empty())
        );
    }
}
