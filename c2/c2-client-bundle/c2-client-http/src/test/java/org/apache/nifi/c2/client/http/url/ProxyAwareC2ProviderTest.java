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
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;

import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ProxyAwareC2ProviderTest {

    @MethodSource("testInsufficientProviderConstructorArguments")
    @ParameterizedTest(name = "{index} => c2RestApi={0}, c2RestPathHeartbeat={1}, c2RestPathAcknowledge={2}")
    public void testExceptionIsThrownWhenUrlsCanNotBeCreatedFromInputParameters(String c2RestApi, String c2RestPathHeartbeat, String c2RestPathAcknowledge) {
        assertThrowsExactly(IllegalArgumentException.class, () -> new ProxyAwareC2UrlProvider(c2RestApi, c2RestPathHeartbeat, c2RestPathAcknowledge));
    }

    private static Stream<Arguments> testInsufficientProviderConstructorArguments() {
        return Stream.of(
            Arguments.of(null, null, null),
            Arguments.of(null, null, ""),
            Arguments.of(null, "", ""),
            Arguments.of("", "", ""),
            Arguments.of("", "", null),
            Arguments.of("", null, null),
            Arguments.of("http://c2/api", null, null),
            Arguments.of("http://c2/api", "", null),
            Arguments.of("http://c2/api", null, ""),
            Arguments.of("http://c2/api", "", ""),
            Arguments.of(null, "path1", null),
            Arguments.of(null, null, "path2"),
            Arguments.of(null, "path1", "path2"),
            Arguments.of("invalid_url/api", "path1", "path2")
        );
    }

    @MethodSource("testValidProviderConstructorArguments")
    @ParameterizedTest(name = "{index} => c2RestApi={0}, c2RestPathHeartbeat={1}, c2RestPathAcknowledge={2}, expectedHeartbeatUrl={3}, expectedAcknowledgeUrl={4}")
    public void testUrlProviderIsCreatedAndHeartbeatAndAcknowledgeUrlsAreReturnedCorrectly(String c2RestApi, String c2RestPathHeartbeat, String c2RestPathAcknowledge,
                                                                                           String expectedHeartbeatUrl, String expectedAcknowledgeUrl) {
        ProxyAwareC2UrlProvider testProvider = new ProxyAwareC2UrlProvider(c2RestApi, c2RestPathHeartbeat, c2RestPathAcknowledge);

        assertEquals(expectedHeartbeatUrl, testProvider.getHeartbeatUrl());
        assertEquals(expectedAcknowledgeUrl, testProvider.getAcknowledgeUrl());
    }

    private static Stream<Arguments> testValidProviderConstructorArguments() {
        String expectedHearbeatUrl = "http://c2/api/path1";
        String expectedAckUrl = "http://c2/api/path2";
        return Stream.of(
            Arguments.of("http://c2/api", "path1", "path2", expectedHearbeatUrl, expectedAckUrl),
            Arguments.of("http://c2/api", "/path1", "path2", expectedHearbeatUrl, expectedAckUrl),
            Arguments.of("http://c2/api", "path1", "/path2", expectedHearbeatUrl, expectedAckUrl),
            Arguments.of("http://c2/api", "/path1", "/path2", expectedHearbeatUrl, expectedAckUrl),
            Arguments.of("http://c2/api/", "path1", "path2", expectedHearbeatUrl, expectedAckUrl),
            Arguments.of("http://c2/api/", "/path1", "path2", expectedHearbeatUrl, expectedAckUrl),
            Arguments.of("http://c2/api/", "path1", "/path2", expectedHearbeatUrl, expectedAckUrl),
            Arguments.of("http://c2/api/", "/path1", "/path2", expectedHearbeatUrl, expectedAckUrl)
        );
    }

    @MethodSource("testCallbackUrlProvidedArguments")
    @ParameterizedTest(name = "{index} => c2RestBase={0}, absoluteUrl={1}, relativeUrl={2}, expectedCallbackUrl={3}")
    public void testCallbackUrlProvidedForValidInputs(String c2RestBase, String absoluteUrl, String relativeUrl, String expectedCallbackUrl) {
        ProxyAwareC2UrlProvider testProvider = new ProxyAwareC2UrlProvider(c2RestBase, "any_path", "any_path");
        assertEquals(expectedCallbackUrl, testProvider.getCallbackUrl(absoluteUrl, relativeUrl));
    }

    @MethodSource("testCallbackUrlProvidedInvalidArguments")
    @ParameterizedTest(name = "{index} => c2RestBase={0}, absoluteUrl={1}, relativeUrl={2}, expectedCallbackUrl={3}")
    public void testCallbackUrlProvidedForInvalidInputs(String c2RestBase, String absoluteUrl, String relativeUrl) {
        ProxyAwareC2UrlProvider testProvider = new ProxyAwareC2UrlProvider(c2RestBase, "any_path", "any_path");
        assertThrows(IllegalArgumentException.class, () -> testProvider.getCallbackUrl(absoluteUrl, relativeUrl));
    }

    private static Stream<Arguments> testCallbackUrlProvidedArguments() {
        String c2RestBaseNoTrailingSlash = "http://c2/api";
        String c2RestBaseWithTrailingSlash = "http://c2/api/";
        String path = "path/endpoint";
        String absoluteUrl = "http://c2-other/api/path/endpoint";
        return Stream.of(
            Arguments.of(c2RestBaseNoTrailingSlash, null, path, c2RestBaseWithTrailingSlash + path),
            Arguments.of(c2RestBaseNoTrailingSlash, "", "/" + path, c2RestBaseWithTrailingSlash + path),
            Arguments.of(c2RestBaseWithTrailingSlash, null, path, c2RestBaseWithTrailingSlash + path),
            Arguments.of(c2RestBaseWithTrailingSlash, "", "/" + path, c2RestBaseWithTrailingSlash + path),
            Arguments.of(c2RestBaseWithTrailingSlash, absoluteUrl, null, absoluteUrl),
            Arguments.of(c2RestBaseWithTrailingSlash, absoluteUrl, "", absoluteUrl)
        );
    }

    private static Stream<Arguments> testCallbackUrlProvidedInvalidArguments() {
        String c2RestBaseNoTrailingSlash = "http://c2/api";
        String c2RestBaseWithTrailingSlash = "http://c2/api/";
        return Stream.of(
            Arguments.of(c2RestBaseNoTrailingSlash, null, null, Optional.empty()),
            Arguments.of(c2RestBaseNoTrailingSlash, "", null, Optional.empty()),
            Arguments.of(c2RestBaseNoTrailingSlash, null, "", Optional.empty()),
            Arguments.of(c2RestBaseNoTrailingSlash, "", "", Optional.empty()),
            Arguments.of(c2RestBaseWithTrailingSlash, null, null, Optional.empty()),
            Arguments.of(c2RestBaseWithTrailingSlash, "", null, Optional.empty()),
            Arguments.of(c2RestBaseWithTrailingSlash, null, "", Optional.empty()),
            Arguments.of(c2RestBaseWithTrailingSlash, "", "", Optional.empty())
        );
    }
}
