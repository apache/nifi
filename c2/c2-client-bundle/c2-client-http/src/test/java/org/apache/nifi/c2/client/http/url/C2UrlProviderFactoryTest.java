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

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.apache.nifi.c2.client.C2ClientConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class C2UrlProviderFactoryTest {

    private static final String C2_REST_BASE = "https://host:8080/c2/api";
    private static final String HEARTBEAT_PATH = "/heartbeat";
    private static final String ACKNOWLEDGE_PATH = "/acknowledge";

    @Mock
    private C2ClientConfig clientConfig;
    @InjectMocks
    private C2UrlProviderFactory testC2UrlProviderFactory;

    @Test
    public void testProxyAwareC2UrlProviderIsCreated() {
        // given
        when(clientConfig.getC2RestPathBase()).thenReturn(C2_REST_BASE);
        when(clientConfig.getC2RestPathHeartbeat()).thenReturn(HEARTBEAT_PATH);
        when(clientConfig.getC2RestPathAcknowledge()).thenReturn(ACKNOWLEDGE_PATH);

        // when
        C2UrlProvider c2UrlProvider = testC2UrlProviderFactory.create();

        // then
        assertInstanceOf(ProxyAwareC2UrlProvider.class, c2UrlProvider);
    }

    @Test
    public void testLegacyC2UrlProviderIsCreated() {
        // given
        when(clientConfig.getC2Url()).thenReturn(C2_REST_BASE + HEARTBEAT_PATH);
        when(clientConfig.getC2AckUrl()).thenReturn(C2_REST_BASE + ACKNOWLEDGE_PATH);

        // when
        C2UrlProvider c2UrlProvider = testC2UrlProviderFactory.create();

        // then
        assertInstanceOf(LegacyC2UrlProvider.class, c2UrlProvider);
    }

    @Test
    public void testProxyAwareProviderTakesPrecedenceOverLegacy() {
        // given
        lenient().when(clientConfig.getC2RestPathBase()).thenReturn(C2_REST_BASE);
        lenient().when(clientConfig.getC2RestPathHeartbeat()).thenReturn(HEARTBEAT_PATH);
        lenient().when(clientConfig.getC2RestPathAcknowledge()).thenReturn(ACKNOWLEDGE_PATH);
        lenient().when(clientConfig.getC2Url()).thenReturn(C2_REST_BASE + HEARTBEAT_PATH);
        lenient().when(clientConfig.getC2AckUrl()).thenReturn(C2_REST_BASE + ACKNOWLEDGE_PATH);

        // when
        C2UrlProvider c2UrlProvider = testC2UrlProviderFactory.create();

        // then
        assertInstanceOf(ProxyAwareC2UrlProvider.class, c2UrlProvider);
    }

    @Test
    public void testInsufficientConfigurationResultsInException() {
        // given
        when(clientConfig.getC2RestPathBase()).thenReturn(C2_REST_BASE);
        when(clientConfig.getC2Url()).thenReturn(C2_REST_BASE + HEARTBEAT_PATH);

        // when + then
        assertThrowsExactly(IllegalArgumentException.class, testC2UrlProviderFactory::create);
    }
}
