/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.gitea;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class GiteaFlowRegistryClientTest {

    @Mock
    private FlowRegistryClientConfigurationContext context;

    private final GiteaFlowRegistryClient client = new GiteaFlowRegistryClient();

    @BeforeEach
    void setContext() {
        setProperty(GiteaFlowRegistryClient.GITEA_API_URL, "https://gitea.example.com/");
        setProperty(GiteaFlowRegistryClient.REPOSITORY_OWNER, "nifi");
        setProperty(GiteaFlowRegistryClient.REPOSITORY_NAME, "flows");
    }

    @ParameterizedTest
    @MethodSource("storageLocationArgs")
    void testIsStorageLocationApplicable(final String location, final boolean applicable) {
        assertEquals(applicable, client.isStorageLocationApplicable(context, location));
    }

    @ParameterizedTest
    @MethodSource("apiUrlArgs")
    void testIsSupportedUrl(final String url, final boolean supported) {
        assertEquals(supported, GiteaFlowRegistryClient.isSupportedUrl(url));
    }

    private static Stream<Arguments> storageLocationArgs() {
        return Stream.of(
                Arguments.argumentSet("Same repository", "https://gitea.example.com/nifi/flows", true),
                Arguments.argumentSet("Same repository with suffix", "https://gitea.example.com/nifi/flows.git", true),
                Arguments.argumentSet("Same repository different case", "https://Gitea.example.com/NiFi/Flows", true),
                Arguments.argumentSet("Different repository", "https://gitea.example.com/nifi/other", false),
                Arguments.argumentSet("Different host", "https://codeberg.org/nifi/flows", false),
                Arguments.argumentSet("GitHub location", "git@github.com:nifi/flows.git", false),
                Arguments.argumentSet("Null location", null, false)
        );
    }

    private static Stream<Arguments> apiUrlArgs() {
        return Stream.of(
                Arguments.argumentSet("HTTPS", "https://gitea.example.com", true),
                Arguments.argumentSet("HTTP with port and context path", "http://10.0.0.1:3000/gitea", true),
                Arguments.argumentSet("SSH", "ssh://git@gitea.example.com", false),
                Arguments.argumentSet("Host only", "gitea.example.com", false)
        );
    }

    private void setProperty(final PropertyDescriptor descriptor, final String value) {
        final PropertyValue propertyValue = mock(PropertyValue.class);
        lenient().when(propertyValue.getValue()).thenReturn(value);
        lenient().when(context.getProperty(descriptor)).thenReturn(propertyValue);
    }
}
