package org.apache.nifi.parameter;/*
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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockParameterProviderInitializationContext;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestOnePasswordParameterProvider {

    public static final String WEB_CLIENT = "web-client-service-provider";

    private OnePasswordParameterProvider parameterProvider;
    private MockParameterProviderInitializationContext initializationContext;

    private Map<PropertyDescriptor, String> properties;

    @BeforeEach
    public void init() throws InitializationException {
        final WebClientServiceProvider webClient = mock(WebClientServiceProvider.class);

        final OnePasswordParameterProvider rawProvider = new OnePasswordParameterProvider();
        initializationContext = new MockParameterProviderInitializationContext("id", "name", mock(ComponentLog.class));
        initializationContext.addControllerService(webClient, WEB_CLIENT);
        rawProvider.initialize(initializationContext);
        parameterProvider = spy(rawProvider);

        final WebClientService webClientService = mock(WebClientService.class);
        when(webClient.getWebClientService()).thenReturn(webClientService);

        final HttpUriBuilder uriBuilder = new StandardHttpUriBuilder();
        when(webClient.getHttpUriBuilder()).thenReturn(uriBuilder);

        final HttpRequestUriSpec uriSpec = mock(HttpRequestUriSpec.class);
        when(webClientService.get()).thenReturn(uriSpec);

        // list vaults
        final HttpRequestBodySpec bodySpec = mock(HttpRequestBodySpec.class);
        when(uriSpec.uri(argThat(argument -> argument == null || argument.getPath().endsWith("/vaults")))).thenReturn(bodySpec);
        when(bodySpec.header(any(), any())).thenReturn(bodySpec);
        final HttpResponseEntity httpEntity = mock(HttpResponseEntity.class);
        when(bodySpec.retrieve()).thenReturn(httpEntity);

        final String responseBody = """
                [
                  {
                    "id": "qeo4jajm7azfh3wnsynbmr5lem",
                    "name": "Engineering"
                  }
                ]
                """;
        InputStream response = new ByteArrayInputStream(responseBody.getBytes());
        when(httpEntity.body()).thenReturn(response);

        // list items
        final HttpRequestBodySpec bodySpecItems = mock(HttpRequestBodySpec.class);
        when(uriSpec.uri(argThat(argument -> argument == null || argument.getPath().endsWith("/items")))).thenReturn(bodySpecItems);
        when(bodySpecItems.header(any(), any())).thenReturn(bodySpecItems);
        final HttpResponseEntity httpEntityItems = mock(HttpResponseEntity.class);
        when(bodySpecItems.retrieve()).thenReturn(httpEntityItems);

        final String responseBodyItems = """
                [
                  {
                    "category": "DATABASE",
                    "id": "evsdsvep67jitka2hmbg5tbxry",
                    "title": "POSTGRES",
                    "vault": {
                      "id": "qeo4jajm7azfh3wnsynbmr5lem",
                      "name": "Engineering"
                    }
                  }
                ]
                """;
        InputStream responseItems = new ByteArrayInputStream(responseBodyItems.getBytes());
        when(httpEntityItems.body()).thenReturn(responseItems);

        // get item
        final HttpRequestBodySpec bodySpecItem = mock(HttpRequestBodySpec.class);
        when(uriSpec.uri(argThat(argument -> argument == null || argument.getPath().endsWith("evsdsvep67jitka2hmbg5tbxry")))).thenReturn(bodySpecItem);
        when(bodySpecItem.header(any(), any())).thenReturn(bodySpecItem);
        final HttpResponseEntity httpEntityItem = mock(HttpResponseEntity.class);
        when(bodySpecItem.retrieve()).thenReturn(httpEntityItem);

        final String responseBodyItem = """
                {
                  "fields": [
                    {
                      "id": "notesPlain",
                      "type": "STRING"
                    },
                    {
                      "id": "database_type",
                      "value": "postgresql"
                    },
                    {
                      "id": "hostname",
                      "value": "localhost"
                    },
                    {
                      "id": "port",
                      "value": "5432"
                    },
                    {
                      "id": "database",
                      "value": "mydatabase"
                    },
                    {
                      "id": "username",
                      "value": "postgres"
                    },
                    {
                      "id": "password",
                      "value": "thisisabadpassword"
                    }
                  ],
                  "id": "evsdsvep67jitka2hmbg5tbxry",
                  "title": "POSTGRES"
                }
                """;
        InputStream responseItem = new ByteArrayInputStream(responseBodyItem.getBytes());
        when(httpEntityItem.body()).thenReturn(responseItem);

        properties = new HashMap<>();
        properties.put(OnePasswordParameterProvider.WEB_CLIENT_SERVICE_PROVIDER, WEB_CLIENT);
        properties.put(OnePasswordParameterProvider.ACCESS_TOKEN, "token");
        properties.put(OnePasswordParameterProvider.CONNECT_SERVER, "http://localhost:8080");
    }

    @Test
    public void testFetchParameters() {
        final ConfigurationContext context = new MockConfigurationContext(properties, initializationContext, null);
        final List<ParameterGroup> groups = parameterProvider.fetchParameters(context);
        assertEquals(1, groups.size());
        assertEquals(6, groups.getFirst().getParameters().size());
    }
}
