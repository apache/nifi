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
package org.apache.nifi.cluster.coordination.http.replication.client;

import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import org.apache.nifi.cluster.coordination.http.replication.PreparedRequest;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TestStandardHttpReplicationClient {

    private static final String GET_METHOD = "GET";

    private static final String POST_METHOD = "POST";

    private static final byte[] EMPTY_MAP_SERIALIZED = new byte[]{123, 125};

    private static final String CONTENT_TYPE_LOWERCASED = "content-type";

    private static final String APPLICATION_JSON = "application/json";

    private static final URI REPLICATE_URI = URI.create("http://localhost/nifi-api/flow/current-user");

    private static final String URI_QUERY = "recursive=false";

    private static final URI REPLICATE_URI_QUERY = URI.create("http://localhost/nifi-api/flow/process-groups/root/status?%s".formatted(URI_QUERY));

    private static final String QUERY_PARAMETER_NAME = "revision";

    private static final String QUERY_PARAMETER_VALUE = "1";

    private static final String QUERY_EXPECTED = "%s=%s".formatted(QUERY_PARAMETER_NAME, QUERY_PARAMETER_VALUE);

    private static final String STATUS_PSEUDO_HEADER = ":status";

    @Mock
    private WebClientService webClientService;

    @Mock
    private HttpRequestUriSpec httpRequestUriSpec;

    @Mock
    private HttpRequestBodySpec httpRequestBodySpec;

    @Mock
    private HttpResponseEntity httpResponseEntity;

    @Mock
    private HttpEntityHeaders httpResponseHeaders;

    @Captor
    private ArgumentCaptor<URI> uriCaptor;

    private StandardHttpReplicationClient client;

    @BeforeEach
    void setClient() {
        client = new StandardHttpReplicationClient(webClientService, StandardHttpUriBuilder::new);
    }

    @Test
    void testPrepareRequest() {
        final Map<String, String> headers = Collections.emptyMap();
        final Map<String, String> requestEntity = Collections.emptyMap();
        final PreparedRequest preparedRequest = client.prepareRequest(GET_METHOD, headers, requestEntity);

        assertNotNull(preparedRequest);
        assertInstanceOf(StandardPreparedRequest.class, preparedRequest);

        assertEquals(GET_METHOD, preparedRequest.getMethod());
        assertNotEquals(headers, preparedRequest.getHeaders());
        assertEquals(requestEntity, preparedRequest.getEntity());

        final StandardPreparedRequest standardPreparedRequest = (StandardPreparedRequest) preparedRequest;
        assertArrayEquals(EMPTY_MAP_SERIALIZED, standardPreparedRequest.requestBody());
    }

    @Test
    void testReplicateIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> client.replicate(null, REPLICATE_URI));
    }

    @Test
    void testReplicate() throws IOException {
        final Map<String, String> headers = Map.of(CONTENT_TYPE_LOWERCASED, APPLICATION_JSON);
        final Map<String, String> requestEntity = Collections.emptyMap();
        final PreparedRequest preparedRequest = client.prepareRequest(GET_METHOD, headers, requestEntity);

        when(webClientService.method(any())).thenReturn(httpRequestUriSpec);
        when(httpRequestUriSpec.uri(any())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.header(anyString(), anyString())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.retrieve()).thenReturn(httpResponseEntity);

        when(httpResponseEntity.statusCode()).thenReturn(HTTP_OK);
        when(httpResponseEntity.headers()).thenReturn(httpResponseHeaders);

        final Set<String> responseHeaderNames = Set.of(
                PreparedRequestHeader.CONTENT_TYPE.getHeader(),
                PreparedRequestHeader.CONTENT_ENCODING.getHeader(),
                PreparedRequestHeader.CONTENT_LENGTH.getHeader(),
                STATUS_PSEUDO_HEADER
        );
        when(httpResponseHeaders.getHeaderNames()).thenReturn(responseHeaderNames);
        when(httpResponseHeaders.getHeader(eq(PreparedRequestHeader.CONTENT_TYPE.getHeader()))).thenReturn(List.of(APPLICATION_JSON));

        final ByteArrayInputStream responseBody = new ByteArrayInputStream(EMPTY_MAP_SERIALIZED);
        when(httpResponseEntity.body()).thenReturn(responseBody);

        final Response response = client.replicate(preparedRequest, REPLICATE_URI);

        assertResponseFound(response);

        final String responseContentType = response.getHeaderString(PreparedRequestHeader.CONTENT_TYPE.getHeader());
        assertEquals(APPLICATION_JSON, responseContentType);

        final String responseStatusHeader = response.getHeaderString(STATUS_PSEUDO_HEADER);
        assertNull(responseStatusHeader);

        final String contentEncodingHeader = response.getHeaderString(PreparedRequestHeader.CONTENT_ENCODING.getHeader());
        assertNull(contentEncodingHeader);

        final String contentLengthHeader = response.getHeaderString(PreparedRequestHeader.CONTENT_LENGTH.getHeader());
        assertNull(contentLengthHeader);
    }

    @Test
    void testReplicatePostBody() throws IOException {
        final Map<String, String> headers = Map.of(CONTENT_TYPE_LOWERCASED, APPLICATION_JSON);
        final Map<String, String> requestEntity = Collections.emptyMap();
        final PreparedRequest preparedRequest = client.prepareRequest(POST_METHOD, headers, requestEntity);

        when(webClientService.method(any())).thenReturn(httpRequestUriSpec);
        when(httpRequestUriSpec.uri(any())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.header(anyString(), anyString())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.retrieve()).thenReturn(httpResponseEntity);

        when(httpResponseEntity.statusCode()).thenReturn(HTTP_OK);
        when(httpResponseEntity.headers()).thenReturn(httpResponseHeaders);

        final ByteArrayInputStream responseBody = new ByteArrayInputStream(EMPTY_MAP_SERIALIZED);
        when(httpResponseEntity.body()).thenReturn(responseBody);

        final Response response = client.replicate(preparedRequest, REPLICATE_URI);

        assertResponseFound(response);
    }

    @Test
    void testReplicateGetMultivaluedMap() throws IOException {
        final Map<String, String> headers = Map.of(PreparedRequestHeader.CONTENT_TYPE.getHeader(), APPLICATION_JSON);

        final MultivaluedMap<String, String> requestEntity = new MultivaluedHashMap<>();
        requestEntity.add(QUERY_PARAMETER_NAME, QUERY_PARAMETER_VALUE);
        final PreparedRequest preparedRequest = client.prepareRequest(GET_METHOD, headers, requestEntity);

        when(webClientService.method(any())).thenReturn(httpRequestUriSpec);
        when(httpRequestUriSpec.uri(any())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.header(anyString(), anyString())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.retrieve()).thenReturn(httpResponseEntity);

        when(httpResponseEntity.statusCode()).thenReturn(HTTP_OK);
        when(httpResponseEntity.headers()).thenReturn(httpResponseHeaders);

        final ByteArrayInputStream responseBody = new ByteArrayInputStream(EMPTY_MAP_SERIALIZED);
        when(httpResponseEntity.body()).thenReturn(responseBody);

        final Response response = client.replicate(preparedRequest, REPLICATE_URI);

        assertResponseFound(response);

        verify(httpRequestUriSpec).uri(uriCaptor.capture());

        final URI requestUri = uriCaptor.getValue();
        assertEquals(QUERY_EXPECTED, requestUri.getQuery());
    }

    @Test
    void testReplicateGetUriQuery() throws IOException {
        final Map<String, String> headers = Map.of(PreparedRequestHeader.CONTENT_TYPE.getHeader(), APPLICATION_JSON);

        final PreparedRequest preparedRequest = client.prepareRequest(GET_METHOD, headers, Collections.emptyMap());

        when(webClientService.method(any())).thenReturn(httpRequestUriSpec);
        when(httpRequestUriSpec.uri(any())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.header(anyString(), anyString())).thenReturn(httpRequestBodySpec);
        when(httpRequestBodySpec.retrieve()).thenReturn(httpResponseEntity);

        when(httpResponseEntity.statusCode()).thenReturn(HTTP_OK);
        when(httpResponseEntity.headers()).thenReturn(httpResponseHeaders);

        final ByteArrayInputStream responseBody = new ByteArrayInputStream(EMPTY_MAP_SERIALIZED);
        when(httpResponseEntity.body()).thenReturn(responseBody);

        final Response response = client.replicate(preparedRequest, REPLICATE_URI_QUERY);

        assertResponseFound(response);

        verify(httpRequestUriSpec).uri(uriCaptor.capture());

        final URI requestUri = uriCaptor.getValue();
        assertEquals(URI_QUERY, requestUri.getQuery());
    }

    private void assertResponseFound(final Response response) {
        assertNotNull(response);
        assertEquals(HTTP_OK, response.getStatus());
    }
}
