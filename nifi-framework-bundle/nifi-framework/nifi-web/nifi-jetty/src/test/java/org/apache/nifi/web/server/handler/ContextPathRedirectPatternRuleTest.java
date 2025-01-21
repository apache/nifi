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
package org.apache.nifi.web.server.handler;

import org.apache.nifi.web.servlet.shared.ProxyHeader;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.http.HttpURI;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.ConnectionMetaData;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.Callback;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ContextPathRedirectPatternRuleTest {

    private static final String PATTERN = "/*";

    private static final String LOCATION = "/nifi";

    private static final String CONTEXT_PATH = "/context";

    private static final String STANDARD_URI = "https://localhost:8443";

    private static final String STANDARD_LOCATION = STANDARD_URI + LOCATION;

    private static final String CONTEXT_PATH_LOCATION = STANDARD_URI + CONTEXT_PATH + LOCATION;

    @Mock
    private Request request;

    @Mock
    private Response response;

    @Mock
    private Callback callback;

    @Mock
    private Server server;

    @Mock
    private HttpFields requestHeaders;

    @Mock
    private ConnectionMetaData connectionMetaData;

    @Mock
    private HttpConfiguration httpConfiguration;

    private ContextPathRedirectPatternRule rule;

    @BeforeEach
    void setRule() {
        rule = new ContextPathRedirectPatternRule(PATTERN, LOCATION, List.of(CONTEXT_PATH));
    }

    @Test
    void testHandleStandardLocation() throws Exception {
        final RewriteHandler rewriteHandler = new RewriteHandler();
        rewriteHandler.addRule(rule);
        rewriteHandler.setServer(server);
        rewriteHandler.start();

        final HttpFields.Mutable responseHeaders = setRequest();

        rewriteHandler.handle(request, response, callback);

        final String location = responseHeaders.get(HttpHeader.LOCATION);
        assertEquals(STANDARD_LOCATION, location);
    }

    @Test
    void testHandleContextPathLocation() throws Exception {
        final RewriteHandler rewriteHandler = new RewriteHandler();
        rewriteHandler.addRule(rule);
        rewriteHandler.setServer(server);
        rewriteHandler.start();

        final HttpFields.Mutable responseHeaders = setRequest();
        when(requestHeaders.get(eq(ProxyHeader.PROXY_CONTEXT_PATH.getHeader()))).thenReturn(CONTEXT_PATH);

        rewriteHandler.handle(request, response, callback);

        final String location = responseHeaders.get(HttpHeader.LOCATION);
        assertEquals(CONTEXT_PATH_LOCATION, location);
    }

    private HttpFields.Mutable setRequest() {
        final HttpURI uri = HttpURI.from(STANDARD_URI);
        when(request.getHttpURI()).thenReturn(uri);

        when(request.getHeaders()).thenReturn(requestHeaders);
        when(request.getConnectionMetaData()).thenReturn(connectionMetaData);
        when(connectionMetaData.getHttpConfiguration()).thenReturn(httpConfiguration);

        final HttpFields.Mutable responseHeaders = HttpFields.build();
        when(response.getHeaders()).thenReturn(responseHeaders);

        return responseHeaders;
    }
}
