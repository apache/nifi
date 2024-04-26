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
package org.apache.nifi.web.api.filter;

import org.junit.jupiter.api.Test;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRedirectResourceFilter {

    @Test
    public void testUnmatched() throws Exception {
        String path = "unmatched";
        String baseUri = "http://example.com:8080/nifi-api/";

        UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn(path);
        when(uriInfo.getBaseUri()).thenReturn(new URI(baseUri));
        when(uriInfo.getRequestUri()).thenReturn(new URI(baseUri + path));

        ContainerRequestContext request = mock(ContainerRequestContext.class);
        when(request.getUriInfo()).thenReturn(uriInfo);

        doAnswer(invocation -> {
            fail("setUris shouldn't be called");
            return null;
        }).when(request).setRequestUri(any(URI.class), any(URI.class));

        RedirectResourceFilter filter = new RedirectResourceFilter();
        filter.filter(request);
    }

    @Test
    public void testController() throws Exception {
        String path = "controller";
        String baseUri = "http://example.com:8080/nifi-api/";

        UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn(path);
        when(uriInfo.getBaseUri()).thenReturn(new URI(baseUri));
        when(uriInfo.getRequestUri()).thenReturn(new URI(baseUri + path));

        ContainerRequestContext request = mock(ContainerRequestContext.class);
        when(request.getUriInfo()).thenReturn(uriInfo);

        doAnswer(invocation -> {
            assertEquals(new URI(baseUri), invocation.getArguments()[0], "base uri should be retained");
            assertEquals(new URI(baseUri + "site-to-site"), invocation.getArguments()[1], "request uri should be redirected");
            return null;
        }).when(request).setRequestUri(any(URI.class), any(URI.class));

        RedirectResourceFilter filter = new RedirectResourceFilter();
        filter.filter(request);

    }

    @Test
    public void testControllerWithParams() throws Exception {
        String path = "controller";
        String baseUri = "http://example.com:8080/nifi-api/";
        String query = "?a=1&b=23&cde=456";

        UriInfo uriInfo = mock(UriInfo.class);
        when(uriInfo.getPath()).thenReturn(path);
        when(uriInfo.getBaseUri()).thenReturn(new URI(baseUri));
        when(uriInfo.getRequestUri()).thenReturn(new URI(baseUri + path + query));

        ContainerRequestContext request = mock(ContainerRequestContext.class);
        when(request.getUriInfo()).thenReturn(uriInfo);

        doAnswer(invocation -> {
            assertEquals(new URI(baseUri), invocation.getArguments()[0], "base uri should be retained");
            assertEquals(new URI(baseUri + "site-to-site" + query), invocation.getArguments()[1], "request uri should be redirected with query parameters");
            return null;
        }).when(request).setRequestUri(any(URI.class), any(URI.class));

        RedirectResourceFilter filter = new RedirectResourceFilter();
        filter.filter(request);

    }
}