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

import com.sun.jersey.spi.container.ContainerRequest;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestRedirectResourceFilter {

    private static Logger logger = LoggerFactory.getLogger(TestRedirectResourceFilter.class);

    @Test
    public void testUnmatched() throws Exception {

        ContainerRequest request = mock(ContainerRequest.class);
        String path = "unmatched";
        String baseUri = "http://example.com:8080/nifi-api/";
        when(request.getPath()).thenReturn(path);
        when(request.getBaseUri()).thenReturn(new URI(baseUri));
        when(request.getRequestUri()).thenReturn(new URI(baseUri + path));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                fail("setUris shouldn't be called");
                return null;
            }
        }).when(request).setUris(any(URI.class), any(URI.class));

        RedirectResourceFilter filter = new RedirectResourceFilter();
        filter.filter(request);

    }

    @Test
    public void testController() throws Exception {

        ContainerRequest request = mock(ContainerRequest.class);
        String path = "controller";
        String baseUri = "http://example.com:8080/nifi-api/";
        when(request.getPath()).thenReturn(path);
        when(request.getBaseUri()).thenReturn(new URI(baseUri));
        when(request.getRequestUri()).thenReturn(new URI(baseUri + path));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                assertEquals("base uri should be retained", new URI(baseUri), invocation.getArguments()[0]);
                assertEquals("request uri should be redirected", new URI(baseUri + "site-to-site"), invocation.getArguments()[1]);
                return null;
            }
        }).when(request).setUris(any(URI.class), any(URI.class));

        RedirectResourceFilter filter = new RedirectResourceFilter();
        filter.filter(request);

    }

    @Test
    public void testControllerWithParams() throws Exception {

        ContainerRequest request = mock(ContainerRequest.class);
        String path = "controller";
        String baseUri = "http://example.com:8080/nifi-api/";
        String query = "?a=1&b=23&cde=456";
        when(request.getPath()).thenReturn(path);
        when(request.getBaseUri()).thenReturn(new URI(baseUri));
        when(request.getRequestUri()).thenReturn(new URI(baseUri + path + query));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                assertEquals("base uri should be retained", new URI(baseUri), invocation.getArguments()[0]);
                assertEquals("request uri should be redirected with query parameters",
                        new URI(baseUri + "site-to-site" + query), invocation.getArguments()[1]);
                return null;
            }
        }).when(request).setUris(any(URI.class), any(URI.class));

        RedirectResourceFilter filter = new RedirectResourceFilter();
        filter.filter(request);

    }
}