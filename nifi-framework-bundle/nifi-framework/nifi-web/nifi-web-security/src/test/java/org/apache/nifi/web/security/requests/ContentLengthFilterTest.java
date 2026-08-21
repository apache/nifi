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
package org.apache.nifi.web.security.requests;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ReadListener;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ContentLengthFilterTest {

    private static final int MAX_LENGTH = 1000;

    private static final int WITHIN_LIMIT_LENGTH = 500;

    private static final int OVERSIZED_LENGTH = 2000;

    private static final String REQUEST_URI = "/nifi-api/process-groups/root";

    private static final int UNKNOWN_CONTENT_LENGTH_VALUE = -1;

    @Test
    void testDecompressedBodyExceedingLimitThrowsRequestContentLengthExceededException() throws Exception {
        final ContentLengthFilter filter = createFilter();

        // Create post request with unknown Content-Length header and compressed content exceeding configured limits
        final HttpServletRequest request = createPostRequest(UNKNOWN_CONTENT_LENGTH_VALUE, OVERSIZED_LENGTH);
        final HttpServletResponse response = mock(HttpServletResponse.class);

        final FilterChain readingChain = (req, res) -> {
            final InputStream inputStream = req.getInputStream();
            inputStream.readAllBytes();
        };

        assertThrows(RequestContentLengthExceededException.class, () -> filter.doFilter(request, response, readingChain));
    }

    @Test
    void testDecompressedBodyWithinLimitConsumedSuccessfully() throws Exception {
        final ContentLengthFilter filter = createFilter();

        final HttpServletRequest request = createPostRequest(UNKNOWN_CONTENT_LENGTH_VALUE, WITHIN_LIMIT_LENGTH);
        final HttpServletResponse response = mock(HttpServletResponse.class);

        final FilterChain readingChain = (req, res) -> {
            final InputStream inputStream = req.getInputStream();
            inputStream.readAllBytes();
        };

        filter.doFilter(request, response, readingChain);
    }

    @Test
    void testDeclaredContentLengthExceedingLimitRejectedWithPayloadTooLarge() throws Exception {
        final ContentLengthFilter filter = createFilter();

        final HttpServletRequest request = createPostRequest(OVERSIZED_LENGTH, OVERSIZED_LENGTH);
        final HttpServletResponse response = mock(HttpServletResponse.class);
        when(response.getOutputStream()).thenReturn(mock(ServletOutputStream.class));

        final FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(response).setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
    }

    private ContentLengthFilter createFilter() throws Exception {
        final ContentLengthFilter filter = new ContentLengthFilter();
        final FilterConfig filterConfig = mock(FilterConfig.class);
        when(filterConfig.getInitParameter(ContentLengthFilter.MAX_LENGTH_INIT_PARAM)).thenReturn(Integer.toString(MAX_LENGTH));
        filter.init(filterConfig);
        return filter;
    }

    private HttpServletRequest createPostRequest(final int declaredContentLength, final int bodyLength) throws IOException {
        final HttpServletRequest request = mock(HttpServletRequest.class);
        when(request.getMethod()).thenReturn("POST");
        when(request.getRequestURI()).thenReturn(REQUEST_URI);
        when(request.getContentLength()).thenReturn(declaredContentLength);
        when(request.getInputStream()).thenReturn(new ByteArrayServletInputStream(new byte[bodyLength]));
        return request;
    }

    private static class ByteArrayServletInputStream extends ServletInputStream {
        private final ByteArrayInputStream inputStream;

        private ByteArrayServletInputStream(final byte[] bytes) {
            this.inputStream = new ByteArrayInputStream(bytes);
        }

        @Override
        public boolean isFinished() {
            return inputStream.available() == 0;
        }

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setReadListener(final ReadListener readListener) {
        }

        @Override
        public int read() {
            return inputStream.read();
        }
    }
}
