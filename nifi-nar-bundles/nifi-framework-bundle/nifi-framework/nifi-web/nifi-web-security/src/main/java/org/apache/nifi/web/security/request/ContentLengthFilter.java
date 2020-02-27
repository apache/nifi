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
package org.apache.nifi.web.security.request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ReadListener;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

/**
 * This {@link Filter} rejects HTTP requests that exceed a specific, maximum size.
 */
public class ContentLengthFilter implements Filter {
    private static final Logger logger = LoggerFactory.getLogger(ContentLengthFilter.class);
    public final static String MAX_LENGTH_INIT_PARAM = "maxContentLength";
    public final static int MAX_LENGTH_DEFAULT = 10_000_000;
    private int maxContentLength;

    public ContentLengthFilter() {
        maxContentLength = MAX_LENGTH_DEFAULT;
    }

    public ContentLengthFilter(int maxLength) {
        maxContentLength = maxLength;
    }

    @Override
    public void init(FilterConfig config) throws ServletException {
        String maxLength = config.getInitParameter(MAX_LENGTH_INIT_PARAM);
        int length = maxLength == null ? MAX_LENGTH_DEFAULT : Integer.parseInt(maxLength);
        if (length < 0) {
            length = MAX_LENGTH_DEFAULT;
            // throw new ServletException("Invalid max request length.");
        }
        maxContentLength = length;
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain next) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String httpMethod = httpRequest.getMethod();

        // Check the HTTP method because the spec says clients don't have to send a content-length header for methods
        // that don't use it.  So even though an attacker may provide a large body in a GET request, the body should go
        // unread and a size filter is unneeded at best.  See RFC 2616 section 14.13, and RFC 1945 section 10.4.
        boolean willReadInputStream = httpMethod.equalsIgnoreCase("POST") || httpMethod.equalsIgnoreCase("PUT");
        if (!willReadInputStream || maxContentLength <= 0) {
            logger.info("skipping check of request with method {} and maximum {}", httpMethod, maxContentLength);
            next.doFilter(request, response);
            return;
        }

        HttpServletResponse httpResponse = (HttpServletResponse) response;
        int contentLength = request.getContentLength();
        if (contentLength < 0) {
            // Request without a content length is rejected:
            logger.info("request rejected with negative or unknown content-length {}", contentLength);
            httpResponse.setContentType("text/plain");
            httpResponse.setStatus(HttpServletResponse.SC_LENGTH_REQUIRED);
        } else if (contentLength > maxContentLength) {
            // Request with a client-specified length greater than our max is rejected:
            logger.info("request rejected with content-length {} greater than maximum {}", contentLength, maxContentLength);
            httpResponse.setContentType("text/plain");
            httpResponse.getOutputStream().write("Payload Too large".getBytes());
            httpResponse.setStatus(HttpServletResponse.SC_REQUEST_ENTITY_TOO_LARGE);
        } else {
            // If or when the request is read, this limits the read to our max:
            logger.info("request allowed with content-length {} less than maximum {}", contentLength, maxContentLength);
            next.doFilter(new LimitedContentLengthRequest(httpRequest, maxContentLength), response);
        }
    }

    @Override
    public void destroy() {
    }

    // This wrapper ensures that the input stream of the wrapped request is not read past the given maximum.
    private static class LimitedContentLengthRequest extends HttpServletRequestWrapper {
        private int maxRequestLength;

        public LimitedContentLengthRequest(HttpServletRequest request, int maxLength) {
            super(request);
            maxRequestLength = maxLength;
        }

        @Override
        public ServletInputStream getInputStream() throws IOException {
            final ServletInputStream originalStream = super.getInputStream();
            return new ServletInputStream() {
                private int inputStreamByteCounter = 0;

                @Override
                public boolean isFinished() {
                    return originalStream.isFinished();
                }

                @Override
                public boolean isReady() {
                    return originalStream.isReady();
                }

                @Override
                public void setReadListener(ReadListener readListener) {
                    originalStream.setReadListener(readListener);
                }

                @Override
                public int read() throws IOException {
                    int read = originalStream.read();
                    if (read == -1) {
                        return read;
                    }

                    inputStreamByteCounter += 1;
                    if (inputStreamByteCounter > maxRequestLength) {
                        throw new IOException(String.format("Request input stream longer than %d bytes.", maxRequestLength));
                    }
                    return read;
                }
            };
        }
    }
}
