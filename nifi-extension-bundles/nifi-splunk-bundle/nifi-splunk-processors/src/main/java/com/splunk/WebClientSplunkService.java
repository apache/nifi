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
package com.splunk;

import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestHeadersSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.StandardHttpRequestMethod;
import org.apache.nifi.web.client.api.WebClientService;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Map;
import javax.net.ssl.HttpsURLConnection;

/**
 * Splunk Service extension using WebClientService for HTTP communication
 */
public class WebClientSplunkService extends Service {
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String SET_COOKIE_HEADER = "Set-Cookie";
    private static final String COOKIE_HEADER = "Cookie";

    private final WebClientService webClientService;

    /**
     * Standard constructor with required Service Arguments and optional WebClientService for HTTP
     *
     * @param serviceArgs Service Arguments required
     * @param webClientService Web Client Service can be null to use SDK Service.send()
     */
    public WebClientSplunkService(final Map<String, Object> serviceArgs, final WebClientService webClientService) {
        super(serviceArgs);
        this.webClientService = webClientService;
    }

    @Override
    public ResponseMessage send(final String path, final RequestMessage request) {
        if (webClientService == null) {
            return super.send(path, request);
        }

        if (token != null && !cookieStore.hasSplunkAuthCookie()) {
            request.getHeader().put(AUTHORIZATION_HEADER, token);
        }

        return sendWithWebClient(fullpath(path), request);
    }

    private ResponseMessage sendWithWebClient(final String path, final RequestMessage request) {
        final StandardHttpRequestMethod requestMethod = StandardHttpRequestMethod.valueOf(request.getMethod());

        final URL url = getUrl(path);
        final URI uri;
        try {
            uri = url.toURI();
        } catch (final URISyntaxException e) {
            throw new IllegalStateException("URI conversion failed for URL [%s]".formatted(url), e);
        }

        final HttpRequestBodySpec requestSpec = webClientService.method(requestMethod).uri(uri);
        final Object content = request.getContent();
        if (content instanceof String body) {
            requestSpec.body(body);
        }

        applyHeaders(requestSpec, request);

        final HttpResponseEntity responseEntity = requestSpec.retrieve();
        for (final String setCookie : responseEntity.headers().getHeader(SET_COOKIE_HEADER)) {
            if (setCookie != null && !setCookie.isEmpty()) {
                cookieStore.add(setCookie);
            }
        }

        final int status = responseEntity.statusCode();
        final ResponseMessage responseMessage = new ResponseMessage(status, responseEntity.body());
        if (status >= HttpsURLConnection.HTTP_BAD_REQUEST) {
            final HttpException httpException = HttpException.create(responseMessage);
            try {
                responseEntity.close();
            } catch (final IOException e) {
                httpException.addSuppressed(e);
            }

            throw httpException;
        }

        return responseMessage;
    }

    private void applyHeaders(final HttpRequestHeadersSpec requestSpec, final RequestMessage request) {
        final Map<String, String> header = request.getHeader();
        for (final Map.Entry<String, String> entry : header.entrySet()) {
            requestSpec.header(entry.getKey(), entry.getValue());
        }

        for (final Map.Entry<String, String> entry : defaultHeader.entrySet()) {
            if (!header.containsKey(entry.getKey())) {
                requestSpec.header(entry.getKey(), entry.getValue());
            }
        }

        for (final Map.Entry<String, String> entry : customHeaders.entrySet()) {
            if (!header.containsKey(entry.getKey())) {
                requestSpec.header(entry.getKey(), entry.getValue());
            }
        }

        final String cookies = cookieStore.getCookies();
        if (!cookies.isEmpty()) {
            requestSpec.header(COOKIE_HEADER, cookies);
        }
    }
}
