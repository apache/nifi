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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jakarta.xmlbind.JakartaXmlBindAnnotationIntrospector;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;

import org.apache.nifi.cluster.coordination.http.replication.HttpReplicationClient;
import org.apache.nifi.cluster.coordination.http.replication.PreparedRequest;
import org.apache.nifi.cluster.coordination.http.replication.io.EntitySerializer;
import org.apache.nifi.cluster.coordination.http.replication.io.ReplicatedResponse;
import org.apache.nifi.cluster.coordination.http.replication.io.JsonEntitySerializer;
import org.apache.nifi.cluster.coordination.http.replication.io.XmlEntitySerializer;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestMethod;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.Supplier;
import java.util.zip.GZIPInputStream;

/**
 * Standard HTTP Replication Client based on Web Client Service
 */
public class StandardHttpReplicationClient implements HttpReplicationClient {
    private static final Set<String> REQUEST_BODY_METHODS = Set.of("PATCH", "POST", "PUT");

    private static final Set<String> DISALLOWED_HEADERS = Set.of("connection", "content-length", "expect", "host", "upgrade");

    private static final int CONTENT_LENGTH_NOT_FOUND = -1;

    private static final char PSEUDO_HEADER_PREFIX = ':';

    private static final String GZIP_ENCODING = "gzip";

    private static final String QUERY_SEPARATOR = "&";

    private static final String QUERY_NAME_VALUE_SEPARATOR = "=";

    private static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";

    private static final String APPLICATION_XML_CONTENT_TYPE = "application/xml";

    private static final String USER_AGENT_PRODUCT = "Apache NiFi";

    private static final String USER_AGENT_FORMAT = "%s/%s";

    private static final String USER_AGENT_VERSION = "SNAPSHOT";

    private static final String USER_AGENT;

    private static final Logger logger = LoggerFactory.getLogger(StandardHttpReplicationClient.class);

    static {
        final Package clientPackage = StandardHttpReplicationClient.class.getPackage();
        final String userAgentVersion;
        if (clientPackage == null || clientPackage.getImplementationVersion() == null) {
            userAgentVersion = USER_AGENT_VERSION;
        } else {
            // Set User Agent Version from JAR MANIFEST.MF Version when found
            userAgentVersion = clientPackage.getImplementationVersion();
        }
        USER_AGENT = USER_AGENT_FORMAT.formatted(USER_AGENT_PRODUCT, userAgentVersion);
    }

    private final WebClientService webClientService;

    private final Supplier<HttpUriBuilder> httpUriBuilderSupplier;

    private final EntitySerializer jsonSerializer;

    private final EntitySerializer xmlSerializer;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public StandardHttpReplicationClient(final WebClientService webClientService, final Supplier<HttpUriBuilder> httpUriBuilderSupplier) {
        this.webClientService = Objects.requireNonNull(webClientService, "Web Client Service required");
        this.httpUriBuilderSupplier = Objects.requireNonNull(httpUriBuilderSupplier, "HTTP URI Builder supplier required");

        objectMapper.setDefaultPropertyInclusion(JsonInclude.Value.construct(JsonInclude.Include.NON_NULL, JsonInclude.Include.ALWAYS));
        objectMapper.setAnnotationIntrospector(new JakartaXmlBindAnnotationIntrospector(objectMapper.getTypeFactory()));

        jsonSerializer = new JsonEntitySerializer(objectMapper);
        xmlSerializer = new XmlEntitySerializer();
    }

    /**
     * Prepare Request for Replication with serialized Request Entity
     *
     * @param method HTTP Method
     * @param headers HTTP Request Headers
     * @param requestEntity Request Entity to be serialized
     * @return Prepared Request for replication
     */
    @Override
    public PreparedRequest prepareRequest(final String method, final Map<String, String> headers, final Object requestEntity) {
        final Map<String, String> preparedHeaders = getPreparedHeaders(headers, method);
        final byte[] requestBody = getRequestBody(requestEntity, preparedHeaders);
        return new StandardPreparedRequest(method, preparedHeaders, requestEntity, requestBody);
    }

    /**
     * Replicate Prepared HTTP Request to destination URI
     *
     * @param request Prepared HTTP Request for replication
     * @param uri Destination URI for sending the request
     * @return Jakarta REST Response
     * @throws IOException Thrown on communication failures sending requests or retrieving responses
     */
    @Override
    public Response replicate(final PreparedRequest request, final URI uri) throws IOException {
        if (request instanceof StandardPreparedRequest preparedRequest) {
            return replicate(preparedRequest, uri);
        } else {
            throw new IllegalArgumentException("HTTP Prepared Request not provided");
        }
    }

    private Map<String, String> getPreparedHeaders(final Map<String, String> headers, final String method) {
        final Map<String, String> preparedHeaders = new LinkedHashMap<>();

        for (final Map.Entry<String, String> header : headers.entrySet()) {
            final String headerName = header.getKey().toLowerCase();
            if (PreparedRequestHeader.ACCEPT_ENCODING.getHeader().equals(headerName)) {
                // Remove Accept-Encoding from original client request in favor of specific value for replication
                continue;
            }

            final String headerValue = header.getValue();
            preparedHeaders.put(headerName, headerValue);
        }

        // Set Accept-Encoding to request gzip encoded responses
        preparedHeaders.put(PreparedRequestHeader.ACCEPT_ENCODING.getHeader(), GZIP_ENCODING);

        processContentType(method, preparedHeaders);
        processUserAgent(preparedHeaders);
        return preparedHeaders;
    }

    private Response replicate(final StandardPreparedRequest preparedRequest, final URI location) throws IOException {
        final HttpRequestMethod requestMethod = getRequestMethod(preparedRequest);
        final URI requestUri = getRequestUri(preparedRequest, location);

        final HttpRequestBodySpec httpRequestBodySpec = webClientService.method(requestMethod).uri(requestUri);

        final Map<String, String> requestHeaders = preparedRequest.headers();
        for (final Map.Entry<String, String> requestHeader : requestHeaders.entrySet()) {
            final String headerName = requestHeader.getKey();
            final String headerNameLowerCased = headerName.toLowerCase();
            if (!DISALLOWED_HEADERS.contains(headerNameLowerCased)) {
                httpRequestBodySpec.header(headerName, requestHeader.getValue());
            }
        }

        if (REQUEST_BODY_METHODS.contains(requestMethod.getMethod())) {
            final byte[] requestBody = preparedRequest.requestBody();
            final ByteArrayInputStream body = new ByteArrayInputStream(requestBody);
            final OptionalLong contentLength = OptionalLong.of(requestBody.length);
            httpRequestBodySpec.body(body, contentLength);
        }

        return replicate(httpRequestBodySpec, preparedRequest.method(), location);
    }

    private Response replicate(final HttpRequestBodySpec httpRequestBodySpec, final String method, final URI location) throws IOException {
        final long started = System.currentTimeMillis();

        final HttpResponseEntity responseEntity = httpRequestBodySpec.retrieve();
        final int statusCode = responseEntity.statusCode();
        final HttpEntityHeaders headers = responseEntity.headers();
        final MultivaluedMap<String, String> responseHeaders = getResponseHeaders(headers);
        final int contentLength = getContentLength(headers);

        final InputStream responseBody = getResponseBody(responseEntity.body(), headers);
        final Runnable closeCallback = () -> {
            try {
                responseEntity.close();
            } catch (final IOException e) {
                logger.warn("Close failed for Replicated {} {} HTTP {}", method, location, statusCode, e);
            }
        };

        final long elapsed = System.currentTimeMillis() - started;
        logger.debug("Replicated {} {} HTTP {} in {} ms", method, location, statusCode, elapsed);

        return new ReplicatedResponse(objectMapper, responseBody, responseHeaders, location, statusCode, contentLength, closeCallback);
    }

    private URI getRequestUri(final StandardPreparedRequest preparedRequest, final URI location) {
        final HttpUriBuilder httpUriBuilder = httpUriBuilderSupplier.get();

        httpUriBuilder.scheme(location.getScheme());
        httpUriBuilder.host(location.getHost());
        httpUriBuilder.port(location.getPort());
        httpUriBuilder.encodedPath(location.getPath());

        final String query = location.getQuery();
        if (query != null) {
            final String[] parameters = query.split(QUERY_SEPARATOR);
            for (final String parameter : parameters) {
                final String[] parameterNameValue = parameter.split(QUERY_NAME_VALUE_SEPARATOR);
                if (parameterNameValue.length == 1) {
                    final String parameterName = parameterNameValue[0];
                    httpUriBuilder.addQueryParameter(parameterName, null);
                } else if (parameterNameValue.length == 2) {
                    final String parameterName = parameterNameValue[0];
                    final String parameterValue = parameterNameValue[1];
                    httpUriBuilder.addQueryParameter(parameterName, parameterValue);
                }
            }
        }

        final Object requestEntity = preparedRequest.entity();
        if (requestEntity instanceof MultivaluedMap<?, ?> parameterEntity) {
            for (final Object key : parameterEntity.keySet()) {
                final String parameterName = key.toString();
                final Object parameterValues = parameterEntity.get(parameterName);
                if (parameterValues instanceof List<?> values) {
                    for (final Object value : values) {
                        httpUriBuilder.addQueryParameter(parameterName, value.toString());
                    }
                }
            }
        }

        return httpUriBuilder.build();
    }

    private HttpRequestMethod getRequestMethod(final PreparedRequest preparedRequest) {
        final String method = preparedRequest.getMethod();
        return new HttpRequestMethod() {
            @Override
            public String getMethod() {
                return method;
            }

            @Override
            public String toString() {
                return method;
            }
        };
    }

    private MultivaluedMap<String, String> getResponseHeaders(final HttpEntityHeaders responseHeaders) {
        final MultivaluedMap<String, String> headers = new MultivaluedHashMap<>();
        for (final String name : responseHeaders.getHeaderNames()) {
            // Remove pseudo-headers returned from HTTP/2 responses
            if (name.charAt(0) == PSEUDO_HEADER_PREFIX) {
                continue;
            }
            // Remove Content-Encoding Response Header to align with gzip decoding of Response Body
            if (PreparedRequestHeader.CONTENT_ENCODING.getHeader().equalsIgnoreCase(name)) {
                continue;
            }
            // Remove Content-Length Response Header to align with gzip decoding of Response Body
            if (PreparedRequestHeader.CONTENT_LENGTH.getHeader().equalsIgnoreCase(name)) {
                continue;
            }
            final List<String> values = responseHeaders.getHeader(name);
            headers.addAll(name, values);
        }
        return headers;
    }

    private InputStream getResponseBody(final InputStream inputStream, final HttpEntityHeaders responseHeaders) throws IOException {
        final boolean gzipEncoded = isGzipEncoded(responseHeaders);
        return gzipEncoded ? new GZIPInputStream(inputStream) : inputStream;
    }

    private int getContentLength(final HttpEntityHeaders headers) {
        final Optional<String> contentLengthFound = headers.getHeaderNames()
                .stream()
                .filter(PreparedRequestHeader.CONTENT_LENGTH.getHeader()::equalsIgnoreCase)
                .findFirst()
                .flatMap(headers::getFirstHeader);

        int contentLength;
        if (contentLengthFound.isPresent()) {
            final String contentLengthHeader = contentLengthFound.get();
            try {
                contentLength = Integer.parseInt(contentLengthHeader);
            } catch (final NumberFormatException e) {
                logger.warn("Replicated Header Content-Length [{}] parsing failed", contentLengthHeader, e);
                contentLength = CONTENT_LENGTH_NOT_FOUND;
            }
        } else {
            contentLength = CONTENT_LENGTH_NOT_FOUND;
        }

        return contentLength;
    }

    private byte[] getRequestBody(final Object requestEntity, final Map<String, String> headers) {
        final Optional<String> contentTypeFound = getContentType(headers);
        final String contentType = contentTypeFound.orElse(APPLICATION_JSON_CONTENT_TYPE);
        final EntitySerializer serializer = getSerializer(contentType);

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            serializer.serialize(requestEntity, outputStream);
        } catch (final IOException e) {
            throw new UncheckedIOException("Request Entity serialization failed", e);
        }

        return outputStream.toByteArray();
    }

    private void processContentType(final String method, final Map<String, String> headers) {
        if (REQUEST_BODY_METHODS.contains(method)) {
            final Optional<String> contentTypeHeaderFound = getHeaderName(headers, PreparedRequestHeader.CONTENT_TYPE);
            if (contentTypeHeaderFound.isEmpty()) {
                // Set default Content-Type to JSON
                headers.put(PreparedRequestHeader.CONTENT_TYPE.getHeader(), APPLICATION_JSON_CONTENT_TYPE);
            }
        }
    }

    private void processUserAgent(final Map<String, String> headers) {
        final Optional<String> userAgentHeaderFound = getHeaderName(headers, PreparedRequestHeader.USER_AGENT);
        final String userAgentHeader = userAgentHeaderFound.orElseGet(PreparedRequestHeader.USER_AGENT::getHeader);
        headers.put(userAgentHeader, USER_AGENT);
    }

    private EntitySerializer getSerializer(final String contentType) {
        final EntitySerializer serializer;

        if (APPLICATION_XML_CONTENT_TYPE.equalsIgnoreCase(contentType)) {
            serializer = xmlSerializer;
        } else {
            serializer = jsonSerializer;
        }

        return serializer;
    }

    private boolean isGzipEncoded(final HttpEntityHeaders headers) {
        final Optional<String> contentEncodingFound = headers.getHeaderNames()
                .stream()
                .filter(PreparedRequestHeader.CONTENT_ENCODING.getHeader()::equalsIgnoreCase)
                .map(headers::getFirstHeader)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst();

        return contentEncodingFound.map(GZIP_ENCODING::equalsIgnoreCase).orElse(false);
    }

    private Optional<String> getContentType(final Map<String, String> headers) {
        final Optional<String> headerNameFound = getHeaderName(headers, PreparedRequestHeader.CONTENT_TYPE);

        final String header;
        if (headerNameFound.isPresent()) {
            final String name = headerNameFound.get();
            header = headers.get(name);
        } else {
            header = null;
        }

        return Optional.ofNullable(header);
    }

    private Optional<String> getHeaderName(final Map<String, String> headers, final PreparedRequestHeader httpHeader) {
        return headers.keySet()
                .stream()
                .filter(httpHeader.getHeader()::equalsIgnoreCase)
                .findFirst();
    }
}
