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
package org.apache.nifi.cluster.manager;

import com.sun.jersey.api.client.ClientResponse;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.api.entity.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Encapsulates a node's response in regards to receiving a external API request.
 *
 * Both the ClientResponse and (server) Response may be obtained from this instance. The ClientResponse is stored as it is received from the node. This includes the entity input stream. The Response
 * is constructed on demand when mapping a ClientResponse to the Response. The ClientResponse to Response mapping includes copying the ClientResponse's input stream to the Response. Therefore, the
 * getResponse() method should not be called more than once. Furthermore, the method should not be called if the caller has already read the ClientResponse's input stream.
 *
 * If a ClientResponse was unable to be created, then a NodeResponse will store the Throwable, which may be obtained by calling getThrowable().
 *
 * This class overrides hashCode and equals and considers two instances to be equal if they have the equal NodeIdentifiers.
 *
 */
public class NodeResponse {

    private static final Logger logger = LoggerFactory.getLogger(NodeResponse.class);
    private final String httpMethod;
    private final URI requestUri;
    private final ClientResponse clientResponse;
    private final NodeIdentifier nodeId;
    private Throwable throwable;
    private boolean hasCreatedResponse = false;
    private final Entity updatedEntity;
    private final long requestDurationNanos;
    private final String requestId;
    private byte[] bufferedResponse;

    public NodeResponse(final NodeIdentifier nodeId, final String httpMethod, final URI requestUri, final ClientResponse clientResponse, final long requestDurationNanos, final String requestId) {
        if (nodeId == null) {
            throw new IllegalArgumentException("Node identifier may not be null.");
        } else if (StringUtils.isBlank(httpMethod)) {
            throw new IllegalArgumentException("Http method may not be null or empty.");
        } else if (requestUri == null) {
            throw new IllegalArgumentException("Request URI may not be null.");
        } else if (clientResponse == null) {
            throw new IllegalArgumentException("ClientResponse may not be null.");
        }
        this.nodeId = nodeId;
        this.httpMethod = httpMethod;
        this.requestUri = requestUri;
        this.clientResponse = clientResponse;
        this.throwable = null;
        this.updatedEntity = null;
        this.requestDurationNanos = requestDurationNanos;
        this.requestId = requestId;
    }

    public NodeResponse(final NodeIdentifier nodeId, final String httpMethod, final URI requestUri, final Throwable throwable) {
        if (nodeId == null) {
            throw new IllegalArgumentException("Node identifier may not be null.");
        } else if (StringUtils.isBlank(httpMethod)) {
            throw new IllegalArgumentException("Http method may not be null or empty.");
        } else if (requestUri == null) {
            throw new IllegalArgumentException("Request URI may not be null.");
        } else if (throwable == null) {
            throw new IllegalArgumentException("Throwable may not be null.");
        }
        this.nodeId = nodeId;
        this.httpMethod = httpMethod;
        this.requestUri = requestUri;
        this.clientResponse = null;
        this.throwable = throwable;
        this.updatedEntity = null;
        this.requestDurationNanos = -1L;
        this.requestId = null;
    }

    public NodeResponse(final NodeResponse example, final Entity updatedEntity) {
        Objects.requireNonNull(example, "NodeResponse cannot be null");
        Objects.requireNonNull(updatedEntity, "UpdatedEntity cannot be null");

        this.nodeId = example.nodeId;
        this.httpMethod = example.httpMethod;
        this.requestUri = example.requestUri;
        this.clientResponse = example.clientResponse;
        this.throwable = example.throwable;
        this.updatedEntity = updatedEntity;
        this.requestDurationNanos = example.requestDurationNanos;
        this.requestId = null;
    }

    public NodeIdentifier getNodeId() {
        return nodeId;
    }

    public String getHttpMethod() {
        return httpMethod;
    }

    public URI getRequestUri() {
        return requestUri;
    }

    public int getStatus() {
        if (hasThrowable()) {
            /*
             * since there is a throwable, there is no client input stream to
             * worry about maintaining, so we can call getResponse() method
             */
            return getResponse().getStatus();
        } else {
            /*
             * use client response's status instead of calling getResponse().getStatus()
             * so that we don't read the client's input stream as part of creating
             * the response in the getResponse() method
             */
            return clientResponse.getStatus();
        }
    }

    public boolean is2xx() {
        final int statusCode = getStatus();
        return (200 <= statusCode && statusCode <= 299);
    }

    public boolean is5xx() {
        final int statusCode = getStatus();
        return (500 <= statusCode && statusCode <= 599);
    }

    public synchronized void bufferResponse() {
        bufferedResponse = new byte[clientResponse.getLength()];
        try {
            StreamUtils.fillBuffer(clientResponse.getEntityInputStream(), bufferedResponse);
        } catch (final IOException e) {
            this.throwable = e;
        }
    }

    public synchronized InputStream getInputStream() {
        if (bufferedResponse == null) {
            return clientResponse.getEntityInputStream();
        }

        return new ByteArrayInputStream(bufferedResponse);
    }

    public ClientResponse getClientResponse() {
        return clientResponse;
    }


    public Entity getUpdatedEntity() {
        return updatedEntity;
    }

    public Response getResponse() {
        // if the response encapsulates a throwable, then the input stream is never read and the below warning is irrelevant
        if (hasCreatedResponse && !hasThrowable()) {
            logger.warn("ClientResponse's input stream has already been read.  The created response will not contain this data.");
        }
        hasCreatedResponse = true;
        return createResponse();
    }


    public Throwable getThrowable() {
        return throwable;
    }

    public boolean hasThrowable() {
        return getThrowable() != null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final NodeResponse other = (NodeResponse) obj;
        if (this.nodeId != other.nodeId && (this.nodeId == null || !this.nodeId.equals(other.nodeId))) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 13 * hash + (this.nodeId != null ? this.nodeId.hashCode() : 0);
        return hash;
    }

    public long getRequestDuration(final TimeUnit timeUnit) {
        return timeUnit.convert(requestDurationNanos, TimeUnit.NANOSECONDS);
    }

    public String getRequestId() {
        return requestId;
    }

    private Response createResponse() {

        // if no client response was created, then generate a 500 response
        if (hasThrowable()) {
            return Response.status(Status.INTERNAL_SERVER_ERROR).entity(getThrowable().toString()).build();
        }

        // set the status
        final ResponseBuilder responseBuilder = Response.status(clientResponse.getStatus());

        // set the headers
        for (final String key : clientResponse.getHeaders().keySet()) {
            final List<String> values = clientResponse.getHeaders().get(key);
            for (final String value : values) {
                if (key.equalsIgnoreCase("transfer-encoding") || key.equalsIgnoreCase("content-length")) {
                    /*
                     * do not copy the transfer-encoding header (i.e., chunked encoding) or
                     * the content-length. Let the outgoing response builder determine it.
                     */
                    continue;
                } else if (key.equals("X-ClusterContext")) {
                    /*
                     * do not copy the cluster context to the response because
                     * this information is private and should not be sent to
                     * the client
                     */
                    continue;
                }

                responseBuilder.header(key, value);
            }
        }

        // head requests must not have a message-body in the response
        if (!HttpMethod.HEAD.equalsIgnoreCase(httpMethod)) {
            // set the entity
            if (updatedEntity == null) {
                responseBuilder.entity(new StreamingOutput() {
                    @Override
                    public void write(final OutputStream output) throws IOException, WebApplicationException {
                        IOUtils.copy(getInputStream(), output);
                    }
                });
            } else {
                responseBuilder.entity(updatedEntity);
            }
        }

        return responseBuilder.build();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("NodeResponse[nodeUri=").append(nodeId.getApiAddress()).append(":").append(nodeId.getApiPort()).append(",")
            .append("method=").append(httpMethod)
            .append(",URI=").append(requestUri)
            .append(",ResponseCode=").append(getStatus())
            .append(",Duration=").append(TimeUnit.MILLISECONDS.convert(requestDurationNanos, TimeUnit.NANOSECONDS)).append(" ms]");
        return sb.toString();
    }
}
