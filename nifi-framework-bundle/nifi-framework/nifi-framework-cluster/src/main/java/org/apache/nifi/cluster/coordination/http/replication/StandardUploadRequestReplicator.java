/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.cluster.coordination.http.replication;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.WebClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Implementation of {@link UploadRequestReplicator} that uses the nifi-web-client-api.
 */
public class StandardUploadRequestReplicator implements UploadRequestReplicator {

    private static final Logger logger = LoggerFactory.getLogger(StandardUploadRequestReplicator.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ClusterCoordinator clusterCoordinator;
    private final WebClientService webClientService;
    private final File uploadWorkingDirectory;

    public StandardUploadRequestReplicator(final ClusterCoordinator clusterCoordinator, final WebClientService webClientService, final NiFiProperties properties) throws IOException {
        this.clusterCoordinator = Objects.requireNonNull(clusterCoordinator, "Cluster Coordinator is required");
        this.webClientService = Objects.requireNonNull(webClientService, "Web Client Service is required");
        this.uploadWorkingDirectory = properties.getUploadWorkingDirectory();
        FileUtils.ensureDirectoryExistAndCanAccess(this.uploadWorkingDirectory);
    }

    @Override
    public <T> T upload(final UploadRequest<T> uploadRequest) throws IOException {
        final String filename = uploadRequest.getFilename();
        final File tempFile = new File(uploadWorkingDirectory, UUID.randomUUID().toString());
        logger.debug("Created temporary file {} to hold contents of upload for {}", tempFile.getAbsolutePath(), filename);

        try {
            Files.copy(uploadRequest.getContents(), tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

            final Set<NodeIdentifier> nodeIds = clusterCoordinator.getNodeIdentifiers();
            final Map<NodeIdentifier, Future<T>> futures = new HashMap<>();
            for (final NodeIdentifier nodeId : nodeIds) {
                final Future<T> future = performUploadAsync(nodeId, uploadRequest, tempFile);
                futures.put(nodeId, future);
            }

            T responseEntity = null;
            for (final Map.Entry<NodeIdentifier, Future<T>> entry : futures.entrySet()) {
                final NodeIdentifier nodeId = entry.getKey();
                final Future<T> future = entry.getValue();
                try {
                    responseEntity = future.get();
                    logger.debug("Node {} successfully processed upload for {}", nodeId, filename);
                } catch (final ExecutionException ee) {
                    final Throwable cause = ee.getCause();
                    if (cause instanceof UploadRequestReplicationException) {
                        throw ((UploadRequestReplicationException) cause);
                    } else {
                        throw new IOException("Failed to replicate upload request to " + nodeId, ee.getCause());
                    }
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted while waiting for upload request to replicate to " + nodeId, e);
                } catch (final UploadRequestReplicationException e) {
                    throw e;
                } catch (final Exception e) {
                    throw new IOException("Failed to replicate upload request to " + nodeId, e);
                }
            }

            return responseEntity;
        } finally {
            final boolean successfulDelete = tempFile.delete();
            if (successfulDelete) {
                logger.debug("Deleted temporary file {} that was created to hold contents of upload for {}", tempFile.getAbsolutePath(), filename);
            } else {
                logger.warn("Failed to delete temporary file {}. This file should be cleaned up manually", tempFile.getAbsolutePath());
            }
        }
    }

    private <T> Future<T> performUploadAsync(final NodeIdentifier nodeId, final UploadRequest<T> uploadRequest, final File contents) {
        final CompletableFuture<T> future = new CompletableFuture<>();
        Thread.ofVirtual().name("Replicate upload to " + nodeId.getApiAddress()).start(() -> {
            try {
                final T response = replicateRequest(nodeId, uploadRequest, contents);
                logger.debug("Successfully replicated upload request for {} to {}", uploadRequest.getFilename(), nodeId.getApiAddress());
                future.complete(response);
            } catch (final IOException | UploadRequestReplicationException e) {
                future.completeExceptionally(e);
            }
        });

        return future;
    }

    private <T> T replicateRequest(final NodeIdentifier nodeId, final UploadRequest<T> uploadRequest, final File contents) throws IOException {
        final URI exampleRequestUri = uploadRequest.getExampleRequestUri();

        final URI requestUri = new StandardHttpUriBuilder()
                .scheme(exampleRequestUri.getScheme())
                .host(nodeId.getApiAddress())
                .port(nodeId.getApiPort())
                .encodedPath(exampleRequestUri.getPath())
                .build();

        final String filename = uploadRequest.getFilename();

        final Map<String, String> outboundHeaders = buildOutboundHeaders(uploadRequest);

        try (final InputStream inputStream = new FileInputStream(contents)) {
            HttpRequestBodySpec request = webClientService.post().uri(requestUri);

            for (final Map.Entry<String, String> entry : outboundHeaders.entrySet()) {
                request = request.header(entry.getKey(), entry.getValue());
            }

            logger.debug("Replicating upload request for {} to {}", filename, nodeId);

            try (final HttpResponseEntity response = request.body(inputStream, OptionalLong.of(contents.length())).retrieve()) {
                final int statusCode = response.statusCode();
                if (uploadRequest.getSuccessfulResponseStatus() != statusCode) {
                    final String responseMessage = IOUtils.toString(response.body(), StandardCharsets.UTF_8);
                    throw new UploadRequestReplicationException("Failed to replicate upload request to [%s] %s".formatted(nodeId, responseMessage), statusCode);
                }
                final InputStream responseBody = response.body();
                return objectMapper.readValue(responseBody, uploadRequest.getResponseClass());
            }
        }
    }

    /**
     * Builds the complete set of outbound headers for a replicated upload request, following the
     * same trust model as {@link ThreadPoolRequestReplicator}:
     * <ol>
     *   <li>Start with any forwarded inbound servlet headers.</li>
     *   <li>Strip all {@link RequestReplicationHeader} names (prevent spoofing).</li>
     *   <li>Strip hop-by-hop / transport-framing headers.</li>
     *   <li>Apply explicit builder headers (filename, content-type, seed) so upload metadata wins.</li>
     *   <li>Apply user proxy headers and strip credentials (Authorization, auth cookies, Host).</li>
     *   <li>Force-set {@code request-replicated} and {@code execution-continue}.</li>
     * </ol>
     */
    <T> Map<String, String> buildOutboundHeaders(final UploadRequest<T> uploadRequest) {
        final Map<String, String> headers = new HashMap<>(uploadRequest.getForwardedRequestHeaders());

        ReplicationHeaderUtils.stripRequestReplicationHeaders(headers);
        ReplicationHeaderUtils.stripHopByHopHeaders(headers);

        headers.putAll(uploadRequest.getHeaders());

        ReplicationHeaderUtils.applyUserProxyAndStripCredentials(headers, uploadRequest.getUser());

        headers.put(RequestReplicationHeader.EXECUTION_CONTINUE.getHeader(), Boolean.TRUE.toString());
        headers.put(RequestReplicationHeader.REQUEST_REPLICATED.getHeader(), Boolean.TRUE.toString());

        return headers;
    }
}
