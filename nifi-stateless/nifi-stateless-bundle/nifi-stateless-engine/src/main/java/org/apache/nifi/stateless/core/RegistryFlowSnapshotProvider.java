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
package org.apache.nifi.stateless.core;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;

public class RegistryFlowSnapshotProvider implements FlowSnapshotProvider {
    private static final Logger logger = LoggerFactory.getLogger(RegistryFlowSnapshotProvider.class);

    private static final Pattern REGISTRY_URL_PATTERN = Pattern.compile("^(https?://.+?)/?nifi-registry-api.*$");

    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final String FORWARD_SLASH = "/";

    private static final String BUCKET_FLOW_PATH_FORMAT = "buckets/%s/flows/%s";

    private static final String BUCKET_FLOW_VERSION_PATH_FORMAT = "buckets/%s/flows/%s/versions/%d";

    private static final ObjectMapper objectMapper = new ObjectMapper().disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private final String registryUrl;
    private final HttpClient httpClient;
    private final SSLContext sslContext;

    public RegistryFlowSnapshotProvider(final String registryUrl, final SSLContext sslContext) {
        this.registryUrl = registryUrl;
        this.sslContext = sslContext;

        final HttpClient.Builder builder = HttpClient.newBuilder();
        builder.connectTimeout(TIMEOUT);

        if (sslContext != null) {
            builder.sslContext(sslContext);
        }
        httpClient = builder.build();
    }

    @Override
    public VersionedFlowSnapshot getFlowSnapshot(final String bucketID, final String flowID, final int versionRequested) throws IOException {
        final int version;
        if (versionRequested == -1) {
            version = getLatestVersion(bucketID, flowID);
        } else {
            version = versionRequested;
        }

        logger.debug("Fetching flow Bucket={}, Flow={}, Version={}, FetchRemoteFlows=true", bucketID, flowID, version);
        final long start = System.nanoTime();
        final VersionedFlowSnapshot snapshot = getFlowContents(bucketID, flowID, version);
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.info("Fetched Flow [{}] Version [{}] from Bucket [{}] in {} ms", flowID, version, bucketID, millis);

        return snapshot;
    }

    private int getLatestVersion(final String bucketId, final String flowId) throws IOException {
        final String path = BUCKET_FLOW_PATH_FORMAT.formatted(bucketId, flowId);
        final URI uri = getUri(path);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri);

        try {
            final byte[] responseBody = sendRequest(requestBuilder);
            final VersionedFlow versionedFlow = objectMapper.readValue(responseBody, VersionedFlow.class);
            final long versionCount = versionedFlow.getVersionCount();
            return Math.toIntExact(versionCount);
        } catch (final Exception e) {
            throw new IOException("Failed to get Latest Version for Bucket [%s] Flow [%s]".formatted(bucketId, flowId));
        }
    }

    private VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version) throws IOException {
        final String path = BUCKET_FLOW_VERSION_PATH_FORMAT.formatted(bucketId, flowId, version);
        final URI uri = getUri(path);
        final HttpRequest.Builder requestBuilder = HttpRequest.newBuilder(uri);

        try {
            final byte[] responseBody = sendRequest(requestBuilder);
            final VersionedFlowSnapshot flowSnapshot = objectMapper.readValue(responseBody, VersionedFlowSnapshot.class);

            final VersionedProcessGroup contents = flowSnapshot.getFlowContents();
            for (final VersionedProcessGroup child : contents.getProcessGroups()) {
                populateVersionedContentsRecursively(child);
            }

            return flowSnapshot;
        } catch (final Exception e) {
            throw new IOException("Failed to get contents for Flow [%s] Version [%d] Bucket [%s]".formatted(flowId, version, bucketId), e);
        }
    }

    protected String getBaseRegistryUrl(final String storageLocation) {
        final Matcher matcher = REGISTRY_URL_PATTERN.matcher(storageLocation);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            return storageLocation;
        }
    }

    private void populateVersionedContentsRecursively(final VersionedProcessGroup group) throws IOException {
        if (group == null) {
            return;
        }

        final VersionedFlowCoordinates coordinates = group.getVersionedFlowCoordinates();
        if (coordinates != null) {
            final String subRegistryUrl = getBaseRegistryUrl(coordinates.getStorageLocation());
            final String bucketId = coordinates.getBucketId();
            final String flowId = coordinates.getFlowId();
            final int version = Integer.parseInt(coordinates.getVersion());

            final FlowSnapshotProvider nestedProvider = new RegistryFlowSnapshotProvider(subRegistryUrl, sslContext);
            final VersionedFlowSnapshot snapshot = nestedProvider.getFlowSnapshot(bucketId, flowId, version);
            final VersionedProcessGroup contents = snapshot.getFlowContents();

            group.setComments(contents.getComments());
            group.setConnections(contents.getConnections());
            group.setControllerServices(contents.getControllerServices());
            group.setFunnels(contents.getFunnels());
            group.setInputPorts(contents.getInputPorts());
            group.setLabels(contents.getLabels());
            group.setOutputPorts(contents.getOutputPorts());
            group.setProcessGroups(contents.getProcessGroups());
            group.setProcessors(contents.getProcessors());
            group.setRemoteProcessGroups(contents.getRemoteProcessGroups());
            group.setFlowFileConcurrency(contents.getFlowFileConcurrency());
            group.setFlowFileOutboundPolicy(contents.getFlowFileOutboundPolicy());
            group.setDefaultFlowFileExpiration(contents.getDefaultFlowFileExpiration());
            group.setDefaultBackPressureObjectThreshold(contents.getDefaultBackPressureObjectThreshold());
            group.setDefaultBackPressureDataSizeThreshold(contents.getDefaultBackPressureDataSizeThreshold());
            group.setLogFileSuffix(contents.getLogFileSuffix());
            coordinates.setLatest(snapshot.isLatest());
        }

        for (final VersionedProcessGroup child : group.getProcessGroups()) {
            populateVersionedContentsRecursively(child);
        }
    }

    private byte[] sendRequest(final HttpRequest.Builder requestBuilder) throws IOException {
        requestBuilder.timeout(TIMEOUT);
        final HttpRequest request = requestBuilder.build();

        final URI uri = request.uri();
        try {
            final HttpResponse<byte[]> response = httpClient.send(request, HttpResponse.BodyHandlers.ofByteArray());
            final int statusCode = response.statusCode();
            if (HttpURLConnection.HTTP_OK == statusCode) {
                return response.body();
            } else {
                throw new IOException("Registry request failed with HTTP %d [%s]".formatted(statusCode, uri));
            }
        } catch (final IOException e) {
            throw new IOException("Registry request failed [%s]".formatted(uri), e);
        } catch (final InterruptedException e) {
            throw new IOException("Registry requested interrupted [%s]".formatted(uri), e);
        }
    }

    private URI getUri(final String path) {
        final StringBuilder builder = new StringBuilder();
        builder.append(registryUrl);
        if (!registryUrl.endsWith(FORWARD_SLASH)) {
            builder.append(FORWARD_SLASH);
        }
        builder.append(path);
        return URI.create(builder.toString());
    }
}
