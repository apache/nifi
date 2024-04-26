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

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.client.FlowClient;
import org.apache.nifi.registry.client.FlowSnapshotClient;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.client.impl.request.ProxiedEntityRequestConfig;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RegistryUtil {
    private static final Logger logger = LoggerFactory.getLogger(RegistryUtil.class);

    private static final Pattern REGISTRY_URL_PATTERN = Pattern.compile("^(https?://.+?)/?nifi-registry-api.*$");

    private final String registryUrl;
    private NiFiRegistryClient registryClient;
    private final SSLContext sslContext;

    public RegistryUtil(final String registryUrl, final SSLContext sslContext) {
        this.registryUrl = registryUrl;
        this.sslContext = sslContext;
    }

    public RegistryUtil(final NiFiRegistryClient registryClient, final String registryUrl, final SSLContext sslContext) {
        this.registryClient = registryClient;
        this.registryUrl = registryUrl;
        this.sslContext = sslContext;
    }

    public VersionedFlowSnapshot getFlowByID(String bucketID, String flowID, int versionID) throws IOException, NiFiRegistryException {
        if (versionID == -1) {
            // TODO: Have to support providing some sort of user
            versionID = getLatestVersion(bucketID, flowID, null);
        }

        logger.debug("Fetching flow Bucket={}, Flow={}, Version={}, FetchRemoteFlows=true", bucketID, flowID, versionID);
        final long start = System.nanoTime();
        final VersionedFlowSnapshot snapshot = getFlowContents(bucketID, flowID, versionID, true, null);
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.info("Successfully fetched flow from registry in {} millis", millis);

        return snapshot;
    }

    private int getLatestVersion(final String bucketId, final String flowId, final NiFiUser user) throws IOException, NiFiRegistryException {
        return (int) getFlowClient(user).get(bucketId, flowId).getVersionCount();
    }

    private FlowClient getFlowClient(final NiFiUser user) {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final FlowClient flowClient = identity == null ? registryClient.getFlowClient() : registryClient.getFlowClient(new ProxiedEntityRequestConfig(identity));
        return flowClient;
    }

    private FlowSnapshotClient getFlowSnapshotClient(final NiFiUser user) {
        final String identity = getIdentity(user);
        final NiFiRegistryClient registryClient = getRegistryClient();
        final FlowSnapshotClient snapshotClient = identity == null ? registryClient.getFlowSnapshotClient() : registryClient.getFlowSnapshotClient(new ProxiedEntityRequestConfig(identity));
        return snapshotClient;
    }

    private synchronized NiFiRegistryClient getRegistryClient() {
        if (registryClient != null) {
            return registryClient;
        }

        final NiFiRegistryClientConfig config = new NiFiRegistryClientConfig.Builder()
            .connectTimeout(30000)
            .readTimeout(30000)
            .sslContext(sslContext)
            .baseUrl(registryUrl)
            .build();

        registryClient = new JerseyNiFiRegistryClient.Builder()
            .config(config)
            .build();

        return registryClient;
    }

    private String getIdentity(final NiFiUser user) {
        return (user == null || user.isAnonymous()) ? null : user.getIdentity();
    }

    public VersionedFlowSnapshot getFlowContents(final String bucketId, final String flowId, final int version, final boolean fetchRemoteFlows, final NiFiUser user)
        throws IOException, NiFiRegistryException {

        final FlowSnapshotClient snapshotClient = getFlowSnapshotClient(user);
        final VersionedFlowSnapshot flowSnapshot = snapshotClient.get(bucketId, flowId, version);

        if (fetchRemoteFlows) {
            final VersionedProcessGroup contents = flowSnapshot.getFlowContents();
            for (final VersionedProcessGroup child : contents.getProcessGroups()) {
                populateVersionedContentsRecursively(child);
            }
        }

        return flowSnapshot;
    }

    protected String getBaseRegistryUrl(final String storageLocation) {
        final Matcher matcher = REGISTRY_URL_PATTERN.matcher(storageLocation);
        if (matcher.matches()) {
            return matcher.group(1);
        } else {
            return storageLocation;
        }
    }

    private void populateVersionedContentsRecursively(final VersionedProcessGroup group) throws NiFiRegistryException, IOException {
        if (group == null) {
            return;
        }

        final VersionedFlowCoordinates coordinates = group.getVersionedFlowCoordinates();
        if (coordinates != null) {
            final String subRegistryUrl = getBaseRegistryUrl(coordinates.getStorageLocation());
            final String bucketId = coordinates.getBucketId();
            final String flowId = coordinates.getFlowId();
            final int version = Integer.parseInt(coordinates.getVersion());

            final RegistryUtil subFlowUtil = getSubRegistryUtil(subRegistryUrl);
            final VersionedFlowSnapshot snapshot = subFlowUtil.getFlowByID(bucketId, flowId, version);
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

    private RegistryUtil getSubRegistryUtil(final String subRegistryUrl) {
        final RegistryUtil subRegistryUtil;
        if (registryUrl.startsWith(subRegistryUrl)) {
            // Share current Registry Client for matching Registry URL
            subRegistryUtil = new RegistryUtil(registryClient, subRegistryUrl, sslContext);
        } else {
            subRegistryUtil = new RegistryUtil(subRegistryUrl, sslContext);
        }
        return subRegistryUtil;
    }
}
