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

package org.apache.nifi.processors.stateless.retrieval;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.stateless.ExecuteStateless;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.client.NiFiRegistryClient;
import org.apache.nifi.registry.client.NiFiRegistryClientConfig;
import org.apache.nifi.registry.client.NiFiRegistryException;
import org.apache.nifi.registry.client.impl.JerseyNiFiRegistryClient;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public class RegistryDataflowProvider implements DataflowProvider {
    private final ComponentLog logger;

    public RegistryDataflowProvider(final ComponentLog logger) {
        this.logger = logger;
    }

    @Override
    public VersionedFlowSnapshot retrieveDataflowContents(final ProcessContext context) throws IOException {
        final SSLContextService sslContextService = context.getProperty(ExecuteStateless.SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        final SSLContext sslContext = sslContextService == null ? null : sslContextService.createContext();

        final String url = context.getProperty(ExecuteStateless.REGISTRY_URL).getValue();
        final NiFiRegistryClientConfig clientConfig = new NiFiRegistryClientConfig.Builder()
            .baseUrl(url)
            .connectTimeout(context.getProperty(ExecuteStateless.COMMS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue())
            .readTimeout(context.getProperty(ExecuteStateless.COMMS_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue())
            .sslContext(sslContext)
            .build();

        final NiFiRegistryClient client = new JerseyNiFiRegistryClient.Builder()
            .config(clientConfig)
            .build();

        final VersionedFlowSnapshot versionedFlowSnapshot;
        final String bucketName = context.getProperty(ExecuteStateless.BUCKET).getValue();
        final String flowName = context.getProperty(ExecuteStateless.FLOW_NAME).getValue();
        final Integer flowVersion = context.getProperty(ExecuteStateless.FLOW_VERSION).asInteger();
        try {
            final String bucketId = getBucketId(client, bucketName);
            final String flowId = getFlowId(client, flowName, bucketId);

            logger.debug("Attempting to fetch dataflow from Registry at URL {}, Bucket {}, Flow {}, flowVersion {}", url, bucketId, flowId, flowVersion == null ? "<Latest>" : flowVersion);
            if (flowVersion == null) {
                versionedFlowSnapshot = client.getFlowSnapshotClient().getLatest(bucketId, flowId);
            } else {
                versionedFlowSnapshot = client.getFlowSnapshotClient().get(bucketId, flowId, flowVersion);
            }

            logger.debug("Successfully fetched dataflow from Registry at URL {}, Bucket {}, Flow {}, flowVersion {}", url, bucketId, flowId, flowVersion == null ? "<Latest>" : flowVersion);
        } catch (final NiFiRegistryException e) {
            throw new IOException("Failed to retrieve Flow Snapshot from Registry", e);
        }

        return versionedFlowSnapshot;
    }

    private String getFlowId(final NiFiRegistryClient client, final String flowName, final String bucketId) throws IOException {
        final List<VersionedFlow> versionedFlows;
        try {
            versionedFlows = client.getFlowClient().getByBucket(bucketId);
        } catch (NiFiRegistryException e) {
            throw new IOException("Could not retrieve list of Flows from NiFi Registry for Bucket ID " + bucketId);
        }

        for (final VersionedFlow versionedFlow : versionedFlows) {
            if (flowName.equals(versionedFlow.getName())) {
                return versionedFlow.getIdentifier();
            }
        }

        throw new IOException("Could not find a flow with the name '" + flowName + "' within bucket with ID '" + bucketId + "' in the given Registry");
    }

    private String getBucketId(final NiFiRegistryClient client, final String bucketName) throws IOException {
        try {
            final List<Bucket> allBuckets = client.getBucketClient().getAll();
            final Optional<Bucket> optionalBucket = allBuckets.stream().filter(bkt -> bkt.getName().equals(bucketName)).findAny();
            if (!optionalBucket.isPresent()) {
                throw new IOException("Could not find a bucket with the name '" + bucketName + "' in the given Registry");
            }

            return optionalBucket.get().getIdentifier();
        } catch (NiFiRegistryException e) {
            throw new IOException("Failed to fetch buckets from NiFi Registry", e);
        }
    }
}
