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

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.stateless.bootstrap.InMemoryFlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.BatchSize;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.FormatUtils;

import javax.net.ssl.SSLContext;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StatelessRemoteOutputPort extends AbstractStatelessComponent {
    public final int DEFAULT_BATCH_COUNT = 1;
    public final long DEFAULT_BATCH_BYTES = 1L;
    public final long DEFAULT_BATCH_MILLIS = 1L;

    private final SiteToSiteClient client;
    private final String url;
    private final String name;

    private final ComponentLog logger = new SLF4JComponentLog(this);
    private final StatelessConnectionContext connectionContext = new StatelessPassThroughConnectionContext();

    public StatelessRemoteOutputPort(final String timeout, final String remotePortName, final String targetUris, final BatchSize batchSize,
                                     final String transportProtocol, final Boolean useCompression, final SSLContext sslContext) {
        final long timeoutMillis = FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS);

        this.url = targetUris;
        this.name = remotePortName;

        final int batchCount;
        final long batchBytes;
        final long batchMillis;
        if (batchSize == null) {
            batchCount = DEFAULT_BATCH_COUNT;
            batchBytes =  DEFAULT_BATCH_BYTES;
            batchMillis =  DEFAULT_BATCH_MILLIS;
        } else {
            batchCount = batchSize.getCount() == null ? DEFAULT_BATCH_COUNT : batchSize.getCount();
            batchBytes = batchSize.getSize() == null ? DEFAULT_BATCH_BYTES : DataUnit.parseDataSize(batchSize.getSize(), DataUnit.B).longValue();
            batchMillis = batchSize.getDuration() == null ? DEFAULT_BATCH_MILLIS : FormatUtils.getTimeDuration(batchSize.getDuration(), TimeUnit.MILLISECONDS);
        }

        this.client = new SiteToSiteClient.Builder()
                .portName(remotePortName)
                .timeout(timeoutMillis, TimeUnit.MILLISECONDS)
                .requestBatchCount(batchCount)
                .requestBatchDuration(batchMillis, TimeUnit.MILLISECONDS)
                .requestBatchSize(batchBytes)
                .transportProtocol(SiteToSiteTransportProtocol.valueOf(transportProtocol))
                .url(targetUris)
                .sslContext(sslContext)
                .useCompression(useCompression)
                .eventReporter(EventReporter.NO_OP)
                .build();
    }

    @Override
    public Set<Relationship> getRelationships() {
        return Collections.emptySet();
    }

    @Override
    protected StatelessConnectionContext getContext() {
        return connectionContext;
    }

    @Override
    protected ComponentLog getLogger() {
        return logger;
    }

    @Override
    public void shutdown() {

    }

    @Override
    public void enqueueAll(final Queue<StatelessFlowFile> list) {
        throw new UnsupportedOperationException("Cannot enqueue FlowFiles for a Remote Output Port");
    }

    @Override
    public boolean runRecursive(final Queue<InMemoryFlowFile> queue) {
        try {
            final Transaction transaction = client.createTransaction(TransferDirection.RECEIVE);
            if (transaction == null) {
                getLogger().error("Unable to create a transaction for Remote Process Group {} to pull from port {}", new Object[]{url, name});
                return false;
            }

            final Queue<StatelessFlowFile> destinationQueue = new LinkedList<>();
            DataPacket dataPacket;
            while ((dataPacket = transaction.receive()) != null) {
                final Map<String, String> attributes = dataPacket.getAttributes();
                final InputStream in = dataPacket.getData();
                final byte[] buffer = new byte[(int) dataPacket.getSize()];
                StreamUtils.fillBuffer(in, buffer);

                final StatelessFlowFile receivedFlowFile = new StatelessFlowFile(buffer, attributes, true);
                destinationQueue.add(receivedFlowFile);

                for (final StatelessComponent childComponent : getChildren().get(Relationship.ANONYMOUS)) {
                    childComponent.enqueueAll(destinationQueue);
                    childComponent.runRecursive(queue);
                }

                destinationQueue.clear();
            }

            transaction.confirm();
            transaction.complete();
        } catch (final Exception e) {
            getLogger().error("Failed to receive FlowFile via site-to-site", e);
            return false;
        }

        return true;
    }

    @Override
    public boolean isMaterializeContent() {
        return false;
    }
}
