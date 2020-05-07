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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.remote.util.StandardDataPacket;
import org.apache.nifi.util.FormatUtils;

import javax.net.ssl.SSLContext;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class StatelessRemoteInputPort extends AbstractStatelessComponent {
    private final Queue<StatelessFlowFile> inputQueue = new LinkedList<>();
    private final SiteToSiteClient client;
    private final String name;
    private final String url;

    private final ComponentLog logger = new SLF4JComponentLog(this);
    private final StatelessConnectionContext connectionContext = new StatelessConnectionContext() {
        @Override
        public void addConnection(final Relationship relationship) {
        }

        @Override
        public boolean isValid() {
            return true;
        }
    };

    public StatelessRemoteInputPort(final VersionedRemoteProcessGroup rpg, final VersionedRemoteGroupPort remotePort, final SSLContext sslContext) {
        final String timeout = rpg.getCommunicationsTimeout();
        final long timeoutMillis = FormatUtils.getTimeDuration(timeout, TimeUnit.MILLISECONDS);

        url = rpg.getTargetUris();
        name = remotePort.getName();

        client = new SiteToSiteClient.Builder()
            .portName(remotePort.getName())
            .timeout(timeoutMillis, TimeUnit.MILLISECONDS)
            .transportProtocol(SiteToSiteTransportProtocol.valueOf(rpg.getTransportProtocol()))
            .url(rpg.getTargetUris())
            .useCompression(remotePort.isUseCompression())
            .sslContext(sslContext)
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
        inputQueue.addAll(list);
    }

    @Override
    public boolean runRecursive(final Queue<InMemoryFlowFile> queue) {
        try {
            final Transaction transaction = client.createTransaction(TransferDirection.SEND);
            if (transaction == null) {
                getLogger().error("Unable to create a transaction for Remote Process Group {} to send to port {}", new Object[] {url, name});
                return false;
            }

            StatelessFlowFile flowFile;
            while ((flowFile = inputQueue.poll()) != null) {
                final DataPacket dataPacket = new StandardDataPacket(flowFile.getAttributes(), flowFile.getDataStream(), flowFile.getSize());
                transaction.send(dataPacket);
            }

            transaction.confirm();
            transaction.complete();
        } catch (final Exception e) {
            getLogger().error("Failed to send FlowFile via site-to-site", e);
            return false;
        }

        return true;
    }

    @Override
    public boolean isMaterializeContent() {
        return false;
    }
}
