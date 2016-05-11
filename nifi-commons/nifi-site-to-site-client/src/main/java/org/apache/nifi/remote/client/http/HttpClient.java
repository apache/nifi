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
package org.apache.nifi.remote.client.http;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.PeerStatus;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.AbstractSiteToSiteClient;
import org.apache.nifi.remote.client.PeerSelector;
import org.apache.nifi.remote.client.PeerStatusProvider;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.nifi.remote.exception.HandshakeException;
import org.apache.nifi.remote.exception.PortNotRunningException;
import org.apache.nifi.remote.exception.ProtocolException;
import org.apache.nifi.remote.exception.UnknownPortException;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.http.HttpClientTransaction;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class HttpClient extends AbstractSiteToSiteClient implements PeerStatusProvider {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    private final ScheduledExecutorService taskExecutor;
    private final PeerSelector peerSelector;

    public HttpClient(final SiteToSiteClientConfig config) {
        super(config);

        peerSelector = new PeerSelector(this, config.getPeerPersistenceFile());
        peerSelector.setEventReporter(config.getEventReporter());

        taskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName("NiFi Site-to-Site Http Maintenance");
                thread.setDaemon(true);
                return thread;
            }
        });

        taskExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                peerSelector.refreshPeers();
            }
        }, 0, 5, TimeUnit.SECONDS);

    }

    @Override
    public Set<PeerStatus> fetchRemotePeerStatuses() throws IOException {
        // TODO: Set protocol version header.
        String clusterUrl = config.getUrl();
        SiteToSiteRestApiUtil apiUtil = new SiteToSiteRestApiUtil(config.getSslContext());
        String clusterApiUri = apiUtil.resolveBaseUrl(clusterUrl);

        int timeoutMillis = (int) config.getTimeout(TimeUnit.MILLISECONDS);
        apiUtil.setConnectTimeoutMillis(timeoutMillis);
        apiUtil.setReadTimeoutMillis(timeoutMillis);
        Collection<PeerDTO> peers = apiUtil.getPeers();
        if(peers == null || peers.size() == 0){
            throw new PortNotRunningException("Couldn't get any peer to communicate with. " + clusterApiUri + " returned zero peers.");
        }

        return peers.stream().map(p -> new PeerStatus(new PeerDescription(p.getHostname(), p.getPort(), p.isSecure()), p.getFlowFileCount())).collect(Collectors.toSet());
    }

    @Override
    public Transaction createTransaction(TransferDirection direction) throws HandshakeException, PortNotRunningException, ProtocolException, UnknownPortException, IOException {

        int timeoutMillis = (int) config.getTimeout(TimeUnit.MILLISECONDS);

        final PeerStatus peerStatus = peerSelector.getNextPeerStatus(direction);

        CommunicationsSession commSession = new HttpCommunicationsSession();
        String nodeApiUrl = resolveNodeApiUrl(peerStatus.getPeerDescription());
        commSession.setUri(nodeApiUrl);
        String clusterUrl = config.getUrl();
        Peer peer = new Peer(peerStatus.getPeerDescription(), commSession, nodeApiUrl, clusterUrl);

        // TODO: handshake with the selected Peer, by checking port status, see EndpointConnectionPool


        // TODO: add version negotiation
        int penaltyMillis = (int)config.getPenalizationPeriod(TimeUnit.MILLISECONDS);
        int protocolVersion = 5;
        String portId = config.getPortIdentifier();
        if(StringUtils.isEmpty(portId)){
            portId = siteInfoProvider.getPortIdentifier(config.getPortName(), direction);
        }
        HttpClientTransaction transaction = new HttpClientTransaction(protocolVersion, peer, direction,
                config.isUseCompression(), portId, penaltyMillis, config.getEventReporter());
        SiteToSiteRestApiUtil apiUtil = new SiteToSiteRestApiUtil(config.getSslContext());
        apiUtil.setBaseUrl(peer.getUrl());
        apiUtil.setConnectTimeoutMillis(timeoutMillis);
        apiUtil.setReadTimeoutMillis(timeoutMillis);
        transaction.initialize(apiUtil);

        return transaction;
    }

    private String resolveNodeApiUrl(PeerDescription description) {
        return (description.isSecure() ? "https" : "http") + "://" + description.getHostname() + ":" + description.getPort() + "/nifi-api";
    }

    @Override
    public boolean isSecure() throws IOException {
        return siteInfoProvider.isWebInterfaceSecure();
    }

    @Override
    public void close() throws IOException {
        taskExecutor.shutdown();
        peerSelector.clear();
    }
}
