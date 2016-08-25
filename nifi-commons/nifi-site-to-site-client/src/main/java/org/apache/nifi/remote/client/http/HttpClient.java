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
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
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
    private final Set<HttpClientTransaction> activeTransactions = Collections.synchronizedSet(new HashSet<>());

    public HttpClient(final SiteToSiteClientConfig config) {
        super(config);

        peerSelector = new PeerSelector(this, config.getPeerPersistenceFile());
        peerSelector.setEventReporter(config.getEventReporter());

        taskExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultFactory.newThread(r);
                thread.setName("Http Site-to-Site PeerSelector");
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
    public PeerDescription getBootstrapPeerDescription() throws IOException {
        if (siteInfoProvider.getSiteToSiteHttpPort() == null) {
            throw new IOException("Remote instance of NiFi is not configured to allow HTTP site-to-site communications");
        }

        final URI clusterUrl;
        try {
            clusterUrl = new URI(config.getUrl());
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Specified clusterUrl was: " + config.getUrl(), e);
        }

        return new PeerDescription(clusterUrl.getHost(), siteInfoProvider.getSiteToSiteHttpPort(), siteInfoProvider.isSecure());
    }

    @Override
    public Set<PeerStatus> fetchRemotePeerStatuses(PeerDescription peerDescription) throws IOException {
        // Each node should has the same URL structure and network reach-ability with the proxy configuration.
        try (final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(config.getSslContext(), config.getHttpProxy(), config.getEventReporter())) {
            final String scheme = peerDescription.isSecure() ? "https" : "http";
            final String clusterApiUrl = apiClient.resolveBaseUrl(scheme, peerDescription.getHostname(), peerDescription.getPort());

            final int timeoutMillis = (int) config.getTimeout(TimeUnit.MILLISECONDS);
            apiClient.setConnectTimeoutMillis(timeoutMillis);
            apiClient.setReadTimeoutMillis(timeoutMillis);

            final Collection<PeerDTO> peers = apiClient.getPeers();
            if(peers == null || peers.size() == 0){
                throw new IOException("Couldn't get any peer to communicate with. " + clusterApiUrl + " returned zero peers.");
            }

            // Convert the PeerDTO's to PeerStatus objects. Use 'true' for the query-peer-for-peers flag because Site-to-Site over HTTP
            // was added in NiFi 1.0.0, which means that peer-to-peer queries are always allowed.
            return peers.stream().map(p -> new PeerStatus(new PeerDescription(p.getHostname(), p.getPort(), p.isSecure()), p.getFlowFileCount(), true))
                    .collect(Collectors.toSet());
        }
    }

    @Override
    public Transaction createTransaction(final TransferDirection direction) throws HandshakeException, PortNotRunningException, ProtocolException, UnknownPortException, IOException {
        final int timeoutMillis = (int) config.getTimeout(TimeUnit.MILLISECONDS);

        PeerStatus peerStatus;
        while ((peerStatus = peerSelector.getNextPeerStatus(direction)) != null) {
            logger.debug("peerStatus={}", peerStatus);

            final CommunicationsSession commSession = new HttpCommunicationsSession();
            final String nodeApiUrl = resolveNodeApiUrl(peerStatus.getPeerDescription());
            final String clusterUrl = config.getUrl();
            final Peer peer = new Peer(peerStatus.getPeerDescription(), commSession, nodeApiUrl, clusterUrl);

            final int penaltyMillis = (int) config.getPenalizationPeriod(TimeUnit.MILLISECONDS);
            String portId = config.getPortIdentifier();
            if (StringUtils.isEmpty(portId)) {
                portId = siteInfoProvider.getPortIdentifier(config.getPortName(), direction);
                if (StringUtils.isEmpty(portId)) {
                    peer.close();
                    throw new IOException("Failed to determine the identifier of port " + config.getPortName());
                }
            }

            final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(config.getSslContext(), config.getHttpProxy(), config.getEventReporter());

            apiClient.setBaseUrl(peer.getUrl());
            apiClient.setConnectTimeoutMillis(timeoutMillis);
            apiClient.setReadTimeoutMillis(timeoutMillis);

            apiClient.setCompress(config.isUseCompression());
            apiClient.setRequestExpirationMillis(config.getIdleConnectionExpiration(TimeUnit.MILLISECONDS));
            apiClient.setBatchCount(config.getPreferredBatchCount());
            apiClient.setBatchSize(config.getPreferredBatchSize());
            apiClient.setBatchDurationMillis(config.getPreferredBatchDuration(TimeUnit.MILLISECONDS));

            final String transactionUrl;
            try {
                transactionUrl = apiClient.initiateTransaction(direction, portId);
                commSession.setUserDn(apiClient.getTrustedPeerDn());
            } catch (final Exception e) {
                apiClient.close();
                logger.debug("Penalizing a peer due to {}", e.getMessage());
                peerSelector.penalize(peer, penaltyMillis);

                if (e instanceof UnknownPortException || e instanceof PortNotRunningException) {
                    throw e;
                }

                logger.debug("Continue trying other peers...");
                continue;
            }

            // We found a valid peer to communicate with.
            final Integer transactionProtocolVersion = apiClient.getTransactionProtocolVersion();
            final HttpClientTransaction transaction = new HttpClientTransaction(transactionProtocolVersion, peer, direction,
                config.isUseCompression(), portId, penaltyMillis, config.getEventReporter()) {

                @Override
                protected void close() throws IOException {
                    try {
                        super.close();
                    } finally {
                        activeTransactions.remove(this);
                    }
                }
            };

            try {
                transaction.initialize(apiClient, transactionUrl);
            } catch (final Exception e) {
                transaction.error();
                throw e;
            }

            activeTransactions.add(transaction);
            return transaction;
        }

        logger.info("Couldn't find a valid peer to communicate with.");
        return null;
    }

    private String resolveNodeApiUrl(final PeerDescription description) {
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

        for (final HttpClientTransaction transaction : activeTransactions) {
            transaction.getCommunicant().getCommunicationsSession().interrupt();
        }
    }
}
