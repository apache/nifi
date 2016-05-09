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

import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.AbstractSiteToSiteClient;
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
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class HttpClient extends AbstractSiteToSiteClient {

    private static final Logger logger = LoggerFactory.getLogger(HttpClient.class);

    public HttpClient(final SiteToSiteClientConfig config) {
        super(config);
    }

    private final Random random = new Random();

    @Override
    public Transaction createTransaction(TransferDirection direction) throws HandshakeException, PortNotRunningException, ProtocolException, UnknownPortException, IOException {

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
        // TODO: Weighted Load balancing based on the number of flow files each port has.
        int nextIndex = random.nextInt(peers.size());
        logger.debug("Got peers: {}, nextIndex={}", peers, nextIndex);
        Iterator<PeerDTO> peersItr = peers.iterator();
        for(int i = 0; i < nextIndex; i++){
            peersItr.next();
        }
        PeerDTO nodeApiPeerDto = peersItr.next();

        PeerDescription description = new PeerDescription(nodeApiPeerDto.getHostname(), nodeApiPeerDto.getPort(), nodeApiPeerDto.isSecure());

        CommunicationsSession commSession = new HttpCommunicationsSession();
        String nodeApiUrl = resolveNodeApiUrl(description);
        commSession.setUri(nodeApiUrl);
        Peer peer = new Peer(description, commSession, nodeApiUrl, clusterUrl);

        // TODO: add version negotiation
        int penaltyMillis = (int)config.getPenalizationPeriod(TimeUnit.MILLISECONDS);
        int protocolVersion = 5;
        HttpClientTransaction transaction = new HttpClientTransaction(protocolVersion, peer, direction,
                config.isUseCompression(), config.getPortIdentifier(), penaltyMillis, config.getEventReporter());
        apiUtil = new SiteToSiteRestApiUtil(config.getSslContext());
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
        // TODO: This method is designed to determine whether it is secured by asking server. It will be implemented in handshaking process.
        return false;
    }

    @Override
    public void close() throws IOException {
        // TODO: Do we have anything to clean up here? If we adopt connection pooling
    }
}
