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
package org.apache.nifi.remote.client;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.net.ssl.SSLContext;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.remote.util.SiteToSiteRestApiClient;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;

public class SiteInfoProvider {

    private static final long REMOTE_REFRESH_MILLIS = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    private final ReadWriteLock listeningPortRWLock = new ReentrantReadWriteLock();
    private final Lock remoteInfoReadLock = listeningPortRWLock.readLock();
    private final Lock remoteInfoWriteLock = listeningPortRWLock.writeLock();
    private Integer siteToSitePort;
    private Integer siteToSiteHttpPort;
    private Boolean siteToSiteSecure;
    private long remoteRefreshTime;
    private HttpProxy proxy;

    private final Map<String, String> inputPortMap = new HashMap<>(); // map input port name to identifier
    private final Map<String, String> outputPortMap = new HashMap<>(); // map output port name to identifier

    private URI clusterUrl;
    private SSLContext sslContext;
    private int connectTimeoutMillis;
    private int readTimeoutMillis;

    private ControllerDTO refreshRemoteInfo() throws IOException {
        final ControllerDTO controller;

        try (final SiteToSiteRestApiClient apiClient = new SiteToSiteRestApiClient(sslContext, proxy, EventReporter.NO_OP)) {
            apiClient.resolveBaseUrl(clusterUrl);
            apiClient.setConnectTimeoutMillis(connectTimeoutMillis);
            apiClient.setReadTimeoutMillis(readTimeoutMillis);
            controller = apiClient.getController();
        }

        remoteInfoWriteLock.lock();
        try {
            this.siteToSitePort = controller.getRemoteSiteListeningPort();
            this.siteToSiteHttpPort = controller.getRemoteSiteHttpListeningPort();
            this.siteToSiteSecure = controller.isSiteToSiteSecure();

            inputPortMap.clear();
            for (final PortDTO inputPort : controller.getInputPorts()) {
                inputPortMap.put(inputPort.getName(), inputPort.getId());
            }

            outputPortMap.clear();
            for (final PortDTO outputPort : controller.getOutputPorts()) {
                outputPortMap.put(outputPort.getName(), outputPort.getId());
            }

            this.remoteRefreshTime = System.currentTimeMillis();
        } finally {
            remoteInfoWriteLock.unlock();
        }

        return controller;
    }

    public boolean isWebInterfaceSecure() {
        return clusterUrl.toString().startsWith("https");
    }

    /**
     * @return the port that the remote instance is listening on for
     * RAW Socket site-to-site communication, or <code>null</code> if the remote instance
     * is not configured to allow site-to-site communications.
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    public Integer getSiteToSitePort() throws IOException {
        Integer listeningPort;
        remoteInfoReadLock.lock();
        try {
            listeningPort = this.siteToSitePort;
            if (listeningPort != null && this.remoteRefreshTime > System.currentTimeMillis() - REMOTE_REFRESH_MILLIS) {
                return listeningPort;
            }
        } finally {
            remoteInfoReadLock.unlock();
        }

        final ControllerDTO controller = refreshRemoteInfo();
        listeningPort = controller.getRemoteSiteListeningPort();

        return listeningPort;
    }

    /**
     * @return the port that the remote instance is listening on for
     * HTTP(S) site-to-site communication, or <code>null</code> if the remote instance
     * is not configured to allow site-to-site communications.
     *
     * @throws IOException if unable to communicate with the remote instance
     */
    public Integer getSiteToSiteHttpPort() throws IOException {
        Integer listeningHttpPort;
        remoteInfoReadLock.lock();
        try {
            listeningHttpPort = this.siteToSiteHttpPort;
            if (listeningHttpPort != null && this.remoteRefreshTime > System.currentTimeMillis() - REMOTE_REFRESH_MILLIS) {
                return listeningHttpPort;
            }
        } finally {
            remoteInfoReadLock.unlock();
        }

        final ControllerDTO controller = refreshRemoteInfo();
        listeningHttpPort = controller.getRemoteSiteHttpListeningPort();

        return listeningHttpPort;
    }

    /**
     * @return {@code true} if the remote instance is configured for secure
     * site-to-site communications, {@code false} otherwise
     * @throws IOException if unable to check if secure
     */
    public boolean isSecure() throws IOException {
        remoteInfoReadLock.lock();
        try {
            final Boolean secure = this.siteToSiteSecure;
            if (secure != null && this.remoteRefreshTime > System.currentTimeMillis() - REMOTE_REFRESH_MILLIS) {
                return secure;
            }
        } finally {
            remoteInfoReadLock.unlock();
        }

        final ControllerDTO controller = refreshRemoteInfo();
        final Boolean isSecure = controller.isSiteToSiteSecure();
        if (isSecure == null) {
            throw new IOException("Remote NiFi instance " + clusterUrl + " is not currently configured to accept site-to-site connections");
        }

        return isSecure;
    }

    public String getPortIdentifier(final String portName, final TransferDirection transferDirection) throws  IOException {
        if (transferDirection == TransferDirection.RECEIVE) {
            return getOutputPortIdentifier(portName);
        } else {
            return getInputPortIdentifier(portName);
        }
    }

    public String getInputPortIdentifier(final String portName) throws IOException {
        return getPortIdentifier(portName, inputPortMap);
    }

    public String getOutputPortIdentifier(final String portName) throws IOException {
        return getPortIdentifier(portName, outputPortMap);
    }

    private String getPortIdentifier(final String portName, final Map<String, String> portMap) throws IOException {
        String identifier;
        remoteInfoReadLock.lock();
        try {
            identifier = portMap.get(portName);
        } finally {
            remoteInfoReadLock.unlock();
        }

        if (identifier != null) {
            return identifier;
        }

        refreshRemoteInfo();

        remoteInfoReadLock.lock();
        try {
            return portMap.get(portName);
        } finally {
            remoteInfoReadLock.unlock();
        }
    }

    public void setClusterUrl(URI clusterUrl) {
        this.clusterUrl = clusterUrl;
    }

    public void setSslContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public void setReadTimeoutMillis(int readTimeoutMillis) {
        this.readTimeoutMillis = readTimeoutMillis;
    }

    public void setProxy(HttpProxy proxy) {
        this.proxy = proxy;
    }
}
