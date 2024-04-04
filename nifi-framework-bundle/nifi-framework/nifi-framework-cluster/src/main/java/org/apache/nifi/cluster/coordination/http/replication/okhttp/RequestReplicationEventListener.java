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

package org.apache.nifi.cluster.coordination.http.replication.okhttp;

import okhttp3.Call;
import okhttp3.Connection;
import okhttp3.EventListener;
import okhttp3.Handshake;
import okhttp3.Request;
import okhttp3.Response;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class RequestReplicationEventListener extends EventListener {
    private static final Logger logger = LoggerFactory.getLogger(RequestReplicationEventListener.class);

    private final ConcurrentMap<Call, CallEventListener> eventListeners = new ConcurrentHashMap<>();

    private CallEventListener getListener(final Call call) {
        return eventListeners.computeIfAbsent(call, CallEventListener::new);
    }

    @Override
    public void dnsStart(@NotNull final Call call, @NotNull final String domainName) {
        super.dnsStart(call, domainName);
        getListener(call).dnsStart(domainName);
    }

    @Override
    public void dnsEnd(@NotNull final Call call, @NotNull final String domainName, @NotNull final List<InetAddress> inetAddressList) {
        super.dnsEnd(call, domainName, inetAddressList);
        getListener(call).dnsEnd(domainName);
    }

    @Override
    public void callStart(@NotNull final Call call) {
        super.callStart(call);
        getListener(call).callStart();
    }

    @Override
    public void callEnd(@NotNull final Call call) {
        super.callEnd(call);
        final CallEventListener callListener = getListener(call);
        callListener.callEnd();

        logTimingInfo(callListener);
        eventListeners.remove(call);
    }

    @Override
    public void callFailed(@NotNull final Call call, @NotNull final IOException ioe) {
        super.callFailed(call, ioe);

        final CallEventListener callListener = getListener(call);
        callListener.callEnd();

        logTimingInfo(callListener);
        eventListeners.remove(call);
    }

    @Override
    public void responseBodyStart(@NotNull final Call call) {
        super.responseBodyStart(call);
        getListener(call).responseBodyStart();
    }

    @Override
    public void responseBodyEnd(@NotNull final Call call, final long byteCount) {
        super.responseBodyEnd(call, byteCount);
        getListener(call).responseBodyEnd();
    }

    @Override
    public void responseFailed(@NotNull final Call call, @NotNull final IOException ioe) {
        super.responseFailed(call, ioe);
        getListener(call).responseBodyEnd();
    }

    @Override
    public void responseHeadersStart(@NotNull final Call call) {
        super.responseHeadersStart(call);
        getListener(call).responseHeaderStart();
    }

    @Override
    public void responseHeadersEnd(@NotNull final Call call, @NotNull final Response response) {
        super.responseHeadersEnd(call, response);
        getListener(call).responseHeaderEnd();
    }

    @Override
    public void requestHeadersStart(@NotNull final Call call) {
        super.requestHeadersStart(call);
        getListener(call).requestHeaderStart();
    }

    @Override
    public void requestHeadersEnd(@NotNull final Call call, @NotNull final Request request) {
        super.requestHeadersEnd(call, request);
        getListener(call).requestHeaderEnd();
    }

    @Override
    public void requestBodyStart(@NotNull final Call call) {
        super.requestBodyStart(call);
        getListener(call).requestBodyStart();
    }

    @Override
    public void requestBodyEnd(@NotNull final Call call, final long byteCount) {
        super.requestBodyEnd(call, byteCount);
        getListener(call).requestBodyEnd();
    }

    @Override
    public void requestFailed(@NotNull final Call call, @NotNull final IOException ioe) {
        super.requestFailed(call, ioe);
        getListener(call).requestBodyEnd();
    }

    @Override
    public void connectStart(@NotNull final Call call, @NotNull final InetSocketAddress inetSocketAddress, @NotNull final Proxy proxy) {
        super.connectStart(call, inetSocketAddress, proxy);
        getListener(call).connectStart(inetSocketAddress);
    }

    @Override
    public void connectionAcquired(@NotNull final Call call, @NotNull final Connection connection) {
        super.connectionAcquired(call, connection);
        getListener(call).connectionAcquired(connection.socket().getRemoteSocketAddress());
    }

    @Override
    public void secureConnectStart(@NotNull final Call call) {
        super.secureConnectStart(call);
        getListener(call).secureConnectStart();
    }

    @Override
    public void secureConnectEnd(@NotNull final Call call, @Nullable final Handshake handshake) {
        super.secureConnectEnd(call, handshake);
        getListener(call).secureConnectEnd();
    }

    private void logTimingInfo(final CallEventListener eventListener) {
        logger.debug("Timing information {}", eventListener);
    }
}
