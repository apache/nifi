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

import java.net.SocketAddress;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

public class CallEventListener {
    private final Call call;
    private final Map<String, Timing> dnsTimings = new HashMap<>();
    private final Map<String, Timing> establishConnectionTiming = new HashMap<>();
    private long callStart;
    private long callEnd;
    private long responseBodyStart;
    private long responseBodyEnd;
    private long responseHeaderStart;
    private long responseHeaderEnd;
    private long requestHeaderStart;
    private long requestHeaderEnd;
    private long requestBodyStart;
    private long requestBodyEnd;
    private long secureConnectStart;
    private long secureConnectEnd;


    public CallEventListener(final Call call) {
        this.call = call;
    }

    public void callStart() {
        callStart = System.nanoTime();
    }

    public void callEnd() {
        callEnd = System.nanoTime();
    }

    public void dnsStart(final String domainName) {
        dnsTimings.computeIfAbsent(domainName, k -> new Timing(domainName)).start();
    }

    public void dnsEnd(final String domainName) {
        dnsTimings.computeIfAbsent(domainName, k -> new Timing(domainName)).end();
    }

    public void responseBodyStart() {
        responseBodyStart = System.nanoTime();
    }

    public void responseBodyEnd() {
        responseBodyEnd = System.nanoTime();
    }

    public void responseHeaderStart() {
        responseHeaderStart = System.nanoTime();
    }

    public void responseHeaderEnd() {
        responseHeaderEnd = System.nanoTime();
    }

    public void requestHeaderStart() {
        requestHeaderStart = System.nanoTime();
    }

    public void requestHeaderEnd() {
        requestHeaderEnd = System.nanoTime();
    }

    public void requestBodyStart() {
        requestBodyStart = System.nanoTime();
    }

    public void requestBodyEnd() {
        requestBodyEnd = System.nanoTime();
    }

    public void connectStart(final SocketAddress address) {
        establishConnectionTiming.computeIfAbsent(address.toString(), Timing::new).start();
    }

    public void connectionAcquired(final SocketAddress address) {
        establishConnectionTiming.computeIfAbsent(address.toString(), Timing::new).end();
    }

    public void secureConnectStart() {
        secureConnectStart = System.nanoTime();
    }

    public void secureConnectEnd() {
        secureConnectEnd = System.nanoTime();
    }

    public Call getCall() {
        return call;
    }

    @Override
    public String toString() {
        final NumberFormat numberFormat = NumberFormat.getInstance();

        return "CallEventListener{" +
            "url=" + call.request().url() +
            ", dnsTimings=" + dnsTimings.values() +
            ", establishConnectionTiming=" + establishConnectionTiming.values() +
            ", tlsInitialization=" + numberFormat.format(secureConnectEnd - secureConnectStart) + " nanos" +
            ", writeRequestHeaders=" + numberFormat.format(requestHeaderEnd - requestHeaderStart) + " nanos" +
            ", writeRequestBody=" + numberFormat.format(requestBodyEnd - requestBodyStart) + " nanos" +
            ", readResponseHeaders=" + numberFormat.format(responseHeaderEnd - responseHeaderStart) + " nanos" +
            ", readResponseBody=" + numberFormat.format(responseBodyEnd - responseBodyStart) + " nanos" +
            ", callTime=" + numberFormat.format(callEnd - callStart) + " nanos" +
            '}';
    }

    private static class Timing {
        private final String address;
        private long start;
        private long nanos;

        public Timing(final String address) {
            this.address = address;
        }

        public String getAddress() {
            return address;
        }

        public void start() {
            start = System.nanoTime();
        }

        public void end() {
            if (start > 0) {
                nanos += (System.nanoTime() - start);
            }
        }

        public String toString() {
            return "{address=" + address + ", nanos=" + NumberFormat.getInstance().format(nanos) + "}";
        }
    }
}
