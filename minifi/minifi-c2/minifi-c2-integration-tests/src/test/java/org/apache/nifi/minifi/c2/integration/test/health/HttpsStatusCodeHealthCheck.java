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

package org.apache.nifi.minifi.c2.integration.test.health;

import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthCheck;
import com.palantir.docker.compose.connection.waiting.SuccessOrFailure;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

public class HttpsStatusCodeHealthCheck implements HealthCheck<List<Container>> {
    private final Function<Container, String> urlFunction;
    private final Function<List<Container>, Container> proxyExtractor;
    private final Function<List<Container>, Container> serverExtractor;
    private final Supplier<SSLSocketFactory> sslSocketFactorySupplier;
    private final int expected;

    public HttpsStatusCodeHealthCheck(Function<Container, String> urlFunction, Function<List<Container>, Container> proxyExtractor,
                                      Function<List<Container>, Container> serverExtractor, Supplier<SSLSocketFactory> sslSocketFactorySupplier, int expected) {
        this.urlFunction = urlFunction;
        this.proxyExtractor = proxyExtractor;
        this.serverExtractor = serverExtractor;
        this.sslSocketFactorySupplier = sslSocketFactorySupplier;
        this.expected = expected;
    }

    @Override
    public SuccessOrFailure isHealthy(List<Container> target) {

        return new HttpStatusCodeHealthCheck(urlFunction, expected) {
            @Override
            protected HttpURLConnection openConnection(String url) throws IOException {
                DockerPort dockerPort = proxyExtractor.apply(target).port(3128);
                return getHttpURLConnection(url, sslSocketFactorySupplier.get(), dockerPort.getIp(), dockerPort.getExternalPort());
            }
        }.isHealthy(serverExtractor.apply(target));
    }

    public static HttpURLConnection getHttpURLConnection(String url, SSLSocketFactory sslSocketFactory, String proxyHostname, int proxyPort) throws IOException {
        HttpsURLConnection httpURLConnection = (HttpsURLConnection) new URL(url).openConnection(
                new Proxy(Proxy.Type.HTTP, new InetSocketAddress(proxyHostname, proxyPort)));
        httpURLConnection.setSSLSocketFactory(sslSocketFactory);
        return httpURLConnection;
    }
}
