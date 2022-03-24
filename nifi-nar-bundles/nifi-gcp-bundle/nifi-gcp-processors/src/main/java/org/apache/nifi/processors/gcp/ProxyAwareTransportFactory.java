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
package org.apache.nifi.processors.gcp;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.apache.ApacheHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpTransportFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.params.ConnRouteParams;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

import java.net.Proxy;

public class ProxyAwareTransportFactory implements HttpTransportFactory {

    private static final HttpTransport DEFAULT_TRANSPORT = new NetHttpTransport();
    public static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};

    private final ProxyConfiguration proxyConfig;

    public ProxyAwareTransportFactory(ProxyConfiguration proxyConfig) {
        this.proxyConfig = proxyConfig;
    }

    @Override
    public HttpTransport create() {

        if (proxyConfig == null) {
            return DEFAULT_TRANSPORT;
        }

        final Proxy proxy = proxyConfig.createProxy();

        if (Proxy.Type.HTTP.equals(proxy.type()) && proxyConfig.hasCredential()) {
            // If it requires authentication via username and password, use ApacheHttpTransport
            final String host = proxyConfig.getProxyServerHost();
            final int port = proxyConfig.getProxyServerPort();
            final HttpHost proxyHost = new HttpHost(host, port);

            final DefaultHttpClient httpClient = new DefaultHttpClient();
            ConnRouteParams.setDefaultProxy(httpClient.getParams(), proxyHost);

            if (proxyConfig.hasCredential()) {
                final AuthScope proxyAuthScope = new AuthScope(host, port);
                final UsernamePasswordCredentials proxyCredential
                        = new UsernamePasswordCredentials(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                credentialsProvider.setCredentials(proxyAuthScope, proxyCredential);
                httpClient.setCredentialsProvider(credentialsProvider);
            }

            return new ApacheHttpTransport(httpClient);

        }

        return new NetHttpTransport.Builder().setProxy(proxy).build();
    }
}