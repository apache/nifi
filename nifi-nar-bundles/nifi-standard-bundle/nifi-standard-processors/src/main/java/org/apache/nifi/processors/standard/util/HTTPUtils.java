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
package org.apache.nifi.processors.standard.util;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;

import java.net.Proxy;
import java.util.Collection;
import java.util.Map;

public class HTTPUtils {

    public static final String HTTP_REQUEST_URI = "http.request.uri";
    public static final String HTTP_REMOTE_HOST = "http.remote.host";
    public static final String HTTP_LOCAL_NAME = "http.local.name";
    public static final String HTTP_PORT = "http.server.port";
    public static final String HTTP_SSL_CERT = "http.subject.dn";
    public static final String HTTP_CONTEXT_ID = "http.context.identifier";

    public static String getURI(Map<String, String> map) {
        final String client = map.get(HTTP_REMOTE_HOST);
        final String server = map.get(HTTP_LOCAL_NAME);
        final String port = map.get(HTTP_PORT);
        final String uri = map.get(HTTP_REQUEST_URI);
        if(map.get(HTTP_SSL_CERT) == null) {
            return "http://" + client + "@" + server + ":" + port + uri;
        } else {
            return "https://" + client + "@" + server + ":" + port + uri;
        }
    }

    public static final PropertyDescriptor PROXY_HOST = new PropertyDescriptor.Builder()
            .name("Proxy Host")
            .description("The fully qualified hostname or IP address of the proxy server")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROXY_PORT = new PropertyDescriptor.Builder()
            .name("Proxy Port")
            .description("The port of the proxy server")
            .required(false)
            .addValidator(StandardValidators.PORT_VALIDATOR)
            .build();

    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP_AUTH};
    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE
            = ProxyConfiguration.createProxyConfigPropertyDescriptor(true, PROXY_SPECS);


    public static void setProxy(final ProcessContext context, final HttpClientBuilder clientBuilder, final CredentialsProvider credentialsProvider) {
        // Set the proxy if specified
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context, () -> {
            if (context.getProperty(PROXY_HOST).isSet() && context.getProperty(PROXY_PORT).isSet()) {
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                final String host = context.getProperty(PROXY_HOST).getValue();
                final int port = context.getProperty(PROXY_PORT).asInteger();
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(host);
                componentProxyConfig.setProxyServerPort(port);
                return componentProxyConfig;
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        if (Proxy.Type.HTTP.equals(proxyConfig.getProxyType())) {
            final String host = proxyConfig.getProxyServerHost();
            final int port = proxyConfig.getProxyServerPort();
            clientBuilder.setProxy(new HttpHost(host, port));

            if (proxyConfig.hasCredential()) {
                final AuthScope proxyAuthScope = new AuthScope(host, port);
                final UsernamePasswordCredentials proxyCredential
                        = new UsernamePasswordCredentials(proxyConfig.getProxyUserName(), proxyConfig.getProxyUserPassword());
                credentialsProvider.setCredentials(proxyAuthScope, proxyCredential);
            }
        }
    }

    public static void validateProxyProperties(ValidationContext context, Collection<ValidationResult> results) {
        if (context.getProperty(PROXY_HOST).isSet() && !context.getProperty(PROXY_PORT).isSet()) {
            results.add(new ValidationResult.Builder()
                    .explanation("Proxy Host was set but no Proxy Port was specified")
                    .valid(false)
                    .subject("Proxy server configuration")
                    .build());
        }

        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }
}
