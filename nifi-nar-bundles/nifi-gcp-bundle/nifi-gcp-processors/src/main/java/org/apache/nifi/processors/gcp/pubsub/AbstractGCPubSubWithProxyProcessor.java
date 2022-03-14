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
package org.apache.nifi.processors.gcp.pubsub;

import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.proxy.ProxyConfiguration;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class AbstractGCPubSubWithProxyProcessor extends AbstractGCPubSubProcessor {
    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
                PROJECT_ID,
                ProxyConfiguration.createProxyConfigPropertyDescriptor(true, ProxyAwareTransportFactory.PROXY_SPECS),
                GCP_CREDENTIALS_PROVIDER_SERVICE)
        );
    }
    protected TransportChannelProvider getTransportChannelProvider(ProcessContext context) {
        final ProxyConfiguration proxyConfiguration = ProxyConfiguration.getConfiguration(context, () -> {
            final String proxyHost = context.getProperty(PROXY_HOST).evaluateAttributeExpressions().getValue();
            final Integer proxyPort = context.getProperty(PROXY_PORT).evaluateAttributeExpressions().asInteger();
            if (proxyHost != null && proxyPort != null && proxyPort > 0) {
                final ProxyConfiguration componentProxyConfig = new ProxyConfiguration();
                final String proxyUser = context.getProperty(HTTP_PROXY_USERNAME).evaluateAttributeExpressions().getValue();
                final String proxyPassword = context.getProperty(HTTP_PROXY_PASSWORD).evaluateAttributeExpressions().getValue();
                componentProxyConfig.setProxyType(Proxy.Type.HTTP);
                componentProxyConfig.setProxyServerHost(proxyHost);
                componentProxyConfig.setProxyServerPort(proxyPort);
                componentProxyConfig.setProxyUserName(proxyUser);
                componentProxyConfig.setProxyUserPassword(proxyPassword);
                return componentProxyConfig;
            }
            return ProxyConfiguration.DIRECT_CONFIGURATION;
        });

        return TopicAdminSettings.defaultGrpcTransportProviderBuilder()
                .setChannelConfigurator(managedChannelBuilder -> managedChannelBuilder.proxyDetector(
                        new ProxyDetector() {
                            @Nullable
                            @Override
                            public ProxiedSocketAddress proxyFor(SocketAddress socketAddress) {
                                if (Proxy.Type.HTTP.equals(proxyConfiguration.getProxyType())) {
                                    return HttpConnectProxiedSocketAddress.newBuilder()
                                            .setUsername(proxyConfiguration.getProxyUserName())
                                            .setPassword(proxyConfiguration.getProxyUserPassword())
                                            .setProxyAddress(new InetSocketAddress(proxyConfiguration.getProxyServerHost(),
                                                    proxyConfiguration.getProxyServerPort()))
                                            .setTargetAddress((InetSocketAddress) socketAddress)
                                            .build();
                                } else {
                                    return null;
                                }
                            }
                        }))
                .build();
    }
}