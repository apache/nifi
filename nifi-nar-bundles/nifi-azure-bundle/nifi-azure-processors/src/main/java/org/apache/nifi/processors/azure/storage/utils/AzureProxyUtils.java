
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
package org.apache.nifi.processors.azure.storage.utils;

import com.azure.core.http.HttpClient;
import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxySpec;
import org.apache.nifi.util.StringUtils;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.Collection;

public class AzureProxyUtils {
    private static final ProxySpec[] PROXY_SPECS = {ProxySpec.HTTP, ProxySpec.HTTP_AUTH, ProxySpec.SOCKS, ProxySpec.SOCKS_AUTH};

    private static ProxyOptions.Type getProxyOptionsTypeFromProxyType(final Proxy.Type proxyType) {
        for (final ProxyOptions.Type item : ProxyOptions.Type.values()) {
            if (item.toProxyType() == proxyType) {
                return item;
            }
        }
        return null;
    }

    public static final PropertyDescriptor PROXY_CONFIGURATION_SERVICE = ProxyConfiguration
            .createProxyConfigPropertyDescriptor(false, PROXY_SPECS);

    public static HttpClient createHttpClient(final PropertyContext propertyContext) {
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(propertyContext);
        final ProxyOptions proxyOptions = getProxyOptions(proxyConfig);

        final HttpClient client = new NettyAsyncHttpClientBuilder()
                .proxy(proxyOptions)
                .build();

        return client;
    }

    public static void validateProxySpec(final ValidationContext context, final Collection<ValidationResult> results) {
        final ProxyConfiguration proxyConfig = ProxyConfiguration.getConfiguration(context);

        final String proxyServerHost = proxyConfig.getProxyServerHost();
        final Integer proxyServerPort = proxyConfig.getProxyServerPort();
        final String proxyServerUser = proxyConfig.getProxyUserName();
        final String proxyServerPassword = proxyConfig.getProxyUserPassword();
        final Proxy.Type proxyServerType = proxyConfig.getProxyType();

        if (proxyServerType.equals(Proxy.Type.DIRECT) && (StringUtils.isNotBlank(proxyServerHost) || proxyServerPort != null
                || StringUtils.isNotBlank(proxyServerUser) || StringUtils.isNotBlank(proxyServerPassword))) {
            results.add(new ValidationResult.Builder().subject("Proxy Configuration").valid(false)
                    .explanation("When using direct mode, no proxy configuration should be provided.")
                    .build());
            return ;
        } else {
            if ((StringUtils.isNotBlank(proxyServerHost) && proxyServerPort == null)
                    || (StringUtils.isBlank(proxyServerHost) && proxyServerPort != null)) {
                results.add(new ValidationResult.Builder().subject("Proxy Configuration").valid(false)
                        .explanation("When specifying address information, both 'host' and 'port' information must be provided.")
                        .build());
                return ;
            }

            if ((StringUtils.isBlank(proxyServerUser) && StringUtils.isNotBlank(proxyServerPassword))
                    || (StringUtils.isNotBlank(proxyServerUser) && StringUtils.isBlank(proxyServerPassword))) {
                results.add(new ValidationResult.Builder().subject("Proxy Configuration").valid(false)
                        .explanation("When specifying credentials, both 'user' and 'password' must be provided.")
                        .build());
                return ;
            }
        }
        ProxyConfiguration.validateProxySpec(context, results, PROXY_SPECS);
    }

    public static ProxyOptions getProxyOptions(final ProxyConfiguration proxyConfig) {
        final String proxyServerHost = proxyConfig.getProxyServerHost();
        final Integer proxyServerPort = proxyConfig.getProxyServerPort();
        final String proxyServerUser = proxyConfig.getProxyUserName();
        final String proxyServerPassword = proxyConfig.getProxyUserPassword();

        final boolean proxyServerProvided = StringUtils.isNotBlank(proxyServerHost) && proxyServerPort != null;
        final boolean proxyCredentialsProvided = StringUtils.isNotBlank(proxyServerUser) && StringUtils.isNotBlank(proxyServerPassword);

        // if no endpoint is provided, return zero
        if (!proxyServerProvided) {
            return null;
        }

        // translate Proxy.Type to ProxyOptions.Type
        final ProxyOptions.Type proxyType = getProxyOptionsTypeFromProxyType(proxyConfig.getProxyType());
        final InetSocketAddress socketAddress = new InetSocketAddress(proxyServerHost, proxyServerPort);

        final ProxyOptions proxyOptions = new ProxyOptions(proxyType, socketAddress);

        if (proxyCredentialsProvided) {
            return proxyOptions.setCredentials(proxyServerUser, proxyServerPassword);
        }

        return proxyOptions;
    }
}
