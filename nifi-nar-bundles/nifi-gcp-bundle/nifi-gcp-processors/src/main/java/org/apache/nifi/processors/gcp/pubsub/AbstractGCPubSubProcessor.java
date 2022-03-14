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

import com.google.api.core.ApiFunction;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ServiceOptions;

import com.google.cloud.pubsub.v1.TopicAdminSettings;
import io.grpc.HttpConnectProxiedSocketAddress;
import io.grpc.ManagedChannelBuilder;
import io.grpc.ProxiedSocketAddress;
import io.grpc.ProxyDetector;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.VerifiableProcessor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.processors.gcp.ProxyAwareTransportFactory;
import org.apache.nifi.proxy.ProxyConfiguration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class AbstractGCPubSubProcessor extends AbstractGCPProcessor implements VerifiableProcessor {

    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("gcp-pubsub-publish-batch-size")
            .displayName("Batch Size")
            .description("Indicates the number of messages the cloud service should bundle together in a batch. If not set and left empty, only one message " +
                    "will be used in a batch")
            .required(true)
            .defaultValue("15")
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("FlowFiles are routed to this relationship after a successful Google Cloud Pub/Sub operation.")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("FlowFiles are routed to this relationship if the Google Cloud Pub/Sub operation fails.")
            .build();

    private static final Set<Relationship> relationships = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return Collections.unmodifiableList(Arrays.asList(
                PROJECT_ID,
                GCP_CREDENTIALS_PROVIDER_SERVICE,
                PROXY_HOST,
                PROXY_PORT,
                HTTP_PROXY_USERNAME,
                HTTP_PROXY_PASSWORD,
                ProxyConfiguration.createProxyConfigPropertyDescriptor(true, ProxyAwareTransportFactory.PROXY_SPECS))
        );
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    protected ServiceOptions getServiceOptions(ProcessContext context, GoogleCredentials credentials) {
        return null;
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final Collection<ValidationResult> results = new HashSet<>(super.customValidate(validationContext));

        final boolean projectId = validationContext.getProperty(PROJECT_ID).isSet();
        if (!projectId) {
            results.add(new ValidationResult.Builder()
                    .subject(PROJECT_ID.getName())
                    .valid(false)
                    .explanation("The Project ID must be set for this processor.")
                    .build());
        }

        return results;
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
                .setChannelConfigurator(new ApiFunction<ManagedChannelBuilder, ManagedChannelBuilder>() {
                    @Override
                    public ManagedChannelBuilder apply(ManagedChannelBuilder managedChannelBuilder) {
                        return managedChannelBuilder.proxyDetector(
                                new ProxyDetector() {
                                    @Nullable
                                    @Override
                                    public ProxiedSocketAddress proxyFor(SocketAddress socketAddress)
                                            throws IOException {
                                        if (Proxy.Type.HTTP.equals(proxyConfiguration.getProxyType()) && proxyConfiguration.hasCredential()) {
                                            return HttpConnectProxiedSocketAddress.newBuilder()
                                                    .setUsername(proxyConfiguration.getProxyUserName())
                                                    .setPassword(proxyConfiguration.getProxyUserPassword())
                                                    .setProxyAddress(new InetSocketAddress(proxyConfiguration.getProxyServerHost(),
                                                            proxyConfiguration.getProxyServerPort()))
                                                    .setTargetAddress((InetSocketAddress) socketAddress)
                                                    .build();
                                        }
                                        else {
                                            return null;
                                        }
                                    }
                                });
                    }
                })
                .build();
    }
}