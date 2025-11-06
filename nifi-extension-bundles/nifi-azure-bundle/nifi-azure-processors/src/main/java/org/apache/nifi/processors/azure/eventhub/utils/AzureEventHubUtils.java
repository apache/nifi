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
package org.apache.nifi.processors.azure.eventhub.utils;

import com.azure.core.amqp.ProxyAuthenticationType;
import com.azure.core.amqp.ProxyOptions;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubAuthenticationStrategy;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubComponent;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;

import java.net.InetSocketAddress;
import java.net.Proxy;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class AzureEventHubUtils {

    public static final AllowableValue AZURE_ENDPOINT = new AllowableValue(".servicebus.windows.net", "Azure", "Servicebus endpoint for general use");
    public static final AllowableValue AZURE_CHINA_ENDPOINT = new AllowableValue(".servicebus.chinacloudapi.cn", "Azure China", "Servicebus endpoint for China");
    public static final AllowableValue AZURE_GERMANY_ENDPOINT = new AllowableValue(".servicebus.cloudapi.de", "Azure Germany", "Servicebus endpoint for Germany");
    public static final AllowableValue AZURE_US_GOV_ENDPOINT = new AllowableValue(".servicebus.usgovcloudapi.net", "Azure US Government", "Servicebus endpoint for US Government");
    public static final String OLD_POLICY_PRIMARY_KEY_DESCRIPTOR_NAME = "Shared Access Policy Primary Key";
    public static final String OLD_USE_MANAGED_IDENTITY_DESCRIPTOR_NAME = "use-managed-identity";
    public static final String USE_MANAGED_IDENTITY_PROPERTY_NAME = "Use Azure Managed Identity";

    public static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
            .name("Shared Access Policy Key")
            .description("The key of the shared access policy. Either the primary or the secondary key can be used.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .sensitive(true)
            .required(false)
            .dependsOn(AzureEventHubComponent.AUTHENTICATION_STRATEGY, AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE)
            .build();

    public static final PropertyDescriptor SERVICE_BUS_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Service Bus Endpoint")
            .description("To support namespaces not in the default windows.net domain.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(AZURE_ENDPOINT, AZURE_CHINA_ENDPOINT, AZURE_GERMANY_ENDPOINT, AZURE_US_GOV_ENDPOINT)
            .defaultValue(AZURE_ENDPOINT)
            .required(true)
            .build();

    public static List<ValidationResult> customValidate(PropertyDescriptor accessPolicyDescriptor,
                                                        PropertyDescriptor policyKeyDescriptor,
                                                        ValidationContext context) {
        List<ValidationResult> validationResults = new ArrayList<>();

        boolean accessPolicyIsSet = context.getProperty(accessPolicyDescriptor).isSet();
        boolean policyKeyIsSet = context.getProperty(policyKeyDescriptor).isSet();
        final AzureEventHubAuthenticationStrategy authenticationStrategy = Optional.ofNullable(
                context.getProperty(AzureEventHubComponent.AUTHENTICATION_STRATEGY)
                        .asAllowableValue(AzureEventHubAuthenticationStrategy.class))
                .orElse(AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY);

        if (authenticationStrategy == AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY && (accessPolicyIsSet || policyKeyIsSet)) {
            final String msg = String.format(
                    "When '%s' is set to '%s', '%s' and '%s' must not be set.",
                    AzureEventHubComponent.AUTHENTICATION_STRATEGY.getDisplayName(),
                    AzureEventHubAuthenticationStrategy.MANAGED_IDENTITY.getDisplayName(),
                    accessPolicyDescriptor.getDisplayName(),
                    POLICY_PRIMARY_KEY.getDisplayName()
            );
            validationResults.add(new ValidationResult.Builder().subject("Credentials config").valid(false).explanation(msg).build());
        } else if (authenticationStrategy == AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE && (!accessPolicyIsSet || !policyKeyIsSet)) {
            final String msg = String.format(
                    "When '%s' is set to '%s', both '%s' and '%s' must be set",
                    AzureEventHubComponent.AUTHENTICATION_STRATEGY.getDisplayName(),
                    AzureEventHubAuthenticationStrategy.SHARED_ACCESS_SIGNATURE.getDisplayName(),
                    accessPolicyDescriptor.getDisplayName(),
                    POLICY_PRIMARY_KEY.getDisplayName()
            );
            validationResults.add(new ValidationResult.Builder().subject("Credentials config").valid(false).explanation(msg).build());
        }
        ProxyConfiguration.validateProxySpec(context, validationResults, AzureEventHubComponent.PROXY_SPECS);
        return validationResults;
    }

    public static Map<String, String> getApplicationProperties(final Map<String, Object> eventProperties) {
        final Map<String, String> properties = new HashMap<>();

        if (eventProperties != null) {
            for (Map.Entry<String, Object> property : eventProperties.entrySet()) {
                properties.put(String.format("eventhub.property.%s", property.getKey()), property.getValue().toString());
            }
        }

        return properties;
    }

    /**
     * Creates the {@link ProxyOptions proxy options}.
     *
     * @param propertyContext to supply Proxy configurations
     * @return {@link ProxyOptions proxy options}, null if Proxy is not set
     */
    public static Optional<ProxyOptions> getProxyOptions(final PropertyContext propertyContext) {
        final AzureEventHubTransportType transportType =
                propertyContext.getProperty(AzureEventHubComponent.TRANSPORT_TYPE).asAllowableValue(AzureEventHubTransportType.class);

        final ProxyConfiguration proxyConfiguration = switch (transportType) {
            case AMQP -> ProxyConfiguration.DIRECT_CONFIGURATION;
            case AMQP_WEB_SOCKETS -> ProxyConfiguration.getConfiguration(propertyContext);
        };

        final ProxyOptions proxyOptions;
        if (proxyConfiguration != ProxyConfiguration.DIRECT_CONFIGURATION) {
            final Proxy proxy = getProxy(proxyConfiguration);
            if (proxyConfiguration.hasCredential()) {
                proxyOptions = new ProxyOptions(
                        ProxyAuthenticationType.BASIC,
                        proxy,
                        proxyConfiguration.getProxyUserName(), proxyConfiguration.getProxyUserPassword());
            } else {
                proxyOptions = new ProxyOptions(
                        ProxyAuthenticationType.NONE,
                        proxy, null, null);
            }
        } else {
            proxyOptions = null;
        }

        return Optional.ofNullable(proxyOptions);
    }

    private static Proxy getProxy(ProxyConfiguration proxyConfiguration) {
        final Proxy.Type type;
        if (proxyConfiguration.getProxyType() == Proxy.Type.HTTP) {
            type = Proxy.Type.HTTP;
        } else {
            throw new IllegalArgumentException("Unsupported proxy type: " + proxyConfiguration.getProxyType());
        }
        return new Proxy(type, new InetSocketAddress(proxyConfiguration.getProxyServerHost(), proxyConfiguration.getProxyServerPort()));
    }
}
