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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;

import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public final class AzureEventHubUtils {

    public static final String MANAGED_IDENTITY_POLICY = ConnectionStringBuilder.MANAGED_IDENTITY_AUTHENTICATION;
    public static final AllowableValue AZURE_ENDPOINT = new AllowableValue(".servicebus.windows.net","Azure", "Servicebus endpoint for general use");
    public static final AllowableValue AZURE_CHINA_ENDPOINT = new AllowableValue(".servicebus.chinacloudapi.cn", "Azure China", "Servicebus endpoint for China");
    public static final AllowableValue AZURE_GERMANY_ENDPOINT = new AllowableValue(".servicebus.cloudapi.de", "Azure Germany", "Servicebus endpoint for Germany");
    public static final AllowableValue AZURE_US_GOV_ENDPOINT = new AllowableValue(".servicebus.usgovcloudapi.net", "Azure US Government", "Servicebus endpoint for US Government");

    public static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
        .name("Shared Access Policy Primary Key")
        .description("The primary key of the shared access policy")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .sensitive(true)
        .required(false)
        .build();

    public static final PropertyDescriptor USE_MANAGED_IDENTITY = new PropertyDescriptor.Builder()
        .name("use-managed-identity")
        .displayName("Use Azure Managed Identity")
        .description("Choose whether or not to use the managed identity of Azure VM/VMSS")
        .required(false).defaultValue("false").allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor SERVICE_BUS_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Service Bus Endpoint")
            .description("To support namespaces not in the default windows.net domain.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(AzureEventHubUtils.AZURE_ENDPOINT, AzureEventHubUtils.AZURE_CHINA_ENDPOINT,
                    AzureEventHubUtils.AZURE_GERMANY_ENDPOINT, AzureEventHubUtils.AZURE_US_GOV_ENDPOINT)
            .defaultValue(AzureEventHubUtils.AZURE_ENDPOINT.getValue())
            .required(true)
            .build();

    public static List<ValidationResult> customValidate(PropertyDescriptor accessPolicyDescriptor,
        PropertyDescriptor policyKeyDescriptor,
        ValidationContext context) {
        List<ValidationResult> retVal = new ArrayList<>();

        boolean accessPolicyIsSet  = context.getProperty(accessPolicyDescriptor).isSet();
        boolean policyKeyIsSet     = context.getProperty(policyKeyDescriptor).isSet();
        boolean useManagedIdentity = context.getProperty(USE_MANAGED_IDENTITY).asBoolean();

        if (useManagedIdentity && (accessPolicyIsSet || policyKeyIsSet) ) {
            final String msg = String.format(
                "('%s') and ('%s' with '%s') fields cannot be set at the same time.",
                USE_MANAGED_IDENTITY.getDisplayName(),
                accessPolicyDescriptor.getDisplayName(),
                POLICY_PRIMARY_KEY.getDisplayName()
            );
            retVal.add(new ValidationResult.Builder().subject("Credentials config").valid(false).explanation(msg).build());
        } else if (!useManagedIdentity && (!accessPolicyIsSet || !policyKeyIsSet)) {
            final String msg = String.format(
                "either('%s') or (%s with '%s') must be set",
                USE_MANAGED_IDENTITY.getDisplayName(),
                accessPolicyDescriptor.getDisplayName(),
                POLICY_PRIMARY_KEY.getDisplayName()
            );
            retVal.add(new ValidationResult.Builder().subject("Credentials config").valid(false).explanation(msg).build());
        }
        return retVal;
    }

    public static String getManagedIdentityConnectionString(final String namespace, final String domainName, final String eventHubName){
        return new ConnectionStringBuilder()
                    .setEndpoint(namespace, removeStartingDotFrom(domainName))
                    .setEventHubName(eventHubName)
                    .setAuthentication(MANAGED_IDENTITY_POLICY).toString();
    }
    public static String getSharedAccessSignatureConnectionString(final String namespace, final String domainName, final String eventHubName, final String sasName, final String sasKey) {
        return new ConnectionStringBuilder()
                    .setEndpoint(namespace, removeStartingDotFrom(domainName))
                    .setEventHubName(eventHubName)
                    .setSasKeyName(sasName)
                    .setSasKey(sasKey).toString();
    }

    public static Map<String, String> getApplicationProperties(EventData eventData) {
        final Map<String, String> properties = new HashMap<>();

        final Map<String,Object> applicationProperties = eventData.getProperties();
        if (null != applicationProperties) {
            for (Map.Entry<String, Object> property : applicationProperties.entrySet()) {
                properties.put(String.format("eventhub.property.%s", property.getKey()), property.getValue().toString());
            }
        }

        return properties;
    }

    private static String removeStartingDotFrom(final String domainName) {
        return domainName.replaceFirst("^\\.", "");
    }

}
