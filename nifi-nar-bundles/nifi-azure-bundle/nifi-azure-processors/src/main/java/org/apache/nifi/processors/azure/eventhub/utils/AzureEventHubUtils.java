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

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

public final class AzureEventHubUtils {

    public static final String MANANGED_IDENDITY_POLICY = ConnectionStringBuilder.MANAGED_IDENTITY_AUTHENTICATION;

    public static final PropertyDescriptor POLICY_PRIMARY_KEY = new PropertyDescriptor.Builder()
        .name("Shared Access Policy Primary Key")
        .description("The primary key of the shared access policy")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .sensitive(true)
        .required(false)
        .build();

    public static final PropertyDescriptor USE_MANANGED_IDENTITY = new PropertyDescriptor.Builder()
        .name("use-managed-identity")
        .displayName("Use Azure Managed Identity")
        .description("Choose whether or not to use the managed identity of azure vm/vmss. ")
        .required(false).defaultValue("false").allowableValues("true", "false")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static List<ValidationResult> customValidate(PropertyDescriptor accessPolicyDescriptor, ValidationContext context) {
        List<ValidationResult> retVal = new ArrayList<>();

        boolean accessPolicyIsSet  = context.getProperty(accessPolicyDescriptor).isSet();
        boolean policyKeyIsSet     = context.getProperty(POLICY_PRIMARY_KEY).isSet();
        boolean useManagedIdentity = context.getProperty(USE_MANANGED_IDENTITY).asBoolean();

        if (useManagedIdentity && (accessPolicyIsSet || policyKeyIsSet) ) {
            final String msg = String.format(
                "%s and %s with %s fields cannot be set at the same time.",
                USE_MANANGED_IDENTITY.getDisplayName(),
                accessPolicyDescriptor.getDisplayName(),
                POLICY_PRIMARY_KEY.getDisplayName()
            );
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        } else if (!useManagedIdentity && (!accessPolicyIsSet || !policyKeyIsSet)) {
            final String msg = String.format(
                "Either %s or %s with %s must be set",
                USE_MANANGED_IDENTITY.getDisplayName(),
                accessPolicyDescriptor.getDisplayName(),
                POLICY_PRIMARY_KEY.getDisplayName()
            );
            retVal.add(new ValidationResult.Builder().valid(false).explanation(msg).build());
        }
        return retVal;
    }

    public static String getManagedIdentityConnectionString(final String namespace, final String eventHubName){
        return new ConnectionStringBuilder().setNamespaceName(namespace).setEventHubName(eventHubName)
                    .setAuthentication(MANANGED_IDENDITY_POLICY).toString();
    }
}
