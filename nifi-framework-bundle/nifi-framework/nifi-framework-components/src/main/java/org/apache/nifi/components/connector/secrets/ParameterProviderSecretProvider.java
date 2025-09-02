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

package org.apache.nifi.components.connector.secrets;

import org.apache.nifi.components.connector.Secret;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterGroup;

import java.util.ArrayList;
import java.util.List;

public class ParameterProviderSecretProvider implements SecretProvider {
    private final ParameterProviderNode parameterProvider;

    public ParameterProviderSecretProvider(final ParameterProviderNode parameterProvider) {
        this.parameterProvider = parameterProvider;
    }

    @Override
    public String getProviderId() {
        return parameterProvider.getIdentifier();
    }

    @Override
    public String getProviderName() {
        return parameterProvider.getName();
    }

    @Override
    public List<Secret> getAllSecrets() {
        final List<Secret> secrets = new ArrayList<>();

        final List<ParameterGroup> parameterGroups = parameterProvider.fetchParameterValues();
        for (final ParameterGroup group : parameterGroups) {
            for (final Parameter parameter : group.getParameters()) {
                final Secret secret = createSecret(group.getGroupName(), parameter);
                secrets.add(secret);
            }
        }

        return secrets;
    }

    private Secret createSecret(final String groupName, final Parameter parameter) {
        final ParameterDescriptor descriptor = parameter.getDescriptor();

        return new StandardSecret.Builder()
            .providerId(getProviderId())
            .providerName(getProviderName())
            .groupName(groupName)
            .name(descriptor.getName())
            .fullyQualifiedName(getProviderName() + "." + groupName + "." + descriptor.getName())
            .description(descriptor.getDescription())
            .value(parameter.getValue())
            .authorizable(parameterProvider)
            .build();
    }

    @Override
    public List<Secret> getSecrets(final List<String> fullyQualifiedSecretNames) {
        final List<ParameterGroup> parameterGroups = parameterProvider.fetchParameterValues(fullyQualifiedSecretNames);
        final List<Secret> secrets = new ArrayList<>();
        for (final ParameterGroup group : parameterGroups) {
            for (final Parameter parameter : group.getParameters()) {
                final Secret secret = createSecret(group.getGroupName(), parameter);
                secrets.add(secret);
            }
        }

        return secrets;
    }
}
