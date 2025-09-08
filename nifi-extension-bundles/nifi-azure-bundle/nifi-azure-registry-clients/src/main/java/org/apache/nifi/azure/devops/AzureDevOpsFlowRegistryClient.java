/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.azure.devops;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.AbstractGitFlowRegistryClient;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.util.List;

@Tags({ "git", "azure", "devops", "registry", "flow" })
@CapabilityDescription("Flow Registry Client that uses the Azure DevOps Git REST API to version control flows in a repository.")
public class AzureDevOpsFlowRegistryClient extends AbstractGitFlowRegistryClient {

    static final PropertyDescriptor AZURE_DEVOPS_API_URL = new PropertyDescriptor.Builder()
            .name("Azure DevOps API URL")
            .description("The base URL of the Azure DevOps instance")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .defaultValue("https://dev.azure.com")
            .required(true)
            .build();

    static final PropertyDescriptor ORGANIZATION = new PropertyDescriptor.Builder()
            .name("Organization")
            .description("The Azure DevOps organization")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor PROJECT = new PropertyDescriptor.Builder()
            .name("Project")
            .description("The Azure DevOps project")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_NAME = new PropertyDescriptor.Builder()
            .name("Repository Name")
            .description("The name of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor AUTHENTICATION_STRATEGY = new PropertyDescriptor.Builder()
            .name("Authentication Strategy")
            .description("Strategy for authenticating with Azure DevOps")
            .allowableValues(AzureDevOpsAuthenticationStrategy.class)
            .defaultValue(AzureDevOpsAuthenticationStrategy.SERVICE_PRINCIPAL.name())
            .required(true)
            .build();

    static final PropertyDescriptor OAUTH_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("OAuth2 Access Token Provider")
            .description("Controller service providing OAuth2 access tokens")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .dependsOn(AUTHENTICATION_STRATEGY, AzureDevOpsAuthenticationStrategy.SERVICE_PRINCIPAL.name())
            .build();

    static final PropertyDescriptor WEBCLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Web Client Service")
            .description("The Web Client Service to use for communicating with Bitbucket")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();


    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            AZURE_DEVOPS_API_URL,
            ORGANIZATION,
            PROJECT,
            REPOSITORY_NAME,
            AUTHENTICATION_STRATEGY,
            OAUTH_TOKEN_PROVIDER,
            WEBCLIENT_SERVICE);

    static final String STORAGE_LOCATION_PREFIX = "https://dev.azure.com/";
    static final String STORAGE_LOCATION_FORMAT = STORAGE_LOCATION_PREFIX + "%s/%s/_git/%s";

    @Override
    protected List<PropertyDescriptor> createPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected GitRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException {
        return AzureDevOpsRepositoryClient.builder()
                .clientId(getIdentifier())
                .apiUrl(context.getProperty(AZURE_DEVOPS_API_URL).getValue())
                .organization(context.getProperty(ORGANIZATION).getValue())
                .project(context.getProperty(PROJECT).getValue())
                .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                .oauthService(context.getProperty(OAUTH_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class))
                .webClient(context.getProperty(WEBCLIENT_SERVICE).asControllerService(WebClientServiceProvider.class))
                .logger(getLogger())
                .build();
    }

    @Override
    protected String getStorageLocation(final GitRepositoryClient repositoryClient) {
        final AzureDevOpsRepositoryClient client = (AzureDevOpsRepositoryClient) repositoryClient;
        return STORAGE_LOCATION_FORMAT.formatted(client.getOrganization(), client.getProject(), client.getRepoName());
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        return location != null && location.startsWith(STORAGE_LOCATION_PREFIX);
    }
}
