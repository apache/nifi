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

package org.apache.nifi.atlassian.bitbucket;

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

@Tags({ "atlassian", "bitbucket", "registry", "flow" })
@CapabilityDescription("Flow Registry Client that uses the Bitbucket REST API to version control flows in a Bitbucket Repository.")
public class BitbucketFlowRegistryClient extends AbstractGitFlowRegistryClient {

    static final PropertyDescriptor FORM_FACTOR = new PropertyDescriptor.Builder()
            .name("Form Factor")
            .description("The Bitbucket deployment form factor")
            .allowableValues(BitbucketFormFactor.class)
            .defaultValue(BitbucketFormFactor.CLOUD.getValue())
            .required(true)
            .build();

    static final PropertyDescriptor BITBUCKET_API_URL = new PropertyDescriptor.Builder()
            .name("Bitbucket API Instance")
            .description("The Bitbucket API host or base URL (for example, api.bitbucket.org for Cloud or https://bitbucket.example.com for Data Center)")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .defaultValue("api.bitbucket.org")
            .required(true)
            .build();

    static final PropertyDescriptor WORKSPACE_NAME = new PropertyDescriptor.Builder()
            .name("Workspace Name")
            .description("The name of the workspace that contains the repository to connect to")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .dependsOn(FORM_FACTOR, BitbucketFormFactor.CLOUD)
            .build();

    static final PropertyDescriptor REPOSITORY_NAME = new PropertyDescriptor.Builder()
            .name("Repository Name")
            .description("The name of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor PROJECT_KEY = new PropertyDescriptor.Builder()
            .name("Project Key")
            .description("The key of the Bitbucket project that contains the repository (required for Data Center)")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .dependsOn(FORM_FACTOR, BitbucketFormFactor.DATA_CENTER)
            .build();

    static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("Authentication Type")
            .description("The type of authentication to use for accessing Bitbucket (Data Center supports only Access Token authentication)")
            .allowableValues(BitbucketAuthenticationType.class)
            .defaultValue(BitbucketAuthenticationType.ACCESS_TOKEN)
            .required(true)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .description("The access token to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, BitbucketAuthenticationType.ACCESS_TOKEN)
            .build();

    static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder()
            .name("Username")
            .description("The username to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(false)
            .dependsOn(AUTHENTICATION_TYPE, BitbucketAuthenticationType.BASIC_AUTH)
            .build();

    static final PropertyDescriptor APP_PASSWORD = new PropertyDescriptor.Builder()
            .name("App Password")
            .displayName("App Password or API Token")
            .description("The App Password or API Token to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, BitbucketAuthenticationType.BASIC_AUTH)
            .build();

    static final PropertyDescriptor OAUTH_TOKEN_PROVIDER = new PropertyDescriptor.Builder()
            .name("OAuth2 Access Token Provider")
            .description("Service providing OAuth2 Access Tokens for authentication")
            .identifiesControllerService(OAuth2AccessTokenProvider.class)
            .required(true)
            .dependsOn(AUTHENTICATION_TYPE, BitbucketAuthenticationType.OAUTH2)
            .build();

    static final PropertyDescriptor WEBCLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Web Client Service")
            .description("The Web Client Service to use for communicating with Bitbucket")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            WEBCLIENT_SERVICE,
            FORM_FACTOR,
            BITBUCKET_API_URL,
            WORKSPACE_NAME,
            PROJECT_KEY,
            REPOSITORY_NAME,
            AUTHENTICATION_TYPE,
            ACCESS_TOKEN,
            USERNAME,
            APP_PASSWORD,
            OAUTH_TOKEN_PROVIDER);

    static final String STORAGE_LOCATION_PREFIX = "git@bitbucket.org:";
    static final String STORAGE_LOCATION_FORMAT = STORAGE_LOCATION_PREFIX + "%s/%s.git";

    @Override
    protected List<PropertyDescriptor> createPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected GitRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException {
        final BitbucketFormFactor formFactor = context.getProperty(FORM_FACTOR).asAllowableValue(BitbucketFormFactor.class);

        return BitbucketRepositoryClient.builder()
                .clientId(getIdentifier())
                .logger(getLogger())
                .formFactor(formFactor)
                .apiUrl(context.getProperty(BITBUCKET_API_URL).getValue())
                .workspace(context.getProperty(WORKSPACE_NAME).getValue())
                .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                .projectKey(context.getProperty(PROJECT_KEY).getValue())
                .authenticationType(context.getProperty(AUTHENTICATION_TYPE).asAllowableValue(BitbucketAuthenticationType.class))
                .accessToken(context.getProperty(ACCESS_TOKEN).evaluateAttributeExpressions().getValue())
                .username(context.getProperty(USERNAME).evaluateAttributeExpressions().getValue())
                .appPassword(context.getProperty(APP_PASSWORD).evaluateAttributeExpressions().getValue())
                .oauthService(context.getProperty(OAUTH_TOKEN_PROVIDER).asControllerService(OAuth2AccessTokenProvider.class))
                .webClient(context.getProperty(WEBCLIENT_SERVICE).asControllerService(WebClientServiceProvider.class))
                .build();
    }

    @Override
    public boolean isStorageLocationApplicable(FlowRegistryClientConfigurationContext context, String location) {
        return location != null && location.startsWith(STORAGE_LOCATION_PREFIX);
    }

    @Override
    protected String getStorageLocation(GitRepositoryClient repositoryClient) {
        final BitbucketRepositoryClient bitbucketRepositoryClient = (BitbucketRepositoryClient) repositoryClient;

        if (bitbucketRepositoryClient.getFormFactor() == BitbucketFormFactor.DATA_CENTER) {
            final String apiHost = bitbucketRepositoryClient.getApiHost();
            final String projectKey = bitbucketRepositoryClient.getProjectKey();
            if (apiHost != null && projectKey != null) {
                return "git@" + apiHost + ":" + projectKey + "/" + bitbucketRepositoryClient.getRepoName() + ".git";
            }
            return bitbucketRepositoryClient.getRepoName();
        }

        return STORAGE_LOCATION_FORMAT.formatted(bitbucketRepositoryClient.getWorkspace(), bitbucketRepositoryClient.getRepoName());
    }
}
