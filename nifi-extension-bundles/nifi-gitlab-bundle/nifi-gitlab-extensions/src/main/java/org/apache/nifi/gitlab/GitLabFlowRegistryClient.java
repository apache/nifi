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

package org.apache.nifi.gitlab;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.AbstractGitFlowRegistryClient;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.gitlab4j.api.GitLabApi;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Tags({"git", "gitlab", "registry", "flow"})
@CapabilityDescription("Flow Registry Client that uses the GitLab REST API to version control flows in a GitLab Project.")
public class GitLabFlowRegistryClient extends AbstractGitFlowRegistryClient {

    static final PropertyDescriptor GITLAB_API_URL = new PropertyDescriptor.Builder()
            .name("GitLab API URL")
            .description("The URL of the GitLab API")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .defaultValue("https://gitlab.com/")
            .required(true)
            .build();

    static final PropertyDescriptor GITLAB_API_VERSION = new PropertyDescriptor.Builder()
            .name("GitLab API Version")
            .description("The version of the GitLab API")
            .allowableValues(GitLabApi.ApiVersion.class)
            .defaultValue(GitLabApi.ApiVersion.V4.name())
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_NAMESPACE = new PropertyDescriptor.Builder()
            .name("Repository Namespace")
            .description("The namespace of the repository. Typically the name of a group that owns the repository.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_NAME = new PropertyDescriptor.Builder()
            .name("Repository Name")
            .description("The name of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("Authentication Type")
            .description("The type of authentication to use for accessing GitLab")
            .allowableValues(GitLabAuthenticationType.class)
            .defaultValue(GitLabAuthenticationType.ACCESS_TOKEN)
            .required(true)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .description("The access token to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, GitLabAuthenticationType.ACCESS_TOKEN)
            .build();

    static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .defaultValue("The timeout for connecting to the API")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("10 seconds")
            .build();

    static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Read Timeout")
            .defaultValue("The timeout for reading from the API")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .required(true)
            .defaultValue("10 seconds")
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GITLAB_API_URL,
            GITLAB_API_VERSION,
            REPOSITORY_NAMESPACE,
            REPOSITORY_NAME,
            AUTHENTICATION_TYPE,
            ACCESS_TOKEN,
            CONNECT_TIMEOUT,
            READ_TIMEOUT
    );

    static final String STORAGE_LOCATION_PREFIX = "git@gitlab.com:";
    static final String STORAGE_LOCATION_FORMAT = STORAGE_LOCATION_PREFIX + "%s/%s.git";

    @Override
    protected List<PropertyDescriptor> createPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected String getStorageLocation(final GitRepositoryClient repositoryClient) {
        final GitLabRepositoryClient gitLabRepositoryClient = (GitLabRepositoryClient) repositoryClient;
        return STORAGE_LOCATION_FORMAT.formatted(gitLabRepositoryClient.getRepoNamespace(), gitLabRepositoryClient.getRepoName());
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        return location != null && location.startsWith(STORAGE_LOCATION_PREFIX);
    }

    @Override
    protected GitRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException {
        return GitLabRepositoryClient.builder()
                .clientId(getIdentifier())
                .apiUrl(context.getProperty(GITLAB_API_URL).getValue())
                .apiVersion(context.getProperty(GITLAB_API_VERSION).asAllowableValue(GitLabApi.ApiVersion.class))
                .repoNamespace(context.getProperty(REPOSITORY_NAMESPACE).getValue())
                .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                .authenticationType(context.getProperty(AUTHENTICATION_TYPE).asAllowableValue(GitLabAuthenticationType.class))
                .authToken(context.getProperty(ACCESS_TOKEN).evaluateAttributeExpressions().getValue())
                .connectTimeout(context.getProperty(CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue())
                .readTimeout(context.getProperty(READ_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue())
                .build();
    }
}
