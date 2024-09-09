/*
 *
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
 *
 */
package org.apache.nifi.github;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.AbstractGitFlowRegistryClient;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;

import java.io.IOException;
import java.util.List;

/**
 * Implementation of {@link org.apache.nifi.registry.flow.FlowRegistryClient} that uses GitHub for version controlling flows.
 */
@Tags({"git", "github", "registry", "flow"})
@CapabilityDescription("Flow Registry Client that uses the GitHub REST API to version control flows in a GitHub repository.")
public class GitHubFlowRegistryClient extends AbstractGitFlowRegistryClient {

    static final PropertyDescriptor GITHUB_API_URL = new PropertyDescriptor.Builder()
            .name("GitHub API URL")
            .description("The URL of the GitHub API")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .defaultValue("https://api.github.com/")
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_NAME = new PropertyDescriptor.Builder()
            .name("Repository Name")
            .description("The name of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_OWNER = new PropertyDescriptor.Builder()
            .name("Repository Owner")
            .description("The owner of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor AUTHENTICATION_TYPE = new PropertyDescriptor.Builder()
            .name("Authentication Type")
            .description("The type of authentication to use for accessing GitHub")
            .allowableValues(GitHubAuthenticationType.class)
            .defaultValue(GitHubAuthenticationType.NONE.name())
            .required(true)
            .build();

    static final PropertyDescriptor PERSONAL_ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Personal Access Token")
            .description("The personal access token to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, GitHubAuthenticationType.PERSONAL_ACCESS_TOKEN.name())
            .build();

    static final PropertyDescriptor APP_ID = new PropertyDescriptor.Builder()
            .name("App ID")
            .description("Identifier of GitHub App to use for authentication")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(false)
            .dependsOn(AUTHENTICATION_TYPE, GitHubAuthenticationType.APP_INSTALLATION.name())
            .build();

    static final PropertyDescriptor APP_PRIVATE_KEY = new PropertyDescriptor.Builder()
            .name("App Private Key")
            .description("RSA private key associated with GitHub App to use for authentication.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .dependsOn(AUTHENTICATION_TYPE, GitHubAuthenticationType.APP_INSTALLATION.name())
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            GITHUB_API_URL,
            REPOSITORY_OWNER,
            REPOSITORY_NAME,
            AUTHENTICATION_TYPE,
            PERSONAL_ACCESS_TOKEN,
            APP_ID,
            APP_PRIVATE_KEY
    );

    static final String STORAGE_LOCATION_PREFIX = "git@github.com:";
    static final String STORAGE_LOCATION_FORMAT = STORAGE_LOCATION_PREFIX + "%s/%s.git";

    @Override
    protected List<PropertyDescriptor> createPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected GitHubRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws IOException, FlowRegistryException {
        return GitHubRepositoryClient.builder()
                .apiUrl(context.getProperty(GITHUB_API_URL).getValue())
                .authenticationType(GitHubAuthenticationType.valueOf(context.getProperty(AUTHENTICATION_TYPE).getValue()))
                .personalAccessToken(context.getProperty(PERSONAL_ACCESS_TOKEN).getValue())
                .appId(context.getProperty(APP_ID).getValue())
                .appPrivateKey(context.getProperty(APP_PRIVATE_KEY).getValue())
                .repoOwner(context.getProperty(REPOSITORY_OWNER).getValue())
                .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                .build();
    }

    @Override
    protected String getStorageLocation(final GitRepositoryClient repositoryClient) {
        final GitHubRepositoryClient gitHubRepositoryClient = (GitHubRepositoryClient) repositoryClient;
        return STORAGE_LOCATION_FORMAT.formatted(gitHubRepositoryClient.getRepoOwner(), gitHubRepositoryClient.getRepoName());
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        return location != null && location.startsWith(STORAGE_LOCATION_PREFIX);
    }
}
