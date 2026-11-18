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

package org.apache.nifi.gitea;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.registry.flow.FlowRegistryClientConfigurationContext;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.AbstractGitFlowRegistryClient;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Tags({ "git", "gitea", "forgejo", "codeberg", "registry", "flow" })
@CapabilityDescription("Flow Registry Client that uses the Gitea REST API to version control flows in a Gitea repository. "
        + "Compatible with Forgejo instances, including Codeberg, which provide the same REST API. "
        + "Note that for a given flow, the registry client will retrieve at most the last 50 commits to limit API calls.")
public class GiteaFlowRegistryClient extends AbstractGitFlowRegistryClient {

    static final PropertyDescriptor WEBCLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("Web Client Service")
            .description("The Web Client Service to use for communicating with Gitea, including TLS and proxy configuration")
            .required(true)
            .identifiesControllerService(WebClientServiceProvider.class)
            .build();

    static final PropertyDescriptor GITEA_API_URL = new PropertyDescriptor.Builder()
            .name("Gitea API URL")
            .description("The base URL of the Gitea or Forgejo instance, including the context path when applicable "
                    + "(for example, https://gitea.example.com, https://example.com/gitea or https://codeberg.org)")
            .addValidator(StandardValidators.URL_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_OWNER = new PropertyDescriptor.Builder()
            .name("Repository Owner")
            .description("The user or organization that owns the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor REPOSITORY_NAME = new PropertyDescriptor.Builder()
            .name("Repository Name")
            .description("The name of the repository")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .build();

    static final PropertyDescriptor ACCESS_TOKEN = new PropertyDescriptor.Builder()
            .name("Access Token")
            .description("The Access Token used for authentication. The token requires the read:repository scope for reading flows "
                    + "and the write:repository scope for committing flows.")
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .required(true)
            .sensitive(true)
            .build();

    static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            WEBCLIENT_SERVICE,
            GITEA_API_URL,
            REPOSITORY_OWNER,
            REPOSITORY_NAME,
            ACCESS_TOKEN
    );

    private static final Set<String> SUPPORTED_SCHEMES = Set.of("http", "https");
    private static final String GIT_SUFFIX = ".git";
    private static final String STORAGE_LOCATION_FORMAT = "%s/%s/%s";

    @Override
    protected List<PropertyDescriptor> createPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final String apiUrl = validationContext.getProperty(GITEA_API_URL).getValue();
        if (apiUrl != null && !isSupportedUrl(apiUrl)) {
            results.add(new ValidationResult.Builder()
                    .subject(GITEA_API_URL.getDisplayName())
                    .input(apiUrl)
                    .valid(false)
                    .explanation("URL must use the http or https scheme and include a host")
                    .build());
        }

        return results;
    }

    @Override
    protected GitRepositoryClient createRepositoryClient(final FlowRegistryClientConfigurationContext context) throws FlowRegistryException {
        return GiteaRepositoryClient.builder()
                .clientId(getIdentifier())
                .logger(getLogger())
                .apiUrl(context.getProperty(GITEA_API_URL).getValue())
                .repoOwner(context.getProperty(REPOSITORY_OWNER).getValue())
                .repoName(context.getProperty(REPOSITORY_NAME).getValue())
                .repoPath(context.getProperty(REPOSITORY_PATH).getValue())
                .accessToken(context.getProperty(ACCESS_TOKEN).getValue())
                .webClient(context.getProperty(WEBCLIENT_SERVICE).asControllerService(WebClientServiceProvider.class))
                .build();
    }

    @Override
    public boolean isStorageLocationApplicable(final FlowRegistryClientConfigurationContext context, final String location) {
        final String apiUrl = context.getProperty(GITEA_API_URL).getValue();
        final String repoOwner = context.getProperty(REPOSITORY_OWNER).getValue();
        final String repoName = context.getProperty(REPOSITORY_NAME).getValue();
        if (location == null || apiUrl == null || repoOwner == null || repoName == null) {
            return false;
        }

        final String storageLocation = getStorageLocation(apiUrl, repoOwner, repoName);
        String normalizedLocation = GiteaRepositoryClient.normalizeApiUrl(location);
        if (normalizedLocation.endsWith(GIT_SUFFIX)) {
            normalizedLocation = normalizedLocation.substring(0, normalizedLocation.length() - GIT_SUFFIX.length());
        }
        // Gitea owner and repository names are case-insensitive
        return storageLocation.equalsIgnoreCase(normalizedLocation);
    }

    @Override
    protected String getStorageLocation(final GitRepositoryClient repositoryClient) {
        final GiteaRepositoryClient giteaRepositoryClient = (GiteaRepositoryClient) repositoryClient;
        return getStorageLocation(giteaRepositoryClient.getApiUrl(), giteaRepositoryClient.getRepoOwner(), giteaRepositoryClient.getRepoName());
    }

    private static String getStorageLocation(final String apiUrl, final String repoOwner, final String repoName) {
        return STORAGE_LOCATION_FORMAT.formatted(GiteaRepositoryClient.normalizeApiUrl(apiUrl), repoOwner.trim(), repoName.trim());
    }

    static boolean isSupportedUrl(final String url) {
        try {
            final URI uri = URI.create(url.trim());
            return uri.getScheme() != null && SUPPORTED_SCHEMES.contains(uri.getScheme().toLowerCase()) && uri.getHost() != null;
        } catch (final IllegalArgumentException e) {
            return false;
        }
    }
}
