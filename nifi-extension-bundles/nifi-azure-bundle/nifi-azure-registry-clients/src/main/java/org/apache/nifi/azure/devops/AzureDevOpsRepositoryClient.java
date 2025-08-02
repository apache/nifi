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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.MediaType;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Git Repository Client implementation for Azure DevOps using the REST API.
 */
public class AzureDevOpsRepositoryClient implements GitRepositoryClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(AzureDevOpsRepositoryClient.class);

    private static final String API = "api-version";
    private static final String API_VERSION = "7.2-preview";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String BEARER = "Bearer ";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String organization;
    private final String project;
    private final String repoName;
    private final String apiUrl;
    private final String repoPath;
    private final String clientId;
    private final OAuth2AccessTokenProvider tokenProvider;
    private WebClientServiceProvider webClient;

    private final boolean canRead;
    private final boolean canWrite;

    /**
     * Constructs an AzureDevOpsRepositoryClient with the provided configuration.
     *
     * @param builder The builder containing the configuration for the client.
     * @throws FlowRegistryException if any required parameters are missing or if
     *                               access validation fails.
     */
    private AzureDevOpsRepositoryClient(final Builder builder) throws FlowRegistryException {
        apiUrl = Objects.requireNonNull(builder.apiUrl, "API URL required");
        organization = Objects.requireNonNull(builder.organization, "Organization required");
        project = Objects.requireNonNull(builder.project, "Project required");
        repoName = Objects.requireNonNull(builder.repoName, "Repository Name required");
        repoPath = builder.repoPath;
        tokenProvider = Objects.requireNonNull(builder.oauthService, "OAuth2 Access Token Provider required");

        clientId = Objects.requireNonNull(builder.clientId, "Client ID is required");
        webClient = Objects.requireNonNull(builder.webClient, "Web Client is required");

        // attempt to retrieve repository information to validate access
        final URI uri = getUriBuilder().build();
        final JsonNode response = executeGet(uri);
        final String projectId = response.get("project").get("id").asText();
        final String repoId = response.get("id").asText();

        canRead = true;
        canWrite = hasWriteAccess(projectId, repoId);

        LOGGER.info("Created {} for clientId = [{}], repository [{}]", getClass().getSimpleName(), clientId, repoName);
    }

    /**
     * Checks if the user has write access to the repository.
     *
     * @param projectId The ID of the project containing the repository.
     * @param repoId    The ID of the repository.
     * @return true if the user has write access, false otherwise.
     * @throws FlowRegistryException if an error occurs while checking access.
     */
    private boolean hasWriteAccess(final String projectId, final String repoId) throws FlowRegistryException {
        final String securityNamespace = "2e9eb7ed-3c0a-47d4-87c1-0ffdd275fd87"; // Azure DevOps Git security namespace
        final String securityToken = String.format("repoV2/%s/%s", projectId, repoId);

        final URI uri = this.webClient.getHttpUriBuilder()
                .scheme("https")
                .host(apiUrl)
                .addPathSegment(getOrganization())
                .addPathSegment("_apis")
                .addPathSegment("permissions")
                .addPathSegment(securityNamespace)
                .addPathSegment("4") // 4 is the permission bit for "Generic Contribute" in Azure DevOps Git
                .addQueryParameter("tokens", securityToken)
                .addQueryParameter(API, API_VERSION)
                .build();

        final JsonNode response = executeGet(uri);

        return response.has("value")
                && response.get("value").isArray()
                && response.get("value").size() > 0
                && response.get("value").get(0).asBoolean();
    }

    @Override
    public boolean hasReadPermission() {
        return canRead;
    }

    @Override
    public boolean hasWritePermission() {
        return canWrite;
    }

    @Override
    public Set<String> getBranches() throws FlowRegistryException {
        final URI uri = getUriBuilder().addPathSegment("refs")
                .addQueryParameter("filter", "heads/")
                .addQueryParameter(API, API_VERSION)
                .build();
        final JsonNode response = executeGet(uri);
        final Set<String> result = new HashSet<>();
        for (JsonNode node : response.get("value")) {
            final String name = node.get("name").asText();
            result.add(name.replace("refs/heads/", ""));
        }
        return result;
    }

    @Override
    public Set<String> getTopLevelDirectoryNames(final String branch) throws FlowRegistryException {
        final URI uri = listingUrl("")
                .addQueryParameter("versionDescriptor.version", branch)
                .addQueryParameter("versionDescriptor.versionType", "branch")
                .addQueryParameter("recursionLevel", "OneLevel")
                .build();
        final JsonNode response = executeGet(uri);
        final Set<String> result = new HashSet<>();
        for (JsonNode node : response.get("value")) {
            if ("tree".equalsIgnoreCase(node.get("gitObjectType").asText())) {
                final String path = node.get("path").asText();
                // Azure DevOps returns the requested directory as part of the
                // listing results. Skip this entry so that the repository path
                // is not reported as a bucket when no directories are present
                if (!path.equals(getResolvedPath(""))) {
                    final String name = getNameFromPath(path);
                    if (name != null) {
                        result.add(name);
                    }
                }
            }
        }
        return result;
    }

    @Override
    public Set<String> getFileNames(final String directory, final String branch) throws FlowRegistryException {
        final URI uri = listingUrl(directory)
                .addQueryParameter("versionDescriptor.version", branch)
                .addQueryParameter("versionDescriptor.versionType", "branch")
                .addQueryParameter("recursionLevel", "OneLevel")
                .build();
        final JsonNode response = executeGet(uri);
        final Set<String> result = new HashSet<>();
        for (JsonNode node : response.get("value")) {
            if ("blob".equalsIgnoreCase(node.get("gitObjectType").asText())) {
                final String path = node.get("path").asText();
                final String[] parts = path.split("/");
                final String name = parts[parts.length - 1];
                result.add(name);
            }
        }
        return result;
    }

    @Override
    public List<GitCommit> getCommits(final String path, final String branch) throws FlowRegistryException {
        final URI uri = getUriBuilder().addPathSegment("commits")
                .addQueryParameter("searchCriteria.itemPath", getResolvedPath(path))
                .addQueryParameter("searchCriteria.itemVersion.version", branch)
                .addQueryParameter("searchCriteria.itemVersion.versionType", "branch")
                .addQueryParameter(API, API_VERSION)
                .build();
        final JsonNode response = executeGet(uri);
        final List<GitCommit> result = new ArrayList<>();
        for (JsonNode node : response.get("value")) {
            final String sha = node.get("commitId").asText();
            final String author = node.get("author").get("name").asText();
            final String message = node.get("comment").asText();
            final Instant date = Instant.parse(node.get("author").get("date").asText());
            result.add(new GitCommit(sha, author, message, date));
        }
        return result;
    }

    @Override
    public InputStream getContentFromBranch(final String path, final String branch) throws FlowRegistryException {
        final URI uri = itemUrl(path)
                .addQueryParameter("versionDescriptor.version", branch)
                .addQueryParameter("versionDescriptor.versionType", "branch")
                .addQueryParameter("includeContent", "true")
                .addQueryParameter("format", "octetStream")
                .build();
        return executeGetStream(uri);
    }

    @Override
    public InputStream getContentFromCommit(final String path, final String commitSha) throws FlowRegistryException {
        final URI uri = itemUrl(path)
                .addQueryParameter("version", commitSha)
                .addQueryParameter("versionType", "commit")
                .addQueryParameter("includeContent", "true")
                .addQueryParameter("format", "octetStream")
                .build();
        return executeGetStream(uri);
    }

    @Override
    public Optional<String> getContentSha(final String path, final String branch) throws FlowRegistryException {
        final URI uri = getUriBuilder().addPathSegment("commits")
                .addQueryParameter("searchCriteria.itemPath", getResolvedPath(path))
                .addQueryParameter("searchCriteria.itemVersion.version", branch)
                .addQueryParameter("searchCriteria.itemVersion.versionType", "branch")
                .addQueryParameter("$top", "1")
                .addQueryParameter(API, API_VERSION)
                .build();

        final JsonNode response = executeGetAllowingNotFound(uri);
        if (response == null) {
            return Optional.empty();
        }

        final JsonNode values = response.get("value");
        if (values != null && values.size() > 0) {
            return Optional.ofNullable(values.get(0).get("commitId")).map(JsonNode::asText);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public String createContent(final GitCreateContentRequest request) throws FlowRegistryException {
        final String path = getResolvedPath(request.getPath());
        final String branch = request.getBranch();
        final String message = request.getMessage();
        // Get branch current commit id
        final URI refUri = getUriBuilder().addPathSegment("refs")
                .addQueryParameter("filter", "heads/" + branch)
                .addQueryParameter(API, API_VERSION)
                .build();
        final JsonNode refResponse = executeGet(refUri);
        final String oldObjectId = refResponse.get("value").get(0).get("objectId").asText();

        final URI pushUri = getUriBuilder().addPathSegment("pushes")
                .addQueryParameter(API, API_VERSION)
                .build();

        final String changeType;
        if (request.getExistingContentSha() == null) {
            final Optional<String> existingSha = getContentSha(request.getPath(), branch);
            changeType = existingSha.isPresent() ? "edit" : "add";
        } else {
            changeType = "edit";
        }

        final String encoded = Base64.getEncoder().encodeToString(request.getContent().getBytes(StandardCharsets.UTF_8));
        final String json = String.format("{\"refUpdates\":[{\"name\":\"refs/heads/%s\",\"oldObjectId\":\"%s\"}],"
                + "\"commits\":[{\"comment\":\"%s\",\"changes\":[{\"changeType\":\"%s\",\"item\":{\"path\":\"%s\"},"
                + "\"newContent\":{\"content\":\"%s\",\"contentType\":\"base64encoded\"}}]}]}",
                branch, oldObjectId, message, changeType, path, encoded);

        final HttpResponseEntity response = this.webClient.getWebClientService()
                .post()
                .uri(pushUri)
                .header(AUTHORIZATION_HEADER, bearerToken())
                .header(CONTENT_TYPE_HEADER, MediaType.APPLICATION_JSON.getMediaType())
                .body(json)
                .retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_CREATED) {
            throw new FlowRegistryException("Request to %s failed - %s".formatted(pushUri, getErrorMessage(response)));
        }

        try {
            final JsonNode pushResponse = MAPPER.readTree(response.body());
            return pushResponse.get("commits").get(0).get("commitId").asText();
        } catch (IOException e) {
            throw new FlowRegistryException("Failed to create content", e);
        }
    }

    @Override
    public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException, IOException {
        final String path = getResolvedPath(filePath);
        final URI refUri = getUriBuilder().addPathSegment("refs")
                .addQueryParameter("filter", "heads/" + branch)
                .addQueryParameter(API, API_VERSION)
                .build();
        final JsonNode refResponse = executeGet(refUri);
        final String oldObjectId = refResponse.get("value").get(0).get("objectId").asText();

        final URI pushUri = getUriBuilder().addPathSegment("pushes")
                .addQueryParameter(API, API_VERSION)
                .build();

        final String json = String.format("{\"refUpdates\":[{\"name\":\"refs/heads/%s\",\"oldObjectId\":\"%s\"}],"
                + "\"commits\":[{\"comment\":\"%s\",\"changes\":[{\"changeType\":\"delete\",\"item\":{\"path\":\"%s\"}}]}]}",
                branch, oldObjectId, commitMessage, path);

        final HttpResponseEntity response = this.webClient.getWebClientService()
                .post()
                .uri(pushUri)
                .header(AUTHORIZATION_HEADER, bearerToken())
                .header(CONTENT_TYPE_HEADER, MediaType.APPLICATION_JSON.getMediaType())
                .body(json)
                .retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_CREATED) {
            throw new FlowRegistryException("Request to %s failed - %s".formatted(pushUri, getErrorMessage(response)));
        }

        return response.body();
    }

    /**
     * Constructs an error message from the HTTP response entity.
     *
     * @param responseEntity The HTTP response entity containing the error details.
     * @return A formatted error message.
     * @throws FlowRegistryException if the error response cannot be parsed.
     */
    private String getErrorMessage(HttpResponseEntity responseEntity) throws FlowRegistryException {
        try {
            final Object response = MAPPER.readValue(responseEntity.body(), Object.class);
            return "[HTTP " + responseEntity.statusCode() + "] - " + response.toString();
        } catch (Exception e) {
            throw new FlowRegistryException("Failed to parse error response", e);
        }
    }

    /**
     * Constructs the Bearer token for authentication.
     *
     * @return The Bearer token string.
     */
    private String bearerToken() {
        return BEARER + tokenProvider.getAccessDetails().getAccessToken();
    }

    /**
     * Create URI builder for accessing the repository.
     *
     * @return HTTP URL Builder configured for repository access
     */
    private HttpUriBuilder getUriBuilder() {
        return this.webClient.getHttpUriBuilder()
                .scheme("https")
                .host(apiUrl)
                .addPathSegment(getOrganization())
                .addPathSegment(project)
                .addPathSegment("_apis")
                .addPathSegment("git")
                .addPathSegment("repositories")
                .addPathSegment(repoName);
    }

    /**
     * Create URI builder for accessing a single item using the {@code path}
     * parameter.
     *
     * @param path Path of item to retrieve relative to repository
     * @return HTTP URL Builder configured for item retrieval
     */
    private HttpUriBuilder itemUrl(final String path) {
        final HttpUriBuilder builder = getUriBuilder().addPathSegment("items");
        builder.addQueryParameter(API, API_VERSION);
        if (path != null) {
            builder.addQueryParameter("path", getResolvedPath(path));
        }
        return builder;
    }

    /**
     * Create URL builder for listing items under the provided directory using the
     * {@code scopePath} parameter.
     *
     * @param directory Directory to list relative to repository
     * @return HTTP URL Builder configured for listing items
     */
    private HttpUriBuilder listingUrl(final String directory) {
        final HttpUriBuilder builder = getUriBuilder().addPathSegment("items");
        builder.addQueryParameter(API, API_VERSION);
        if (directory != null) {
            builder.addQueryParameter("scopePath", getResolvedPath(directory));
        }
        return builder;
    }

    /**
     * Resolves the given path against the repository path, ensuring that it is
     * properly formatted.
     *
     * @param path The path to resolve.
     * @return The resolved path, ensuring it starts with a slash and does not
     *         duplicate slashes.
     */
    private String getResolvedPath(final String path) {
        final String prefix = (repoPath == null || repoPath.isBlank())
                ? ""
                : (repoPath.startsWith("/") ? repoPath : "/" + repoPath);

        final String normalizedPath = (path == null || path.isBlank())
                ? ""
                : (path.startsWith("/") ? path : "/" + path);

        if (prefix.isEmpty()) {
            return normalizedPath.isEmpty() ? "/" : normalizedPath;
        }

        if (normalizedPath.isEmpty()) {
            return prefix;
        }

        if (normalizedPath.equals(prefix) || normalizedPath.startsWith(prefix + "/")) {
            return normalizedPath;
        }

        return prefix + normalizedPath;
    }

    /**
     * Extracts the name from a given path.
     *
     * @param path The path from which to extract the name.
     * @return The name extracted from the path, or null if the path is null or
     *         empty.
     */
    private String getNameFromPath(final String path) {
        if (path == null) {
            return null;
        }
        final String[] parts = path.split("/");
        if (parts.length == 0) {
            return null;
        }
        return parts[parts.length - 1];
    }

    /**
     * Executes a GET request to the specified URI and returns the response as a
     * JsonNode.
     *
     * @param uri The URI to send the GET request to.
     * @return The response body as a JsonNode.
     * @throws FlowRegistryException if the request fails or if the response cannot
     *                               be parsed.
     */
    private JsonNode executeGet(final URI uri) throws FlowRegistryException {
        final HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, bearerToken()).retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException("Request to %s failed - %s".formatted(uri, getErrorMessage(response)));
        }

        try {
            return MAPPER.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Request to %s failed".formatted(uri), e);
        }
    }

    /**
     * Executes a GET request to the specified URI, allowing for a 404 Not Found
     * response.
     *
     * @param uri The URI to send the GET request to.
     * @return The response body as a JsonNode, or null if the response is 404 Not
     *         Found.
     * @throws FlowRegistryException if the request fails with an error other than
     *                               404 Not Found.
     */
    private JsonNode executeGetAllowingNotFound(final URI uri) throws FlowRegistryException {
        final HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, bearerToken()).retrieve();

        if (response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
            return null;
        } else if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException("Request to %s failed - %s".formatted(uri, getErrorMessage(response)));
        }

        try {
            return MAPPER.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Request to %s failed".formatted(uri), e);
        }
    }

    /**
     * Executes a GET request to the specified URI and returns the response body as
     * an InputStream.
     *
     * @param uri The URI to send the GET request to.
     * @return The response body as an InputStream.
     * @throws FlowRegistryException if the request fails or if the response cannot
     *                               be read.
     */
    private InputStream executeGetStream(final URI uri) throws FlowRegistryException {
        final HttpResponseEntity response = this.webClient.getWebClientService().get().uri(uri).header(AUTHORIZATION_HEADER, bearerToken()).retrieve();

        if (response.statusCode() != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException("Request to %s failed - %s".formatted(uri, getErrorMessage(response)));
        }

        return response.body();
    }

    public static Builder builder() {
        return new Builder();
    }

    protected String getOrganization() {
        return organization;
    }

    protected String getProject() {
        return project;
    }

    protected String getRepoName() {
        return repoName;
    }

    public static class Builder {
        private String clientId;
        private String apiUrl = "https://dev.azure.com";
        private String organization;
        private String project;
        private String repoName;
        private String repoPath;
        private OAuth2AccessTokenProvider oauthService;
        private WebClientServiceProvider webClient;

        public Builder clientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder apiUrl(final String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public Builder organization(final String organization) {
            this.organization = organization;
            return this;
        }

        public Builder project(final String project) {
            this.project = project;
            return this;
        }

        public Builder repoName(final String repoName) {
            this.repoName = repoName;
            return this;
        }

        public Builder repoPath(final String repoPath) {
            this.repoPath = repoPath;
            return this;
        }

        public Builder oauthService(final OAuth2AccessTokenProvider oauthService) {
            this.oauthService = oauthService;
            return this;
        }

        public Builder webClient(final WebClientServiceProvider webClient) {
            this.webClient = webClient;
            return this;
        }

        public AzureDevOpsRepositoryClient build() throws FlowRegistryException {
            return new AzureDevOpsRepositoryClient(this);
        }
    }
}
