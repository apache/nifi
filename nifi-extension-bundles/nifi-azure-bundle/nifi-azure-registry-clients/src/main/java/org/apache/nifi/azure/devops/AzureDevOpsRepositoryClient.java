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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.MediaType;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
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

    private static final String API = "api-version";
    private static final String API_VERSION = "7.2-preview";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String BEARER = "Bearer ";

    // Azure DevOps Git security namespace identifier used for permissions API
    private static final String GIT_SECURITY_NAMESPACE = "2e9eb7ed-3c0a-47d4-87c1-0ffdd275fd87";
    // Security token format used for repository-scoped permission checks
    private static final String SECURITY_TOKEN_FORMAT = "repoV2/%s/%s";

    // Common string values
    private static final String EMPTY_STRING = "";
    private static final String FORWARD_SLASH = "/";

    // Path segments
    private static final String SEGMENT_APIS = "_apis";
    private static final String SEGMENT_PERMISSIONS = "permissions";
    private static final String SEGMENT_GIT = "git";
    private static final String SEGMENT_REPOSITORIES = "repositories";
    private static final String SEGMENT_ITEMS = "items";
    private static final String SEGMENT_REFS = "refs";
    private static final String SEGMENT_COMMITS = "commits";
    private static final String SEGMENT_PUSHES = "pushes";

    // Permission bit for Generic Contribute
    private static final String GENERIC_CONTRIBUTE_PERMISSION_BIT = "4";

    // Query parameter names
    private static final String PARAM_TOKENS = "tokens";
    private static final String PARAM_FILTER = "filter";
    private static final String PARAM_INCLUDE_CONTENT = "includeContent";
    private static final String PARAM_FORMAT = "format";
    private static final String PARAM_VERSION = "version";
    private static final String PARAM_VERSION_TYPE = "versionType";
    private static final String PARAM_PATH = "path";
    private static final String PARAM_SCOPE_PATH = "scopePath";
    private static final String PARAM_TOP = "$top";
    private static final String PARAM_SEARCH_ITEM_PATH = "searchCriteria.itemPath";
    private static final String PARAM_SEARCH_ITEM_VERSION = "searchCriteria.itemVersion.version";
    private static final String PARAM_SEARCH_ITEM_VERSION_TYPE = "searchCriteria.itemVersion.versionType";

    // Common parameter values
    private static final String INCLUDE_TRUE = "true";
    private static final String FORMAT_OCTET_STREAM = "octetStream";
    private static final String VERSION_TYPE_COMMIT = "commit";
    private static final String REFS_HEADS_PREFIX = "refs/heads/";
    private static final String FILTER_HEADS_PREFIX = "heads/";
    private static final String OBJECT_TYPE_TREE = "tree";
    private static final String OBJECT_TYPE_BLOB = "blob";
    private static final String JSON_FIELD_VALUE = "value";
    private static final String JSON_FIELD_PROJECT = "project";
    private static final String JSON_FIELD_ID = "id";
    private static final String JSON_FIELD_NAME = "name";
    private static final String JSON_FIELD_GIT_OBJECT_TYPE = "gitObjectType";
    private static final String JSON_FIELD_PATH = "path";
    private static final String JSON_FIELD_AUTHOR = "author";
    private static final String JSON_FIELD_COMMENT = "comment";
    private static final String JSON_FIELD_DATE = "date";
    private static final String JSON_FIELD_COMMIT_ID = "commitId";
    private static final String JSON_FIELD_OBJECT_ID = "objectId";

    // Change request constants
    private static final String CHANGE_TYPE_ADD = "add";
    private static final String CHANGE_TYPE_EDIT = "edit";
    private static final String CHANGE_TYPE_DELETE = "delete";
    private static final String CONTENT_TYPE_BASE64 = "base64encoded";

    // Common query parameter names and values
    private static final String VERSION_DESCRIPTOR_VERSION = "versionDescriptor.version";
    private static final String VERSION_DESCRIPTOR_VERSION_TYPE = "versionDescriptor.versionType";
    private static final String VERSION_TYPE_BRANCH = "branch";
    private static final String RECURSION_LEVEL = "recursionLevel";
    private static final String RECURSION_LEVEL_ONE_LEVEL = "OneLevel";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String organization;
    private final String project;
    private final String repoName;
    private final URL apiUrl;
    private final String repoPath;
    private final String clientId;
    private final OAuth2AccessTokenProvider tokenProvider;
    private WebClientServiceProvider webClient;
    private final ComponentLog logger;

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
        logger = Objects.requireNonNull(builder.logger, "ComponentLog required");

        // attempt to retrieve repository information to validate access
        final URI uri = getUriBuilder().build();
        final JsonNode response = executeGet(uri);
        final String projectId = response.get(JSON_FIELD_PROJECT).get(JSON_FIELD_ID).asText();
        final String repoId = response.get(JSON_FIELD_ID).asText();

        canRead = true;
        canWrite = hasWriteAccess(projectId, repoId);

        logger.info("Created {} for Flow Registry Client ID = [{}], repository [{}]", getClass().getSimpleName(), clientId, repoName);
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
        final String securityToken = String.format(SECURITY_TOKEN_FORMAT, projectId, repoId);

        final URI uri = baseUriBuilder()
                .addPathSegment(getOrganization())
                .addPathSegment(SEGMENT_APIS)
                .addPathSegment(SEGMENT_PERMISSIONS)
                .addPathSegment(GIT_SECURITY_NAMESPACE)
                .addPathSegment(GENERIC_CONTRIBUTE_PERMISSION_BIT) // 4 is the permission bit for "Generic Contribute" in Azure DevOps Git
                .addQueryParameter(PARAM_TOKENS, securityToken)
                .addQueryParameter(API, API_VERSION)
                .build();

        final JsonNode response = executeGet(uri);

        final JsonNode values = response.get(JSON_FIELD_VALUE);
        return values != null
                && values.isArray()
                && values.size() > 0
                && values.get(0).asBoolean();
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
        logger.debug("Getting branches for repo [{}]", repoName);
        final URI uri = getUriBuilder().addPathSegment(SEGMENT_REFS)
                .addQueryParameter(PARAM_FILTER, FILTER_HEADS_PREFIX)
                .addQueryParameter(API, API_VERSION)
                .build();
        final JsonNode response = executeGet(uri);
        final Set<String> result = new HashSet<>();
        for (JsonNode node : response.get(JSON_FIELD_VALUE)) {
            final String name = node.get(JSON_FIELD_NAME).asText();
            result.add(name.replace(REFS_HEADS_PREFIX, EMPTY_STRING));
        }
        return result;
    }

    @Override
    public Set<String> getTopLevelDirectoryNames(final String branch) throws FlowRegistryException {
        logger.debug("Getting top-level directories for repo [{}] on branch [{}]", repoName, branch);
        final URI uri = listingUrl("")
                .addQueryParameter(VERSION_DESCRIPTOR_VERSION, branch)
                .addQueryParameter(VERSION_DESCRIPTOR_VERSION_TYPE, VERSION_TYPE_BRANCH)
                .addQueryParameter(RECURSION_LEVEL, RECURSION_LEVEL_ONE_LEVEL)
                .build();
        final JsonNode response = executeGet(uri);
        final Set<String> result = new HashSet<>();
        for (JsonNode node : response.get(JSON_FIELD_VALUE)) {
            if (OBJECT_TYPE_TREE.equalsIgnoreCase(node.get(JSON_FIELD_GIT_OBJECT_TYPE).asText())) {
                final String path = node.get(JSON_FIELD_PATH).asText();
                // Azure DevOps returns the requested directory as part of the
                // listing results. Skip this entry so that the repository path
                // is not reported as a bucket when no directories are present
                if (!path.equals(getResolvedPath(EMPTY_STRING))) {
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
        logger.debug("Getting file names in directory [{}] for repo [{}] on branch [{}]", directory, repoName, branch);
        final URI uri = listingUrl(directory)
                .addQueryParameter(VERSION_DESCRIPTOR_VERSION, branch)
                .addQueryParameter(VERSION_DESCRIPTOR_VERSION_TYPE, VERSION_TYPE_BRANCH)
                .addQueryParameter(RECURSION_LEVEL, RECURSION_LEVEL_ONE_LEVEL)
                .build();
        final JsonNode response = executeGet(uri);
        final Set<String> result = new HashSet<>();
        for (JsonNode node : response.get(JSON_FIELD_VALUE)) {
            if (OBJECT_TYPE_BLOB.equalsIgnoreCase(node.get(JSON_FIELD_GIT_OBJECT_TYPE).asText())) {
                final String path = node.get(JSON_FIELD_PATH).asText();
                final String[] parts = path.split(FORWARD_SLASH);
                final String name = parts[parts.length - 1];
                result.add(name);
            }
        }
        return result;
    }

    @Override
    public List<GitCommit> getCommits(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting commits for [{}] from branch [{}] in repo [{}]", resolvedPath, branch, repoName);
        final URI uri = getUriBuilder().addPathSegment(SEGMENT_COMMITS)
                .addQueryParameter(PARAM_SEARCH_ITEM_PATH, resolvedPath)
                .addQueryParameter(PARAM_SEARCH_ITEM_VERSION, branch)
                .addQueryParameter(PARAM_SEARCH_ITEM_VERSION_TYPE, VERSION_TYPE_BRANCH)
                .addQueryParameter(API, API_VERSION)
                .build();
        final JsonNode response = executeGet(uri);
        final List<GitCommit> result = new ArrayList<>();
        for (JsonNode node : response.get(JSON_FIELD_VALUE)) {
            final String sha = node.get(JSON_FIELD_COMMIT_ID).asText();
            final String author = node.get(JSON_FIELD_AUTHOR).get(JSON_FIELD_NAME).asText();
            final String message = node.get(JSON_FIELD_COMMENT).asText();
            final Instant date = Instant.parse(node.get(JSON_FIELD_AUTHOR).get(JSON_FIELD_DATE).asText());
            result.add(new GitCommit(sha, author, message, date));
        }
        return result;
    }

    @Override
    public InputStream getContentFromBranch(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting content for [{}] from branch [{}] in repo [{}]", resolvedPath, branch, repoName);
        final URI uri = itemUrl(path)
                .addQueryParameter(VERSION_DESCRIPTOR_VERSION, branch)
                .addQueryParameter(VERSION_DESCRIPTOR_VERSION_TYPE, VERSION_TYPE_BRANCH)
                .addQueryParameter(PARAM_INCLUDE_CONTENT, INCLUDE_TRUE)
                .addQueryParameter(PARAM_FORMAT, FORMAT_OCTET_STREAM)
                .build();
        return executeGetStream(uri);
    }

    @Override
    public InputStream getContentFromCommit(final String path, final String commitSha) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting content for [{}] from commit [{}] in repo [{}]", resolvedPath, commitSha, repoName);
        final URI uri = itemUrl(path)
                .addQueryParameter(PARAM_VERSION, commitSha)
                .addQueryParameter(PARAM_VERSION_TYPE, VERSION_TYPE_COMMIT)
                .addQueryParameter(PARAM_INCLUDE_CONTENT, INCLUDE_TRUE)
                .addQueryParameter(PARAM_FORMAT, FORMAT_OCTET_STREAM)
                .build();
        return executeGetStream(uri);
    }

    @Override
    public Optional<String> getContentSha(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting latest commit SHA affecting [{}] on branch [{}] in repo [{}]", resolvedPath, branch, repoName);
        final URI uri = getUriBuilder().addPathSegment(SEGMENT_COMMITS)
                .addQueryParameter(PARAM_SEARCH_ITEM_PATH, resolvedPath)
                .addQueryParameter(PARAM_SEARCH_ITEM_VERSION, branch)
                .addQueryParameter(PARAM_SEARCH_ITEM_VERSION_TYPE, VERSION_TYPE_BRANCH)
                .addQueryParameter(PARAM_TOP, "1")
                .addQueryParameter(API, API_VERSION)
                .build();

        final JsonNode response = executeGetAllowingNotFound(uri);
        if (response == null) {
            logger.debug("Unable to get content SHA for [{}] from branch [{}] because content does not exist", resolvedPath, branch);
            return Optional.empty();
        }

        final JsonNode values = response.get(JSON_FIELD_VALUE);
        if (values != null && values.size() > 0) {
            return Optional.ofNullable(values.get(0).get(JSON_FIELD_COMMIT_ID)).map(JsonNode::asText);
        } else {
            logger.debug("Unable to get content SHA for [{}] from branch [{}] because no commits were found", resolvedPath, branch);
            return Optional.empty();
        }
    }

    @Override
    public String createContent(final GitCreateContentRequest request) throws FlowRegistryException {
        final String path = getResolvedPath(request.getPath());
        final String branch = request.getBranch();
        final String message = request.getMessage();
        logger.debug("Creating content at path [{}] on branch [{}] in repo [{}]", path, branch, repoName);

        // Use expectedCommitSha for atomic commit if provided, otherwise fetch current branch HEAD
        // Azure DevOps will reject the push if oldObjectId doesn't match the current branch HEAD
        final String oldObjectId;
        final String expectedCommitSha = request.getExpectedCommitSha();
        if (expectedCommitSha != null && !expectedCommitSha.isBlank()) {
            oldObjectId = expectedCommitSha;
        } else {
            // Fall back to fetching current branch commit id
            final URI refUri = getUriBuilder().addPathSegment(SEGMENT_REFS)
                    .addQueryParameter(PARAM_FILTER, FILTER_HEADS_PREFIX + branch)
                    .addQueryParameter(API, API_VERSION)
                    .build();
            final JsonNode refResponse = executeGet(refUri);
            oldObjectId = refResponse.get(JSON_FIELD_VALUE).get(0).get(JSON_FIELD_OBJECT_ID).asText();
        }

        final URI pushUri = getUriBuilder().addPathSegment(SEGMENT_PUSHES)
                .addQueryParameter(API, API_VERSION)
                .build();

        final String changeType;
        if (request.getExistingContentSha() == null) {
            final Optional<String> existingSha = getContentSha(request.getPath(), branch);
            changeType = existingSha.isPresent() ? CHANGE_TYPE_EDIT : CHANGE_TYPE_ADD;
        } else {
            changeType = CHANGE_TYPE_EDIT;
        }

        final String encoded = Base64.getEncoder().encodeToString(request.getContent().getBytes(StandardCharsets.UTF_8));

        final PushRequest pushRequest = new PushRequest(
                List.of(new RefUpdate(REFS_HEADS_PREFIX + branch, oldObjectId)),
                List.of(new Commit(message,
                        List.of(new Change(changeType, new Item(path), new NewContent(encoded, CONTENT_TYPE_BASE64)))))
        );

        final String json;
        try {
            json = MAPPER.writeValueAsString(pushRequest);
        } catch (final Exception e) {
            throw new FlowRegistryException("Failed to serialize push request", e);
        }

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
            return pushResponse.get(SEGMENT_COMMITS).get(0).get(JSON_FIELD_COMMIT_ID).asText();
        } catch (IOException e) {
            throw new FlowRegistryException("Failed to create content", e);
        }
    }

    @Override
    public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException, IOException {
        final String path = getResolvedPath(filePath);
        logger.debug("Deleting file [{}] in repo [{}] on branch [{}]", path, repoName, branch);
        final URI refUri = getUriBuilder().addPathSegment(SEGMENT_REFS)
                .addQueryParameter(PARAM_FILTER, FILTER_HEADS_PREFIX + branch)
                .addQueryParameter(API, API_VERSION)
                .build();
        final JsonNode refResponse = executeGet(refUri);
        final String oldObjectId = refResponse.get(JSON_FIELD_VALUE).get(0).get(JSON_FIELD_OBJECT_ID).asText();

        final URI pushUri = getUriBuilder().addPathSegment(SEGMENT_PUSHES)
                .addQueryParameter(API, API_VERSION)
                .build();

        final PushRequest pushRequest = new PushRequest(
                List.of(new RefUpdate(REFS_HEADS_PREFIX + branch, oldObjectId)),
                List.of(new Commit(commitMessage,
                        List.of(new Change(CHANGE_TYPE_DELETE, new Item(path), null))))
        );

        final String json = MAPPER.writeValueAsString(pushRequest);

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

    // Request body types for Azure DevOps Push API
    private record PushRequest(List<RefUpdate> refUpdates, List<Commit> commits) { }

    private record RefUpdate(String name, String oldObjectId) { }

    private record Commit(String comment, List<Change> changes) { }

    private record Item(String path) { }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    private record Change(String changeType, Item item, NewContent newContent) { }

    private record NewContent(String content, String contentType) { }

    /**
     * Create URI builder for accessing the repository.
     *
     * @return HTTP URL Builder configured for repository access
     */
    private HttpUriBuilder getUriBuilder() {
        return baseUriBuilder()
                .addPathSegment(getOrganization())
                .addPathSegment(project)
                .addPathSegment(SEGMENT_APIS)
                .addPathSegment(SEGMENT_GIT)
                .addPathSegment(SEGMENT_REPOSITORIES)
                .addPathSegment(repoName);
    }

    /**
     * Create base URI builder from configured API URL supporting values with or without scheme
     */
    private HttpUriBuilder baseUriBuilder() {
        final String scheme = apiUrl.getProtocol() == null ? "https" : apiUrl.getProtocol();
        final String hostValue = apiUrl.getHost();
        final int port = apiUrl.getPort();

        return this.webClient.getHttpUriBuilder()
                .scheme(scheme)
                .host(hostValue)
                .port(port);
    }

    /**
     * Create URI builder for accessing a single item using the {@code path}
     * parameter.
     *
     * @param path Path of item to retrieve relative to repository
     * @return HTTP URL Builder configured for item retrieval
     */
    private HttpUriBuilder itemUrl(final String path) {
        final HttpUriBuilder builder = getUriBuilder().addPathSegment(SEGMENT_ITEMS);
        builder.addQueryParameter(API, API_VERSION);
        if (path != null) {
            builder.addQueryParameter(PARAM_PATH, getResolvedPath(path));
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
        final HttpUriBuilder builder = getUriBuilder().addPathSegment(SEGMENT_ITEMS);
        builder.addQueryParameter(API, API_VERSION);
        if (directory != null) {
            builder.addQueryParameter(PARAM_SCOPE_PATH, getResolvedPath(directory));
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
        final String prefix = normalizeSegment(repoPath);
        final String normalizedPath = normalizeSegment(path);

        if (prefix.isEmpty()) {
            return normalizedPath.isEmpty() ? FORWARD_SLASH : normalizedPath;
        }

        if (normalizedPath.isEmpty()) {
            return prefix;
        }

        if (normalizedPath.equals(prefix) || normalizedPath.startsWith(prefix + FORWARD_SLASH)) {
            return normalizedPath;
        }

        return prefix + normalizedPath;
    }

    private static String normalizeSegment(final String segment) {
        if (segment == null || segment.isBlank()) {
            return EMPTY_STRING;
        }
        return segment.startsWith(FORWARD_SLASH) ? segment : FORWARD_SLASH + segment;
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
        final String[] parts = path.split(FORWARD_SLASH);
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
        private URL apiUrl;
        private String organization;
        private String project;
        private String repoName;
        private String repoPath;
        private OAuth2AccessTokenProvider oauthService;
        private WebClientServiceProvider webClient;
        private ComponentLog logger;

        public Builder clientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder apiUrl(final String apiUrl) {
            try {
                this.apiUrl = new URI(apiUrl).toURL();
            } catch (final MalformedURLException | URISyntaxException e) {
                throw new IllegalArgumentException("API URL not valid: " + apiUrl, e);
            }
            return this;
        }

        public Builder apiUrl(final URL apiUrl) {
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

        public Builder logger(final ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public AzureDevOpsRepositoryClient build() throws FlowRegistryException {
            return new AzureDevOpsRepositoryClient(this);
        }
    }
}
