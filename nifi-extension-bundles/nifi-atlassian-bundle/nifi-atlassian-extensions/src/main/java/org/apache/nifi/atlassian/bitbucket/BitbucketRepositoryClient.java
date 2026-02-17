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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.StandardHttpContentType;
import org.apache.nifi.web.client.api.StandardMultipartFormDataStreamBuilder;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Implementation of {@link GitRepositoryClient} for Bitbucket.
 */
public class BitbucketRepositoryClient implements GitRepositoryClient {

    private final ComponentLog logger;

    public static final String CLOUD_API_VERSION = "2.0";
    public static final String DATA_CENTER_API_VERSION = "1.0";

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String BASIC = "Basic";
    private static final String BEARER = "Bearer";
    private static final String FIELD_BRANCH = "branch";
    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_SOURCE_COMMIT_ID = "sourceCommitId";
    private static final String FIELD_PARENTS = "parents";
    private static final String FIELD_CONTENT = "content";
    private static final String FIELD_FILES = "files";
    private static final String FIELD_CHILDREN = "children";
    private static final String FIELD_VALUES = "values";
    private static final String FIELD_NEXT = "next";
    private static final String FIELD_NEXT_PAGE_START = "nextPageStart";
    private static final String FIELD_IS_LAST_PAGE = "isLastPage";
    private static final String FIELD_PERMISSION = "permission";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_PATH = "path";
    private static final String FIELD_TO_STRING = "toString";
    private static final String FIELD_DISPLAY_NAME = "displayName";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_EMAIL_ADDRESS = "emailAddress";
    private static final String FIELD_DISPLAY_ID = "displayId";
    private static final String FIELD_ID = "id";
    private static final String FIELD_HASH = "hash";
    private static final String FIELD_MESSAGE_TEXT = "message";
    private static final String FIELD_AUTHOR = "author";
    private static final String FIELD_AUTHOR_TIMESTAMP = "authorTimestamp";
    private static final String FIELD_ERROR_MESSAGE = "error";
    private static final String FIELD_ERRORS = "errors";
    private static final String FIELD_RAW = "raw";
    private static final String EMPTY_STRING = "";
    private static final String ENTRY_DIRECTORY_DATA_CENTER = "DIRECTORY";
    private static final String ENTRY_DIRECTORY_CLOUD = "commit_directory";
    private static final String ENTRY_FILE_DATA_CENTER = "FILE";
    private static final String ENTRY_FILE_CLOUD = "commit_file";

    private final ObjectMapper objectMapper = JsonMapper.builder().build();

    private final String apiUrl;
    private final String apiVersion;
    private final BitbucketFormFactor formFactor;
    private final String projectKey;
    private final String apiScheme;
    private final String apiHost;
    private final int apiPort;
    private final List<String> apiBasePathSegments;
    private final String clientId;
    private final String workspace;
    private final String repoName;
    private final String repoPath;
    private WebClientServiceProvider webClient;
    private BitbucketToken<String> authToken;

    private final boolean canRead;
    private final boolean canWrite;
    private boolean hasCommits = false;

    public static String getDefaultApiVersion(final BitbucketFormFactor formFactor) {
        return formFactor == BitbucketFormFactor.DATA_CENTER ? DATA_CENTER_API_VERSION : CLOUD_API_VERSION;
    }

    private BitbucketRepositoryClient(final Builder builder) throws FlowRegistryException {
        webClient = Objects.requireNonNull(builder.webClient, "Web Client is required");
        logger = Objects.requireNonNull(builder.logger, "ComponentLog required");

        formFactor = builder.formFactor == null ? BitbucketFormFactor.CLOUD : builder.formFactor;

        apiUrl = Objects.requireNonNull(builder.apiUrl, "API Instance is required");

        final ParsedApiUrl parsedApiUrl = parseApiUrl(apiUrl);
        apiScheme = parsedApiUrl.scheme();
        apiHost = parsedApiUrl.host();
        apiPort = parsedApiUrl.port();
        apiBasePathSegments = parsedApiUrl.pathSegments();

        if (formFactor == BitbucketFormFactor.CLOUD) {
            workspace = Objects.requireNonNull(builder.workspace, "Workspace is required for Bitbucket Cloud");
            projectKey = null;
        } else {
            projectKey = Objects.requireNonNull(builder.projectKey, "Project Key is required for Bitbucket Data Center");
            workspace = builder.workspace;
        }

        repoName = Objects.requireNonNull(builder.repoName, "Repository Name is required");

        clientId = Objects.requireNonNull(builder.clientId, "Client ID is required");
        repoPath = builder.repoPath;

        apiVersion = getDefaultApiVersion(formFactor);

        final BitbucketAuthenticationType authenticationType = Objects.requireNonNull(builder.authenticationType, "Authentication type is required");

        if (formFactor == BitbucketFormFactor.DATA_CENTER && authenticationType != BitbucketAuthenticationType.ACCESS_TOKEN) {
            throw new FlowRegistryException("Bitbucket Data Center only supports Access Token authentication");
        }

        switch (authenticationType) {
            case ACCESS_TOKEN -> {
                Objects.requireNonNull(builder.accessToken, "Access Token is required");
                authToken = new AccessToken(builder.accessToken);
            }
            case BASIC_AUTH -> {
                Objects.requireNonNull(builder.username, "Username is required");
                Objects.requireNonNull(builder.appPassword, "App Password is required");
                authToken = new BasicAuthToken(builder.username, builder.appPassword);
            }
            case OAUTH2 -> {
                Objects.requireNonNull(builder.oauthService, "OAuth 2.0 Token Provider is required");
                authToken = new OAuthToken(builder.oauthService);
            }
        }

        final String permission = checkRepoPermissions(authenticationType);

        switch (permission) {
            case "admin", "write" -> {
                canRead = true;
                canWrite = true;
            }
            case "read" -> {
                canRead = true;
                canWrite = false;
            }
            case "none" -> {
                canRead = false;
                canWrite = false;
            }
            default -> {
                canRead = false;
                canWrite = false;
            }
        }

        logger.info("Created {} for clientId = [{}], repository [{}]", getClass().getSimpleName(), clientId, repoName);
    }

    @Override
    public boolean hasReadPermission() {
        return canRead;
    }

    @Override
    public boolean hasWritePermission() {
        return canWrite;
    }

    /**
     * @return the name of the workspace
     */
    public String getWorkspace() {
        return workspace;
    }

    public String getProjectKey() {
        return projectKey;
    }

    /**
     * @return the name of the repository
     */
    public String getRepoName() {
        return repoName;
    }

    public BitbucketFormFactor getFormFactor() {
        return formFactor;
    }

    public String getApiHost() {
        return apiHost;
    }

    @Override
    public Set<String> getBranches() throws FlowRegistryException {
        logger.debug("Getting branches for repository [{}]", repoName);
        return formFactor == BitbucketFormFactor.DATA_CENTER
                ? getBranchesDataCenter()
                : getBranchesCloud();
    }

    private Set<String> getBranchesCloud() throws FlowRegistryException {
        final URI uri = getRepositoryUriBuilder().addPathSegment("refs").addPathSegment("branches").build();
        try (final HttpResponseEntity response = this.webClient.getWebClientService()
                .get()
                .uri(uri)
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .retrieve()) {

            verifyStatusCode(response, "Error while listing branches for repository [%s]".formatted(repoName), HttpURLConnection.HTTP_OK);

            final JsonNode jsonResponse = parseResponseBody(response, uri);
            final JsonNode values = jsonResponse.get(FIELD_VALUES);
            final Set<String> result = new HashSet<>();
            if (values != null && values.isArray()) {
                for (JsonNode branch : values) {
                    final String branchName = branch.path(FIELD_NAME).asText(EMPTY_STRING);
                    if (!branchName.isEmpty()) {
                        result.add(branchName);
                    }
                }
            }
            return result;
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed closing Bitbucket branch listing response", e);
        }
    }

    private Set<String> getBranchesDataCenter() throws FlowRegistryException {
        final URI uri = getRepositoryUriBuilder().addPathSegment("branches").build();
        try (final HttpResponseEntity response = this.webClient.getWebClientService()
                .get()
                .uri(uri)
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .retrieve()) {

            verifyStatusCode(response, "Error while listing branches for repository [%s]".formatted(repoName), HttpURLConnection.HTTP_OK);

            final JsonNode jsonResponse = parseResponseBody(response, uri);
            final JsonNode values = jsonResponse.get(FIELD_VALUES);
            final Set<String> result = new HashSet<>();
            if (values != null && values.isArray()) {
                for (JsonNode branch : values) {
                    final String displayId = branch.path(FIELD_DISPLAY_ID).asText(EMPTY_STRING);
                    if (!displayId.isEmpty()) {
                        result.add(displayId);
                    }
                }
            }
            return result;
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed closing Bitbucket branch listing response", e);
        }
    }

    @Override
    public Set<String> getTopLevelDirectoryNames(final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath("");
        logger.debug("Getting top-level directories for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);

        final Iterator<JsonNode> files = getFiles(branch, resolvedPath);

        final Set<String> result = new HashSet<>();
        while (files.hasNext()) {
            JsonNode file = files.next();
            if (isDirectoryEntry(file)) {
                final String entryPath = getEntryPath(file);
                if (!entryPath.isEmpty()) {
                    final Path fullPath = Paths.get(entryPath);
                    result.add(fullPath.getFileName().toString());
                }
            }
        }

        return result;
    }

    @Override
    public Set<String> getFileNames(final String directory, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(directory);
        logger.debug("Getting filenames for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);

        final Iterator<JsonNode> files = getFiles(branch, resolvedPath);

        final Set<String> result = new HashSet<>();
        while (files.hasNext()) {
            JsonNode file = files.next();
            if (isFileEntry(file)) {
                final String entryPath = getEntryPath(file);
                if (!entryPath.isEmpty()) {
                    final Path fullPath = Paths.get(entryPath);
                    result.add(fullPath.getFileName().toString());
                }
            }
        }

        return result;
    }

    @Override
    public List<GitCommit> getCommits(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting commits for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);

        Iterator<JsonNode> commits = getListCommits(branch, resolvedPath);

        final List<GitCommit> result = new ArrayList<>();
        while (commits.hasNext()) {
            JsonNode commit = commits.next();
            result.add(toGitCommit(commit));
        }

        return result;
    }

    @Override
    public InputStream getContentFromBranch(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting content for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);
        final Optional<String> lastCommit = getLatestCommit(branch, resolvedPath);

        if (lastCommit.isEmpty()) {
            throw new FlowRegistryException(String.format("Could not find committed files at %s on branch %s response from Bitbucket API", resolvedPath, branch));
        }
        return getContentFromCommit(path, lastCommit.get());
    }

    @Override
    public InputStream getContentFromCommit(final String path, final String commitSha) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting content for path [{}] from commit [{}] in repository [{}]", resolvedPath, commitSha, repoName);
        return formFactor == BitbucketFormFactor.DATA_CENTER
                ? getContentFromCommitDataCenter(resolvedPath, commitSha)
                : getContentFromCommitCloud(resolvedPath, commitSha);
    }

    private InputStream getContentFromCommitCloud(final String resolvedPath, final String commitSha) throws FlowRegistryException {
        final HttpUriBuilder builder = getRepositoryUriBuilder()
                .addPathSegment("src")
                .addPathSegment(commitSha);
        addPathSegments(builder, resolvedPath);
        final URI uri = builder.build();
        final String errorMessage = "Error while retrieving content for repository [%s] at path %s".formatted(repoName, resolvedPath);
        final HttpResponseEntity response = this.webClient.getWebClientService()
                .get()
                .uri(uri)
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .retrieve();

        return getResponseBody(response, errorMessage, HttpURLConnection.HTTP_OK);
    }

    private InputStream getContentFromCommitDataCenter(final String resolvedPath, final String commitSha) throws FlowRegistryException {
        final URI uri = buildRawUri(resolvedPath, commitSha);
        final String errorMessage = "Error while retrieving content for repository [%s] at path %s".formatted(repoName, resolvedPath);
        final HttpResponseEntity response = this.webClient.getWebClientService()
                .get()
                .uri(uri)
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .retrieve();

        return getResponseBody(response, errorMessage, HttpURLConnection.HTTP_OK);
    }

    @Override
    public Optional<String> getContentSha(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        logger.debug("Getting content SHA for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, repoName);
        return getLatestCommit(branch, resolvedPath);
    }

    @Override
    public String createContent(final GitCreateContentRequest request) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(request.getPath());
        final String branch = request.getBranch();
        logger.debug("Creating content at path [{}] on branch [{}] in repository [{}] ", resolvedPath, branch, repoName);
        return formFactor == BitbucketFormFactor.DATA_CENTER
                ? createContentDataCenter(request, resolvedPath, branch)
                : createContentCloud(request, resolvedPath, branch);
    }

    private String createContentCloud(final GitCreateContentRequest request, final String resolvedPath, final String branch) throws FlowRegistryException {
        final StandardMultipartFormDataStreamBuilder multipartBuilder = new StandardMultipartFormDataStreamBuilder();
        multipartBuilder.addPart(resolvedPath, StandardHttpContentType.APPLICATION_JSON, request.getContent().getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart(FIELD_MESSAGE, StandardHttpContentType.TEXT_PLAIN, request.getMessage().getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart(FIELD_BRANCH, StandardHttpContentType.TEXT_PLAIN, branch.getBytes(StandardCharsets.UTF_8));

        // Add parents parameter for atomic commit - Bitbucket Cloud will reject if the branch has moved
        final String expectedCommitSha = request.getExpectedCommitSha();
        if (expectedCommitSha != null && !expectedCommitSha.isBlank()) {
            multipartBuilder.addPart(FIELD_PARENTS, StandardHttpContentType.TEXT_PLAIN, expectedCommitSha.getBytes(StandardCharsets.UTF_8));
        }

        final URI uri = getRepositoryUriBuilder().addPathSegment("src").build();
        final String errorMessage = "Error while committing content for repository [%s] on branch %s at path %s"
                .formatted(repoName, branch, resolvedPath);
        try (final HttpResponseEntity response = this.webClient.getWebClientService()
                .post()
                .uri(uri)
                .body(multipartBuilder.build(), OptionalLong.empty())
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .header(CONTENT_TYPE_HEADER, multipartBuilder.getHttpContentType().getContentType())
                .retrieve()) {
            verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_CREATED);
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed closing Bitbucket create content response", e);
        }

        return getRequiredLatestCommit(branch, resolvedPath);
    }

    private String createContentDataCenter(final GitCreateContentRequest request, final String resolvedPath, final String branch) throws FlowRegistryException {
        final StandardMultipartFormDataStreamBuilder multipartBuilder = new StandardMultipartFormDataStreamBuilder();
        final String fileName = getFileName(resolvedPath);
        multipartBuilder.addPart(FIELD_CONTENT, fileName, StandardHttpContentType.APPLICATION_OCTET_STREAM, request.getContent().getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart(FIELD_BRANCH, StandardHttpContentType.TEXT_PLAIN, branch.getBytes(StandardCharsets.UTF_8));

        final String message = request.getMessage();
        if (message != null && !message.isEmpty()) {
            multipartBuilder.addPart(FIELD_MESSAGE, StandardHttpContentType.TEXT_PLAIN, message.getBytes(StandardCharsets.UTF_8));
        }

        // Use expectedCommitSha for atomic commit - Bitbucket DC will reject if the file has changed since this commit
        final String expectedCommitSha = request.getExpectedCommitSha();
        if (expectedCommitSha != null && !expectedCommitSha.isBlank()) {
            multipartBuilder.addPart(FIELD_SOURCE_COMMIT_ID, StandardHttpContentType.TEXT_PLAIN, expectedCommitSha.getBytes(StandardCharsets.UTF_8));
        }

        final HttpUriBuilder uriBuilder = getRepositoryUriBuilder().addPathSegment("browse");
        addPathSegments(uriBuilder, resolvedPath);
        final URI uri = uriBuilder.build();
        final byte[] requestBody = toByteArray(multipartBuilder.build());
        final String errorMessage = "Error while committing content for repository [%s] on branch %s at path %s"
                .formatted(repoName, branch, resolvedPath);
        try (final HttpResponseEntity response = this.webClient.getWebClientService()
                .put()
                .uri(uri)
                .body(new ByteArrayInputStream(requestBody), OptionalLong.of(requestBody.length))
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .header(CONTENT_TYPE_HEADER, multipartBuilder.getHttpContentType().getContentType())
                .retrieve()) {
            verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_CREATED, HttpURLConnection.HTTP_OK);
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed closing Bitbucket create content response", e);
        }

        return getRequiredLatestCommit(branch, resolvedPath);
    }

    @Override
    public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(filePath);
        logger.debug("Deleting content at path [{}] on branch [{}] in repository [{}] ", resolvedPath, branch, repoName);

        final InputStream fileToBeDeleted = getContentFromBranch(filePath, branch);

        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            deleteContentDataCenter(resolvedPath, commitMessage, branch);
        } else {
            deleteContentCloud(resolvedPath, commitMessage, branch);
        }

        return fileToBeDeleted;
    }

    private void deleteContentCloud(final String resolvedPath, final String commitMessage, final String branch) throws FlowRegistryException {
        final StandardMultipartFormDataStreamBuilder multipartBuilder = new StandardMultipartFormDataStreamBuilder();
        multipartBuilder.addPart(FIELD_FILES, StandardHttpContentType.TEXT_PLAIN, resolvedPath.getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart(FIELD_MESSAGE, StandardHttpContentType.TEXT_PLAIN, commitMessage.getBytes(StandardCharsets.UTF_8));
        multipartBuilder.addPart(FIELD_BRANCH, StandardHttpContentType.TEXT_PLAIN, branch.getBytes(StandardCharsets.UTF_8));

        final URI uri = getRepositoryUriBuilder().addPathSegment("src").build();
        final String errorMessage = "Error while deleting content for repository [%s] on branch %s at path %s"
                .formatted(repoName, branch, resolvedPath);
        try (final HttpResponseEntity response = this.webClient.getWebClientService()
                .post()
                .uri(uri)
                .body(multipartBuilder.build(), OptionalLong.empty())
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .header(CONTENT_TYPE_HEADER, multipartBuilder.getHttpContentType().getContentType())
                .retrieve()) {
            verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_CREATED);
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed closing Bitbucket delete content response", e);
        }
    }

    private void deleteContentDataCenter(final String resolvedPath, final String commitMessage, final String branch) throws FlowRegistryException {
        final Optional<String> latestCommit = getLatestCommit(branch, resolvedPath);
        final HttpUriBuilder uriBuilder = getRepositoryUriBuilder().addPathSegment("browse");
        addPathSegments(uriBuilder, resolvedPath);
        uriBuilder.addQueryParameter(FIELD_BRANCH, branch);
        if (commitMessage != null) {
            uriBuilder.addQueryParameter(FIELD_MESSAGE, commitMessage);
        }
        latestCommit.ifPresent(commit -> uriBuilder.addQueryParameter(FIELD_SOURCE_COMMIT_ID, commit));

        final URI uri = uriBuilder.build();
        final String errorMessage = "Error while deleting content for repository [%s] on branch %s at path %s"
                .formatted(repoName, branch, resolvedPath);
        try (final HttpResponseEntity response = this.webClient.getWebClientService()
                .delete()
                .uri(uri)
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .retrieve()) {
            verifyStatusCode(response, errorMessage,
                    HttpURLConnection.HTTP_OK,
                    HttpURLConnection.HTTP_ACCEPTED,
                    HttpURLConnection.HTTP_NO_CONTENT,
                    HttpURLConnection.HTTP_CREATED);
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed closing Bitbucket delete content response", e);
        }
    }

    private Iterator<JsonNode> getFiles(final String branch, final String resolvedPath) throws FlowRegistryException {
        final Optional<String> lastCommit = getLatestCommit(branch, resolvedPath);

        if (lastCommit.isEmpty()) {
            return Collections.emptyIterator();
        }

        return formFactor == BitbucketFormFactor.DATA_CENTER
                ? getFilesDataCenter(branch, resolvedPath, lastCommit.get())
                : getFilesCloud(branch, resolvedPath, lastCommit.get());
    }

    private Iterator<JsonNode> getFilesCloud(final String branch, final String resolvedPath, final String commit) throws FlowRegistryException {
        final URI uri = getRepositoryUriBuilder()
                .addPathSegment("src")
                .addPathSegment(commit)
                .addPathSegment(resolvedPath)
                .build();
        final String errorMessage = String.format("Error while listing content for repository [%s] on branch %s at path %s", repoName, branch, resolvedPath);

        return getPagedResponseValues(uri, errorMessage);
    }

    private Iterator<JsonNode> getFilesDataCenter(final String branch, final String resolvedPath, final String commit) throws FlowRegistryException {
        final List<JsonNode> allValues = new ArrayList<>();
        Integer nextPageStart = null;

        do {
            final URI uri = buildBrowseUri(resolvedPath, commit, nextPageStart, false);
            final String errorMessage = "Error while listing content for repository [%s] on branch %s at path %s"
                    .formatted(repoName, branch, resolvedPath);
            try (final HttpResponseEntity response = this.webClient.getWebClientService()
                    .get()
                    .uri(uri)
                    .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                    .retrieve()) {
                verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_OK);

                final JsonNode root = parseResponseBody(response, uri);

                final JsonNode children = root.path(FIELD_CHILDREN);
                final JsonNode values = children.path(FIELD_VALUES);
                if (values.isArray()) {
                    values.forEach(allValues::add);
                }

                if (children.path(FIELD_IS_LAST_PAGE).asBoolean(true)) {
                    nextPageStart = null;
                } else {
                    final JsonNode nextPageStartNode = children.get(FIELD_NEXT_PAGE_START);
                    nextPageStart = nextPageStartNode != null && nextPageStartNode.isInt() ? nextPageStartNode.intValue() : null;
                }
            } catch (final IOException e) {
                throw new FlowRegistryException("Failed closing Bitbucket browse response", e);
            }
        } while (nextPageStart != null);

        return allValues.iterator();
    }

    private URI buildBrowseUri(final String resolvedPath, final String commit, final Integer start, final boolean rawContent) {
        final HttpUriBuilder builder = getRepositoryUriBuilder().addPathSegment("browse");
        addPathSegments(builder, resolvedPath);
        builder.addQueryParameter("at", commit);
        if (start != null) {
            builder.addQueryParameter("start", String.valueOf(start));
        }
        return builder.build();
    }

    private URI buildRawUri(final String resolvedPath, final String commit) {
        final HttpUriBuilder builder = getRepositoryUriBuilder().addPathSegment("raw");
        addPathSegments(builder, resolvedPath);
        builder.addQueryParameter("at", commit);
        return builder.build();
    }

    private Iterator<JsonNode> getListCommits(final String branch, final String path) throws FlowRegistryException {
        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            return getListCommitsDataCenter(branch, path);
        }
        return getListCommitsCloud(branch, path);
    }

    private Iterator<JsonNode> getListCommitsCloud(final String branch, final String path) throws FlowRegistryException {

        if (!hasCommits) {
            // very specific behavior when there is no commit at all yet in the repo
            final URI uri = getRepositoryUriBuilder()
                    .addPathSegment("commits")
                    .build();
            final String errorMessage = "Error while listing commits for repository [%s] on branch %s".formatted(repoName, branch);
            try (final HttpResponseEntity response = webClient.getWebClientService()
                    .get()
                    .uri(uri)
                    .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                    .retrieve()) {
                verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_OK);

                final JsonNode root = parseResponseBody(response, uri);

                final JsonNode values = root.path(FIELD_VALUES);
                if (values.isArray() && values.isEmpty()) {
                    return Collections.emptyIterator();
                } else {
                    // There is at least one commit, proceed as usual
                    // and never check again
                    hasCommits = true;
                }
            } catch (final IOException e) {
                throw new FlowRegistryException("Failed closing Bitbucket commits response", e);
            }
        }

        final URI uri = getRepositoryUriBuilder()
                .addPathSegment("commits")
                .addPathSegment(branch)
                .addQueryParameter("path", path)
                .build();
        final String errorMessage = String.format("Error while listing commits for repository [%s] on branch %s", repoName, branch);
        return getPagedResponseValues(uri, errorMessage);
    }

    private Iterator<JsonNode> getListCommitsDataCenter(final String branch, final String path) throws FlowRegistryException {
        final List<JsonNode> allValues = new ArrayList<>();
        Integer nextPageStart = null;

        do {
            if (!hasCommits) {
                final URI uri = buildCommitsUri(null, null, null);
                final String errorMessage = "Error while listing commits for repository [%s] on branch %s".formatted(repoName, branch);
                try (final HttpResponseEntity response = this.webClient.getWebClientService()
                        .get()
                        .uri(uri)
                        .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                        .retrieve()) {
                    verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_OK);

                    final JsonNode root = parseResponseBody(response, uri);
                    final JsonNode values = root.path(FIELD_VALUES);
                    if (values.isArray() && values.isEmpty()) {
                        return Collections.emptyIterator();
                    }
                    hasCommits = true;
                } catch (final IOException e) {
                    throw new FlowRegistryException("Failed closing Bitbucket commits response", e);
                }
            }

            final URI uri = buildCommitsUri(branch, path, nextPageStart);
            final String errorMessage = "Error while listing commits for repository [%s] on branch %s".formatted(repoName, branch);
            try (final HttpResponseEntity response = this.webClient.getWebClientService()
                    .get()
                    .uri(uri)
                    .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                    .retrieve()) {

                verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_OK);

                final JsonNode root = parseResponseBody(response, uri);
                final JsonNode values = root.path(FIELD_VALUES);
                if (values.isArray()) {
                    values.forEach(allValues::add);
                }

                if (root.path(FIELD_IS_LAST_PAGE).asBoolean(true)) {
                    nextPageStart = null;
                } else {
                    final JsonNode nextPageStartNode = root.get(FIELD_NEXT_PAGE_START);
                    nextPageStart = nextPageStartNode != null && nextPageStartNode.isInt() ? nextPageStartNode.intValue() : null;
                }
            } catch (final IOException e) {
                throw new FlowRegistryException("Failed closing Bitbucket commits response", e);
            }
        } while (nextPageStart != null);

        return allValues.iterator();
    }

    private URI buildCommitsUri(final String branch, final String path, final Integer start) {
        final HttpUriBuilder builder = getRepositoryUriBuilder().addPathSegment("commits");
        if (path != null && !path.isBlank()) {
            builder.addQueryParameter("path", path);
        }
        if (branch != null && !branch.isBlank()) {
            builder.addQueryParameter("until", branch);
        }
        if (start != null) {
            builder.addQueryParameter("start", String.valueOf(start));
        }
        return builder.build();
    }

    private Iterator<JsonNode> getPagedResponseValues(final URI uri, final String errorMessage) throws FlowRegistryException {
        final List<JsonNode> allValues = new ArrayList<>();
        URI nextUri = uri;
        while (nextUri != null) {
            try (final HttpResponseEntity response = webClient.getWebClientService()
                    .get()
                    .uri(nextUri)
                    .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                    .retrieve()) {

                verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_OK);

                final JsonNode root = parseResponseBody(response, nextUri);

                // collect this page’s values
                final JsonNode values = root.get(FIELD_VALUES);
                if (values != null && values.isArray()) {
                    values.forEach(allValues::add);
                }

                // prepare next iteration
                final JsonNode next = root.get(FIELD_NEXT);
                nextUri = (next != null && next.isTextual()) ? URI.create(next.asText()) : null;
            } catch (final IOException e) {
                throw new FlowRegistryException("Failed closing Bitbucket paged response", e);
            }
        }

        return allValues.iterator();
    }

    private Optional<String> getLatestCommit(final String branch, final String path) throws FlowRegistryException {
        Iterator<JsonNode> commits = getListCommits(branch, path);
        if (commits.hasNext()) {
            return Optional.ofNullable(getCommitHash(commits.next())).filter(hash -> !hash.isEmpty());
        } else {
            return Optional.empty();
        }
    }

    private String getRequiredLatestCommit(final String branch, final String resolvedPath) throws FlowRegistryException {
        final Optional<String> lastCommit = getLatestCommit(branch, resolvedPath);

        if (lastCommit.isEmpty()) {
            throw new FlowRegistryException(String.format("Could not find commit for the file %s we just tried to commit on branch %s", resolvedPath, branch));
        }

        return lastCommit.get();
    }

    private String getCommitHash(final JsonNode commit) {
        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            return commit.path(FIELD_ID).asText(EMPTY_STRING);
        }
        return commit.path(FIELD_HASH).asText(EMPTY_STRING);
    }

    private String checkRepoPermissions(BitbucketAuthenticationType authenticationType) throws FlowRegistryException {
        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            return checkReadByListingBranches();
        }

        return switch (authenticationType) {
            case OAUTH2 -> checkOAuthPermissions();
            case ACCESS_TOKEN, BASIC_AUTH -> checkReadByListingBranches();
        };
    }

    private String checkOAuthPermissions() throws FlowRegistryException {
        logger.debug("Retrieving information about current user");

        final HttpUriBuilder builder = this.webClient.getHttpUriBuilder()
                .scheme(apiScheme)
                .host(apiHost);

        if (apiPort != -1) {
            builder.port(apiPort);
        }

        apiBasePathSegments.forEach(builder::addPathSegment);

        final URI uri = builder
                .addPathSegment(apiVersion)
                .addPathSegment("user")
                .addPathSegment("permissions")
                .addPathSegment("repositories")
                .addQueryParameter("q", "repository.name=\"" + repoName + "\"")
                .build();
        final String errorMessage = "Error while retrieving permission metadata for specified repo";
        try (final HttpResponseEntity response = this.webClient.getWebClientService()
                .get()
                .uri(uri)
                .header(AUTHORIZATION_HEADER, authToken.getAuthzHeaderValue())
                .retrieve()) {

            verifyStatusCode(response, errorMessage, HttpURLConnection.HTTP_OK);

            final JsonNode jsonResponse = parseResponseBody(response, uri);

            final JsonNode values = jsonResponse.get(FIELD_VALUES);
            if (values != null && values.isArray() && values.elements().hasNext()) {
                final JsonNode permissionNode = values.elements().next().get(FIELD_PERMISSION);
                return permissionNode == null ? "none" : permissionNode.asText();
            }
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed closing Bitbucket permission response", e);
        }

        return "none";
    }

    private String checkReadByListingBranches() {
        try {
            getBranches();
            return "admin";
        } catch (FlowRegistryException e) {
            return "none";
        }
    }

    private GitCommit toGitCommit(final JsonNode commit) {
        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            final String hash = commit.path(FIELD_ID).asText();
            final JsonNode authorNode = commit.path(FIELD_AUTHOR);
            final String authorName = authorNode.path(FIELD_DISPLAY_NAME).asText(authorNode.path(FIELD_NAME).asText(EMPTY_STRING));
            final String authorEmail = authorNode.path(FIELD_EMAIL_ADDRESS).asText(EMPTY_STRING);
            final String author = authorEmail.isBlank() ? authorName : authorName + " <" + authorEmail + ">";
            final String message = commit.path(FIELD_MESSAGE_TEXT).asText();
            final long timestamp = commit.path(FIELD_AUTHOR_TIMESTAMP).asLong(0L);
            final Instant date = timestamp == 0L ? Instant.now() : Instant.ofEpochMilli(timestamp);
            return new GitCommit(hash, author, message, date);
        }

        final JsonNode authorNode = commit.path(FIELD_AUTHOR);
        final String author = authorNode.path(FIELD_RAW).asText();
        final String message = commit.path(FIELD_MESSAGE_TEXT).asText();
        final String dateText = commit.path("date").asText();
        final Instant date = (dateText == null || dateText.isEmpty()) ? Instant.now() : Instant.parse(dateText);
        return new GitCommit(commit.path(FIELD_HASH).asText(), author, message, date);
    }

    private InputStream getResponseBody(final HttpResponseEntity response, final String errorMessage, final int... expectedStatusCodes) throws FlowRegistryException {
        try {
            verifyStatusCode(response, errorMessage, expectedStatusCodes);
            return new HttpResponseBodyInputStream(response);
        } catch (final FlowRegistryException e) {
            closeQuietly(response);
            throw e;
        }
    }

    private void closeQuietly(final HttpResponseEntity response) {
        try {
            response.close();
        } catch (final IOException ioe) {
            logger.warn("Failed closing Bitbucket HTTP response", ioe);
        }
    }

    private static class HttpResponseBodyInputStream extends FilterInputStream {
        private final HttpResponseEntity response;
        private boolean closed;

        protected HttpResponseBodyInputStream(final HttpResponseEntity response) {
            super(response.body());
            this.response = response;
        }

        @Override
        public void close() throws IOException {
            if (!closed) {
                closed = true;
                IOException suppressed = null;
                try {
                    super.close();
                } catch (final IOException closeException) {
                    suppressed = closeException;
                }
                try {
                    response.close();
                } catch (final IOException responseCloseException) {
                    if (suppressed != null) {
                        responseCloseException.addSuppressed(suppressed);
                    }
                    throw responseCloseException;
                }
                if (suppressed != null) {
                    throw suppressed;
                }
            }
        }
    }

    private String getErrorMessage(HttpResponseEntity response) throws FlowRegistryException {
        final JsonNode jsonResponse;
        try {
            jsonResponse = this.objectMapper.readTree(response.body());
        } catch (IOException e) {
            throw new FlowRegistryException("Could not parse response from Bitbucket API", e);
        }
        if (jsonResponse == null) {
            return "Unknown error";
        }

        final JsonNode errorNode = jsonResponse.get(FIELD_ERROR_MESSAGE);
        if (errorNode != null) {
            final String type = jsonResponse.path(FIELD_TYPE).asText("Error");
            final String message = errorNode.path(FIELD_MESSAGE_TEXT).asText(errorNode.toString());
            return String.format("[%s] - %s", type, message);
        }

        final JsonNode errorsNode = jsonResponse.get(FIELD_ERRORS);
        if (errorsNode != null && errorsNode.isArray() && errorsNode.size() > 0) {
            final JsonNode firstError = errorsNode.get(0);
            return firstError.path(FIELD_MESSAGE_TEXT).asText(firstError.toString());
        }

        final JsonNode messageNode = jsonResponse.get(FIELD_MESSAGE_TEXT);
        if (messageNode != null) {
            return messageNode.asText();
        }

        return jsonResponse.toString();
    }

    private JsonNode parseResponseBody(final HttpResponseEntity response, final URI uri) throws FlowRegistryException {
        try {
            return objectMapper.readTree(response.body());
        } catch (final IOException e) {
            throw new FlowRegistryException(String.format("Could not parse Bitbucket API response at %s", uri), e);
        }
    }

    private void verifyStatusCode(final HttpResponseEntity response, final String errorMessage, final int... expectedStatusCodes) throws FlowRegistryException {
        final int statusCode = response.statusCode();
        for (final int expectedStatusCode : expectedStatusCodes) {
            if (statusCode == expectedStatusCode) {
                return;
            }
        }
        throw new FlowRegistryException("%s: %s".formatted(errorMessage, getErrorMessage(response)));
    }

    private String getResolvedPath(final String path) {
        return repoPath == null ? path : repoPath + "/" + path;
    }

    private void addPathSegments(final HttpUriBuilder builder, final String path) {
        if (path == null || path.isBlank()) {
            return;
        }

        final String normalizedPath = path.startsWith("/") ? path.substring(1) : path;
        final String[] segments = normalizedPath.split("/");
        for (final String segment : segments) {
            if (!segment.isBlank()) {
                builder.addPathSegment(segment);
            }
        }
    }

    private boolean isDirectoryEntry(final JsonNode entry) {
        final JsonNode typeNode = entry.get(FIELD_TYPE);
        if (typeNode == null) {
            return false;
        }

        final String type = typeNode.asText();
        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            return ENTRY_DIRECTORY_DATA_CENTER.equalsIgnoreCase(type);
        }
        return ENTRY_DIRECTORY_CLOUD.equals(type);
    }

    private boolean isFileEntry(final JsonNode entry) {
        final JsonNode typeNode = entry.get(FIELD_TYPE);
        if (typeNode == null) {
            return false;
        }

        final String type = typeNode.asText();
        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            return ENTRY_FILE_DATA_CENTER.equalsIgnoreCase(type);
        }
        return ENTRY_FILE_CLOUD.equals(type);
    }

    private String getEntryPath(final JsonNode entry) {
        if (formFactor == BitbucketFormFactor.DATA_CENTER) {
            final JsonNode pathNode = entry.get(FIELD_PATH);
            if (pathNode == null) {
                return EMPTY_STRING;
            }

            final JsonNode toStringNode = pathNode.get(FIELD_TO_STRING);
            if (toStringNode != null && toStringNode.isTextual()) {
                return toStringNode.asText();
            }

            return pathNode.asText(EMPTY_STRING);
        }

        final JsonNode pathNode = entry.get(FIELD_PATH);
        return pathNode == null ? EMPTY_STRING : pathNode.asText();
    }

    private HttpUriBuilder getRepositoryUriBuilder() {
        final HttpUriBuilder builder = this.webClient.getHttpUriBuilder()
                .scheme(apiScheme)
                .host(apiHost);

        if (apiPort != -1) {
            builder.port(apiPort);
        }

        apiBasePathSegments.forEach(builder::addPathSegment);

        if (formFactor == BitbucketFormFactor.CLOUD) {
            builder.addPathSegment(apiVersion)
                    .addPathSegment("repositories")
                    .addPathSegment(workspace)
                    .addPathSegment(repoName);
        } else {
            builder.addPathSegment("rest")
                    .addPathSegment("api")
                    .addPathSegment(apiVersion)
                    .addPathSegment("projects")
                    .addPathSegment(projectKey)
                    .addPathSegment("repos")
                    .addPathSegment(repoName);
        }

        return builder;
    }

    private static ParsedApiUrl parseApiUrl(final String apiUrl) throws FlowRegistryException {
        final String trimmedApiUrl = apiUrl == null ? null : apiUrl.trim();
        if (trimmedApiUrl == null || trimmedApiUrl.isEmpty()) {
            throw new FlowRegistryException("API Instance is required");
        }

        if (!trimmedApiUrl.contains("://")) {
            return new ParsedApiUrl("https", trimmedApiUrl, -1, List.of());
        }

        final URI uri = URI.create(trimmedApiUrl);
        final String scheme = uri.getScheme() == null ? "https" : uri.getScheme();
        final String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new FlowRegistryException("API Instance must include a host");
        }

        final List<String> segments = new ArrayList<>();
        final String path = uri.getPath();
        if (path != null && !path.isBlank()) {
            final String[] rawSegments = path.split("/");
            for (final String segment : rawSegments) {
                if (!segment.isBlank()) {
                    segments.add(segment);
                }
            }
        }

        return new ParsedApiUrl(scheme, host, uri.getPort(), List.copyOf(segments));
    }

    private record ParsedApiUrl(String scheme, String host, int port, List<String> pathSegments) {
    }

    private String getFileName(final String resolvedPath) {
        if (resolvedPath == null || resolvedPath.isBlank()) {
            return "content";
        }

        final Path path = Paths.get(resolvedPath);
        final Path fileName = path.getFileName();
        return fileName == null ? resolvedPath : fileName.toString();
    }

    private byte[] toByteArray(final InputStream inputStream) throws FlowRegistryException {
        try (inputStream; ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            StreamUtils.copy(inputStream, outputStream);
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new FlowRegistryException("Failed to prepare multipart request", e);
        }
    }

    private interface BitbucketToken<T> {
        T getAuthzHeaderValue();
    }

    private class BasicAuthToken implements BitbucketToken<String> {
        private String token;

        public BasicAuthToken(final String username, final String appPassword) {
            final String basicCreds = username + ":" + appPassword;
            final byte[] basicCredsBytes = basicCreds.getBytes(StandardCharsets.UTF_8);

            final Base64.Encoder encoder = Base64.getEncoder();
            token = encoder.encodeToString(basicCredsBytes);
        }

        @Override
        public String getAuthzHeaderValue() {
            return BASIC + " " + token;
        }
    }

    private class AccessToken implements BitbucketToken<String> {
        private String token;

        public AccessToken(final String token) {
            this.token = token;
        }

        @Override
        public String getAuthzHeaderValue() {
            return BEARER + " " + token;
        }
    }

    private class OAuthToken implements BitbucketToken<String> {
        private OAuth2AccessTokenProvider oauthService;

        public OAuthToken(final OAuth2AccessTokenProvider oauthService) {
            this.oauthService = oauthService;
        }

        @Override
        public String getAuthzHeaderValue() {
            return BEARER + " " + oauthService.getAccessDetails().getAccessToken();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String clientId;
        private String apiUrl;
        private BitbucketFormFactor formFactor;
        private BitbucketAuthenticationType authenticationType;
        private String accessToken;
        private String username;
        private String appPassword;
        private OAuth2AccessTokenProvider oauthService;
        private WebClientServiceProvider webClient;
        private String workspace;
        private String projectKey;
        private String repoName;
        private String repoPath;
        private ComponentLog logger;

        public Builder clientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder apiUrl(final String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public Builder formFactor(final BitbucketFormFactor formFactor) {
            this.formFactor = formFactor;
            return this;
        }

        public Builder authenticationType(final BitbucketAuthenticationType authenticationType) {
            this.authenticationType = authenticationType;
            return this;
        }

        public Builder accessToken(final String accessToken) {
            this.accessToken = accessToken;
            return this;
        }

        public Builder username(final String username) {
            this.username = username;
            return this;
        }

        public Builder appPassword(final String appPassword) {
            this.appPassword = appPassword;
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

        public Builder workspace(final String workspace) {
            this.workspace = workspace;
            return this;
        }

        public Builder projectKey(final String projectKey) {
            this.projectKey = projectKey;
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

        public Builder logger(final ComponentLog logger) {
            this.logger = logger;
            return this;
        }

        public BitbucketRepositoryClient build() throws FlowRegistryException {
            return new BitbucketRepositoryClient(this);
        }
    }
}
