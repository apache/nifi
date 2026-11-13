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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.MediaType;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Git Repository Client implementation for Gitea using the REST API version 1.
 * The same API is provided by Forgejo, including Codeberg.
 */
public class GiteaRepositoryClient implements GitRepositoryClient {

    static final String API_PATH = "/api/v1";

    // Gitea returns the server [git] COMMITS_RANGE_SIZE number of commits when listing commits for a path, which defaults to 50
    static final int COMMIT_PAGE_SIZE = 50;

    static final int BRANCH_PAGE_SIZE = 50;

    private static final int MAXIMUM_PAGES = 10_000;
    private static final int HTTP_UNPROCESSABLE_ENTITY = 422;
    private static final int HTTP_CONTENT_TOO_LARGE = 413;
    private static final int MAXIMUM_ERROR_BODY_LENGTH = 1024;

    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String ACCEPT_HEADER = "Accept";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String TOKEN_PREFIX = "token ";
    private static final String TOTAL_COUNT_HEADER = "X-Total-Count";

    private static final String FORWARD_SLASH = "/";

    private static final String SEGMENT_REPOS = "repos";
    private static final String SEGMENT_BRANCHES = "branches";
    private static final String SEGMENT_CONTENTS = "contents";
    private static final String SEGMENT_MEDIA = "media";
    private static final String SEGMENT_COMMITS = "commits";
    private static final String SEGMENT_GIT = "git";
    private static final String SEGMENT_BLOBS = "blobs";

    private static final String PARAM_REF = "ref";
    private static final String PARAM_SHA = "sha";
    private static final String PARAM_PATH = "path";
    private static final String PARAM_PAGE = "page";
    private static final String PARAM_LIMIT = "limit";
    private static final String PARAM_STAT = "stat";
    private static final String PARAM_VERIFICATION = "verification";
    private static final String PARAM_FILES = "files";
    private static final String FALSE = "false";

    private static final String FIELD_PERMISSIONS = "permissions";
    private static final String FIELD_PULL = "pull";
    private static final String FIELD_PUSH = "push";
    private static final String FIELD_ARCHIVED = "archived";
    private static final String FIELD_MIRROR = "mirror";
    private static final String FIELD_EMPTY = "empty";
    private static final String FIELD_NAME = "name";
    private static final String FIELD_TYPE = "type";
    private static final String FIELD_SHA = "sha";
    private static final String FIELD_CONTENT = "content";
    private static final String FIELD_COMMIT = "commit";
    private static final String FIELD_MESSAGE = "message";
    private static final String FIELD_AUTHOR = "author";
    private static final String FIELD_COMMITTER = "committer";
    private static final String FIELD_EMAIL = "email";
    private static final String FIELD_DATE = "date";
    private static final String FIELD_BRANCH = "branch";
    private static final String FIELD_NEW_BRANCH_NAME = "new_branch_name";
    private static final String FIELD_OLD_REF_NAME = "old_ref_name";

    private static final String TYPE_FILE = "file";
    private static final String TYPE_DIR = "dir";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String clientId;
    private final String apiUrl;
    private final URI apiUri;
    private final String repoOwner;
    private final String repoName;
    private final String repoPath;
    private final String accessToken;
    private final WebClientServiceProvider webClient;
    private final ComponentLog logger;

    private final boolean canRead;
    private final boolean canWrite;

    private GiteaRepositoryClient(final Builder builder) throws FlowRegistryException {
        clientId = Objects.requireNonNull(builder.clientId, "Client ID required");
        apiUrl = normalizeApiUrl(Objects.requireNonNull(builder.apiUrl, "API URL required"));
        apiUri = URI.create(apiUrl);
        repoOwner = Objects.requireNonNull(builder.repoOwner, "Repository Owner required");
        repoName = Objects.requireNonNull(builder.repoName, "Repository Name required");
        repoPath = trimSlashes(builder.repoPath);
        accessToken = Objects.requireNonNull(builder.accessToken, "Access Token required");
        webClient = Objects.requireNonNull(builder.webClient, "Web Client Service required");
        logger = Objects.requireNonNull(builder.logger, "Logger required");

        final URI uri = repositoryUriBuilder().build();
        try (HttpResponseEntity response = execute(webClientService().get(), uri, null)) {
            if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                throw new FlowRegistryException("Failed to access repository [%s/%s] - %s".formatted(repoOwner, repoName, getErrorMessage(response)));
            }
            final JsonNode repository = readJson(response, uri);
            final JsonNode permissions = repository.path(FIELD_PERMISSIONS);
            final boolean archived = repository.path(FIELD_ARCHIVED).asBoolean(false);
            final boolean mirror = repository.path(FIELD_MIRROR).asBoolean(false);
            canRead = permissions.path(FIELD_PULL).asBoolean(false);
            canWrite = permissions.path(FIELD_PUSH).asBoolean(false) && !archived && !mirror;
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed to access repository [%s/%s]".formatted(repoOwner, repoName), e);
        }

        logger.info("Created {} for Flow Registry Client ID [{}] repository [{}/{}] read [{}] write [{}]",
                getClass().getSimpleName(), clientId, repoOwner, repoName, canRead, canWrite);
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Normalize the configured URL of the Gitea instance by removing trailing separators and the API path when provided.
     *
     * @param apiUrl URL of the Gitea instance
     * @return Normalized URL of the Gitea instance without trailing separator
     */
    static String normalizeApiUrl(final String apiUrl) {
        String normalized = apiUrl.trim();
        while (normalized.endsWith(FORWARD_SLASH)) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (normalized.endsWith(API_PATH)) {
            normalized = normalized.substring(0, normalized.length() - API_PATH.length());
        }
        while (normalized.endsWith(FORWARD_SLASH)) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    public String getApiUrl() {
        return apiUrl;
    }

    public String getRepoOwner() {
        return repoOwner;
    }

    public String getRepoName() {
        return repoName;
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
    public Set<String> getBranches() throws IOException, FlowRegistryException {
        logger.debug("Getting branches for repository [{}/{}]", repoOwner, repoName);
        final Set<String> branches = new HashSet<>();
        for (int page = 1; page <= MAXIMUM_PAGES; page++) {
            final URI uri = repositoryUriBuilder(SEGMENT_BRANCHES)
                    .addQueryParameter(PARAM_PAGE, Integer.toString(page))
                    .addQueryParameter(PARAM_LIMIT, Integer.toString(BRANCH_PAGE_SIZE))
                    .build();

            final JsonNode response;
            final Optional<Long> totalCount;
            try (HttpResponseEntity entity = execute(webClientService().get(), uri, null)) {
                if (entity.statusCode() != HttpURLConnection.HTTP_OK) {
                    throw new FlowRegistryException("Request to [%s] failed - %s".formatted(uri, getErrorMessage(entity)));
                }
                totalCount = getHeaderLong(entity, TOTAL_COUNT_HEADER);
                response = readJson(entity, uri);
            }

            for (final JsonNode branch : response) {
                branches.add(branch.path(FIELD_NAME).asText());
            }

            final boolean allRetrieved = totalCount.map(total -> branches.size() >= total).orElse(response.size() < BRANCH_PAGE_SIZE);
            if (response.isEmpty() || allRetrieved) {
                break;
            }
        }
        return branches;
    }

    @Override
    public Set<String> getTopLevelDirectoryNames(final String branch) throws IOException, FlowRegistryException {
        logger.debug("Getting top-level directories for repository [{}/{}] on branch [{}]", repoOwner, repoName, branch);
        return getDirectoryEntryNames(resolvePath(""), branch, TYPE_DIR);
    }

    @Override
    public Set<String> getFileNames(final String directory, final String branch) throws IOException, FlowRegistryException {
        logger.debug("Getting file names in directory [{}] for repository [{}/{}] on branch [{}]", directory, repoOwner, repoName, branch);
        return getDirectoryEntryNames(resolvePath(directory), branch, TYPE_FILE);
    }

    @Override
    public List<GitCommit> getCommits(final String path, final String branch) throws IOException, FlowRegistryException {
        final String resolvedPath = resolvePath(path);
        logger.debug("Getting commits for [{}] on branch [{}] in repository [{}/{}]", resolvedPath, branch, repoOwner, repoName);

        final URI uri = repositoryUriBuilder(SEGMENT_COMMITS)
                .addQueryParameter(PARAM_SHA, branch)
                .addQueryParameter(PARAM_PATH, resolvedPath)
                .addQueryParameter(PARAM_STAT, FALSE)
                .addQueryParameter(PARAM_VERIFICATION, FALSE)
                .addQueryParameter(PARAM_FILES, FALSE)
                .addQueryParameter(PARAM_LIMIT, Integer.toString(COMMIT_PAGE_SIZE))
                .build();

        try (HttpResponseEntity response = execute(webClientService().get(), uri, null)) {
            final int statusCode = response.statusCode();
            // Not Found indicates that the path does not exist and Conflict indicates that the repository is empty
            if (statusCode == HttpURLConnection.HTTP_NOT_FOUND || statusCode == HttpURLConnection.HTTP_CONFLICT) {
                logger.debug("No commits found for [{}] on branch [{}]: HTTP {}", resolvedPath, branch, statusCode);
                return List.of();
            } else if (statusCode != HttpURLConnection.HTTP_OK) {
                throw new FlowRegistryException("Request to [%s] failed - %s".formatted(uri, getErrorMessage(response)));
            }

            final List<GitCommit> commits = new ArrayList<>();
            for (final JsonNode node : readJson(response, uri)) {
                commits.add(toGitCommit(node));
            }
            return commits;
        }
    }

    @Override
    public InputStream getContentFromBranch(final String path, final String branch) throws IOException, FlowRegistryException {
        final String resolvedPath = resolvePath(path);
        logger.debug("Getting content for [{}] from branch [{}] in repository [{}/{}]", resolvedPath, branch, repoOwner, repoName);
        return getMedia(resolvedPath, branch);
    }

    @Override
    public InputStream getContentFromCommit(final String path, final String commitSha) throws IOException, FlowRegistryException {
        final String resolvedPath = resolvePath(path);
        logger.debug("Getting content for [{}] from commit [{}] in repository [{}/{}]", resolvedPath, commitSha, repoOwner, repoName);
        return getMedia(resolvedPath, commitSha);
    }

    @Override
    public Optional<String> getContentSha(final String path, final String branch) throws IOException, FlowRegistryException {
        final String resolvedPath = resolvePath(path);
        logger.debug("Getting content SHA for [{}] on branch [{}] in repository [{}/{}]", resolvedPath, branch, repoOwner, repoName);
        return getFile(resolvedPath, branch).map(file -> file.path(FIELD_SHA).asText());
    }

    @Override
    public Optional<String> getContentShaAtCommit(final String path, final String commitSha) throws IOException, FlowRegistryException {
        final String resolvedPath = resolvePath(path);
        logger.debug("Getting content SHA for [{}] at commit [{}] in repository [{}/{}]", resolvedPath, commitSha, repoOwner, repoName);
        return getFile(resolvedPath, commitSha).map(file -> file.path(FIELD_SHA).asText());
    }

    @Override
    public String createContent(final GitCreateContentRequest request) throws IOException, FlowRegistryException {
        final String resolvedPath = resolvePath(request.getPath());
        final String branch = request.getBranch();
        final String existingContentSha = request.getExistingContentSha();
        logger.debug("Creating content at [{}] on branch [{}] in repository [{}/{}]", resolvedPath, branch, repoOwner, repoName);

        final ObjectNode body = createCommitBody(request.getMessage(), branch, request.getAuthorName(), request.getAuthorEmail());
        body.put(FIELD_CONTENT, Base64.getEncoder().encodeToString(request.getContent().getBytes(StandardCharsets.UTF_8)));

        // Gitea and Forgejo create files with POST and update files with PUT providing the SHA of the current blob
        final HttpRequestUriSpec requestSpec;
        if (existingContentSha == null) {
            requestSpec = webClientService().post();
        } else {
            body.put(FIELD_SHA, existingContentSha);
            requestSpec = webClientService().put();
        }

        final URI uri = repositoryUriBuilder(SEGMENT_CONTENTS, resolvedPath).build();
        try (HttpResponseEntity response = execute(requestSpec, uri, body)) {
            final int statusCode = response.statusCode();
            if (statusCode == HttpURLConnection.HTTP_OK || statusCode == HttpURLConnection.HTTP_CREATED) {
                return readJson(response, uri).path(FIELD_COMMIT).path(FIELD_SHA).asText(null);
            }
            throw createWriteException(response, resolvedPath, branch, existingContentSha != null);
        }
    }

    @Override
    public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException, IOException {
        return deleteContent(filePath, commitMessage, branch, null, null);
    }

    @Override
    public InputStream deleteContent(final String filePath, final String commitMessage, final String branch,
                                     final String authorName, final String authorEmail) throws FlowRegistryException, IOException {
        final String resolvedPath = resolvePath(filePath);
        logger.debug("Deleting [{}] on branch [{}] in repository [{}/{}]", resolvedPath, branch, repoOwner, repoName);

        final JsonNode file = getFile(resolvedPath, branch)
                .orElseThrow(() -> new FlowRegistryException("File [%s] not found on branch [%s]".formatted(resolvedPath, branch)));
        final String blobSha = file.path(FIELD_SHA).asText();
        final byte[] content = getFileContent(file);

        final ObjectNode body = createCommitBody(commitMessage, branch, authorName, authorEmail);
        body.put(FIELD_SHA, blobSha);

        final URI uri = repositoryUriBuilder(SEGMENT_CONTENTS, resolvedPath).build();
        try (HttpResponseEntity response = execute(webClientService().delete(), uri, body)) {
            if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                throw createWriteException(response, resolvedPath, branch, true);
            }
        }
        return new ByteArrayInputStream(content);
    }

    @Override
    public void createBranch(final String newBranchName, final String sourceBranch, final Optional<String> sourceCommitSha)
            throws IOException, FlowRegistryException {
        if (newBranchName == null || newBranchName.isBlank()) {
            throw new IllegalArgumentException("Branch name must be specified");
        }
        if (sourceBranch == null || sourceBranch.isBlank()) {
            throw new IllegalArgumentException("Source branch must be specified");
        }

        final String trimmedNewBranch = newBranchName.trim();
        final String sourceRef = sourceCommitSha.filter(sha -> !sha.isBlank()).orElse(sourceBranch.trim());
        logger.info("Creating branch [{}] from [{}] in repository [{}/{}]", trimmedNewBranch, sourceRef, repoOwner, repoName);

        final ObjectNode body = MAPPER.createObjectNode();
        body.put(FIELD_NEW_BRANCH_NAME, trimmedNewBranch);
        body.put(FIELD_OLD_REF_NAME, sourceRef);

        final URI uri = repositoryUriBuilder(SEGMENT_BRANCHES).build();
        try (HttpResponseEntity response = execute(webClientService().post(), uri, body)) {
            final int statusCode = response.statusCode();
            if (statusCode == HttpURLConnection.HTTP_CONFLICT) {
                throw new FlowRegistryException("Branch [%s] already exists in repository [%s/%s]".formatted(trimmedNewBranch, repoOwner, repoName));
            } else if (statusCode != HttpURLConnection.HTTP_CREATED) {
                throw new FlowRegistryException("Failed to create branch [%s] from [%s] in repository [%s/%s] - %s"
                        .formatted(trimmedNewBranch, sourceRef, repoOwner, repoName, getErrorMessage(response)));
            }
        }
    }

    private Set<String> getDirectoryEntryNames(final String directory, final String ref, final String entryType) throws IOException, FlowRegistryException {
        final URI uri = repositoryUriBuilder(SEGMENT_CONTENTS, directory).addQueryParameter(PARAM_REF, ref).build();
        try (HttpResponseEntity response = execute(webClientService().get(), uri, null)) {
            if (response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                // Not Found indicates a missing directory, a missing branch or an empty repository
                if (isBranchMissing(ref)) {
                    throw new FlowRegistryException("Branch [%s] not found in repository [%s/%s]".formatted(ref, repoOwner, repoName));
                }
                return Set.of();
            } else if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                throw new FlowRegistryException("Request to [%s] failed - %s".formatted(uri, getErrorMessage(response)));
            }

            final JsonNode entries = readJson(response, uri);
            final Set<String> names = new HashSet<>();
            // Directory listings are arrays while file paths return an object
            if (entries.isArray()) {
                for (final JsonNode entry : entries) {
                    if (entryType.equals(entry.path(FIELD_TYPE).asText())) {
                        names.add(entry.path(FIELD_NAME).asText());
                    }
                }
            }
            return names;
        }
    }

    private boolean isBranchMissing(final String branch) throws IOException, FlowRegistryException {
        final URI branchUri = repositoryUriBuilder(SEGMENT_BRANCHES, branch).build();
        try (HttpResponseEntity response = execute(webClientService().get(), branchUri, null)) {
            if (response.statusCode() == HttpURLConnection.HTTP_OK) {
                return false;
            } else if (response.statusCode() != HttpURLConnection.HTTP_NOT_FOUND) {
                throw new FlowRegistryException("Request to [%s] failed - %s".formatted(branchUri, getErrorMessage(response)));
            }
        }

        // Empty repositories have no branches until the first commit creates the default branch
        final URI repositoryUri = repositoryUriBuilder().build();
        try (HttpResponseEntity response = execute(webClientService().get(), repositoryUri, null)) {
            if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                throw new FlowRegistryException("Request to [%s] failed - %s".formatted(repositoryUri, getErrorMessage(response)));
            }
            return !readJson(response, repositoryUri).path(FIELD_EMPTY).asBoolean(false);
        }
    }

    private Optional<JsonNode> getFile(final String resolvedPath, final String ref) throws IOException, FlowRegistryException {
        final URI uri = repositoryUriBuilder(SEGMENT_CONTENTS, resolvedPath).addQueryParameter(PARAM_REF, ref).build();
        try (HttpResponseEntity response = execute(webClientService().get(), uri, null)) {
            if (response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                return Optional.empty();
            } else if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                throw new FlowRegistryException("Request to [%s] failed - %s".formatted(uri, getErrorMessage(response)));
            }

            final JsonNode file = readJson(response, uri);
            if (file.isObject() && TYPE_FILE.equals(file.path(FIELD_TYPE).asText())) {
                return Optional.of(file);
            }
            return Optional.empty();
        }
    }

    private byte[] getFileContent(final JsonNode file) throws IOException, FlowRegistryException {
        final JsonNode content = file.get(FIELD_CONTENT);
        if (content != null && content.isTextual()) {
            return Base64.getMimeDecoder().decode(content.asText());
        }

        // Content is omitted for files larger than the server [api] DEFAULT_MAX_BLOB_SIZE setting
        final String blobSha = file.path(FIELD_SHA).asText();
        final URI uri = repositoryUriBuilder(SEGMENT_GIT, SEGMENT_BLOBS, blobSha).build();
        try (HttpResponseEntity response = execute(webClientService().get(), uri, null)) {
            if (response.statusCode() != HttpURLConnection.HTTP_OK) {
                throw new FlowRegistryException("Request to [%s] failed - %s".formatted(uri, getErrorMessage(response)));
            }
            return Base64.getMimeDecoder().decode(readJson(response, uri).path(FIELD_CONTENT).asText());
        }
    }

    private InputStream getMedia(final String resolvedPath, final String ref) throws IOException, FlowRegistryException {
        // The media endpoint returns the raw file and resolves Git LFS pointers
        final URI uri = repositoryUriBuilder(SEGMENT_MEDIA, resolvedPath).addQueryParameter(PARAM_REF, ref).build();
        final HttpResponseEntity response = execute(webClientService().get(), uri, null);
        if (response.statusCode() == HttpURLConnection.HTTP_OK) {
            return response.body();
        }

        try (response) {
            if (response.statusCode() == HttpURLConnection.HTTP_NOT_FOUND) {
                throw new FlowRegistryException("File [%s] not found at [%s] in repository [%s/%s]".formatted(resolvedPath, ref, repoOwner, repoName));
            }
            throw new FlowRegistryException("Request to [%s] failed - %s".formatted(uri, getErrorMessage(response)));
        }
    }

    private GitCommit toGitCommit(final JsonNode node) {
        final String sha = node.path(FIELD_SHA).asText();
        final JsonNode commit = node.path(FIELD_COMMIT);
        final String message = commit.path(FIELD_MESSAGE).asText("").stripTrailing();
        final String author = commit.path(FIELD_AUTHOR).path(FIELD_NAME).asText("");
        final String date = commit.path(FIELD_COMMITTER).path(FIELD_DATE).asText(commit.path(FIELD_AUTHOR).path(FIELD_DATE).asText());
        final Instant commitDate = OffsetDateTime.parse(date).toInstant();
        return new GitCommit(sha, author, message, commitDate);
    }

    private ObjectNode createCommitBody(final String message, final String branch, final String authorName, final String authorEmail) {
        final ObjectNode body = MAPPER.createObjectNode();
        body.put(FIELD_MESSAGE, message);
        body.put(FIELD_BRANCH, branch);
        // Committer remains the authenticated user while the author is set when both name and email are provided
        if (authorName != null && authorEmail != null) {
            final ObjectNode author = body.putObject(FIELD_AUTHOR);
            author.put(FIELD_NAME, authorName);
            author.put(FIELD_EMAIL, authorEmail);
        }
        return body;
    }

    private FlowRegistryException createWriteException(final HttpResponseEntity response, final String resolvedPath, final String branch,
                                                       final boolean existingContent) {
        final int statusCode = response.statusCode();
        final String errorMessage = getErrorMessage(response);
        // Gitea returns Unprocessable Entity and Forgejo returns Conflict when the provided blob SHA does not match the current blob
        if (existingContent && (statusCode == HttpURLConnection.HTTP_CONFLICT || statusCode == HTTP_UNPROCESSABLE_ENTITY)) {
            return new FlowRegistryException("File [%s] on branch [%s] has been modified by another commit - %s".formatted(resolvedPath, branch, errorMessage));
        } else if (statusCode == HttpURLConnection.HTTP_FORBIDDEN) {
            return new FlowRegistryException("Write access denied for [%s] on branch [%s]: the Access Token requires the write:repository scope and the branch must not be protected - %s"
                    .formatted(resolvedPath, branch, errorMessage));
        } else if (statusCode == HTTP_CONTENT_TOO_LARGE) {
            return new FlowRegistryException("Repository quota exceeded writing [%s] on branch [%s] - %s".formatted(resolvedPath, branch, errorMessage));
        }
        return new FlowRegistryException("Failed to write [%s] on branch [%s] - %s".formatted(resolvedPath, branch, errorMessage));
    }

    private HttpResponseEntity execute(final HttpRequestUriSpec requestSpec, final URI uri, final JsonNode body) throws FlowRegistryException {
        final HttpRequestBodySpec bodySpec = requestSpec.uri(uri)
                .header(AUTHORIZATION_HEADER, TOKEN_PREFIX + accessToken)
                .header(ACCEPT_HEADER, MediaType.APPLICATION_JSON.getMediaType());

        if (body == null) {
            return bodySpec.retrieve();
        }

        final byte[] serialized;
        try {
            serialized = MAPPER.writeValueAsBytes(body);
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed to serialize request for [%s]".formatted(uri), e);
        }
        return bodySpec.header(CONTENT_TYPE_HEADER, MediaType.APPLICATION_JSON.getMediaType())
                .body(new ByteArrayInputStream(serialized), OptionalLong.of(serialized.length))
                .retrieve();
    }

    private JsonNode readJson(final HttpResponseEntity response, final URI uri) throws FlowRegistryException {
        try (InputStream body = response.body()) {
            return MAPPER.readTree(body);
        } catch (final IOException e) {
            throw new FlowRegistryException("Failed to read response from [%s]".formatted(uri), e);
        }
    }

    private String getErrorMessage(final HttpResponseEntity response) {
        String responseBody;
        try (InputStream body = response.body()) {
            responseBody = body == null ? "" : new String(body.readNBytes(MAXIMUM_ERROR_BODY_LENGTH), StandardCharsets.UTF_8);
        } catch (final IOException e) {
            responseBody = "";
        }

        String message = responseBody;
        try {
            final JsonNode error = MAPPER.readTree(responseBody);
            if (error != null && error.hasNonNull(FIELD_MESSAGE)) {
                message = error.get(FIELD_MESSAGE).asText();
            }
        } catch (final IOException ignored) {
            // Use response body when not formatted as JSON
        }
        return "HTTP %d: %s".formatted(response.statusCode(), message);
    }

    private Optional<Long> getHeaderLong(final HttpResponseEntity response, final String headerName) {
        return response.headers().getFirstHeader(headerName).flatMap(value -> {
            try {
                return Optional.of(Long.parseLong(value.trim()));
            } catch (final NumberFormatException e) {
                return Optional.empty();
            }
        });
    }

    private WebClientService webClientService() {
        return webClient.getWebClientService();
    }

    /**
     * Create URI builder for repository resources. Path elements containing forward slashes are added as multiple path segments.
     *
     * @param pathElements Path elements relative to the repository
     * @return HTTP URI Builder configured for repository resources
     */
    private HttpUriBuilder repositoryUriBuilder(final String... pathElements) {
        final HttpUriBuilder builder = webClient.getHttpUriBuilder()
                .scheme(apiUri.getScheme())
                .host(apiUri.getHost())
                .port(apiUri.getPort())
                .encodedPath(apiUri.getRawPath() + API_PATH)
                .addPathSegment(SEGMENT_REPOS)
                .addPathSegment(repoOwner)
                .addPathSegment(repoName);

        for (final String pathElement : pathElements) {
            for (final String segment : pathElement.split(FORWARD_SLASH)) {
                if (!segment.isEmpty()) {
                    builder.addPathSegment(segment);
                }
            }
        }
        return builder;
    }

    private String resolvePath(final String path) {
        final String trimmedPath = trimSlashes(path);
        if (repoPath == null) {
            return trimmedPath == null ? "" : trimmedPath;
        }
        return trimmedPath == null ? repoPath : repoPath + FORWARD_SLASH + trimmedPath;
    }

    private static String trimSlashes(final String path) {
        if (path == null) {
            return null;
        }
        String trimmed = path.trim();
        while (trimmed.startsWith(FORWARD_SLASH)) {
            trimmed = trimmed.substring(1);
        }
        while (trimmed.endsWith(FORWARD_SLASH)) {
            trimmed = trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed.isEmpty() ? null : trimmed;
    }

    public static class Builder {
        private String clientId;
        private String apiUrl;
        private String repoOwner;
        private String repoName;
        private String repoPath;
        private String accessToken;
        private WebClientServiceProvider webClient;
        private ComponentLog logger;

        public Builder clientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder apiUrl(final String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public Builder repoOwner(final String repoOwner) {
            this.repoOwner = repoOwner;
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

        public Builder accessToken(final String accessToken) {
            this.accessToken = accessToken;
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

        public GiteaRepositoryClient build() throws FlowRegistryException {
            return new GiteaRepositoryClient(this);
        }
    }
}
