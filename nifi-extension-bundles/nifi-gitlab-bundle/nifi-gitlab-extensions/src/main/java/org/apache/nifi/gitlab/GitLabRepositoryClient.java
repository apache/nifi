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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.registry.flow.git.client.GitRepositoryClient;
import org.gitlab4j.api.CommitsApi;
import org.gitlab4j.api.GitLabApi;
import org.gitlab4j.api.GitLabApiException;
import org.gitlab4j.api.ProjectApi;
import org.gitlab4j.api.RepositoryApi;
import org.gitlab4j.api.models.AccessLevel;
import org.gitlab4j.api.models.Branch;
import org.gitlab4j.api.models.Commit;
import org.gitlab4j.api.models.CommitAction;
import org.gitlab4j.api.models.Permissions;
import org.gitlab4j.api.models.Project;
import org.gitlab4j.api.models.ProjectAccess;
import org.gitlab4j.api.models.RepositoryFile;
import org.gitlab4j.api.models.TreeItem;
import org.gitlab4j.models.Constants;
import org.gitlab4j.models.Constants.Encoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implementation of {@link GitRepositoryClient} for GitLab.
 */
public class GitLabRepositoryClient implements GitRepositoryClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(GitLabRepositoryClient.class);

    private static final String PERSONAL_ACCESS_TOKENS_SELF_PATH = "/personal_access_tokens/self";
    private static final String PRIVATE_TOKEN_HEADER  = "PRIVATE-TOKEN";
    private static final String READ_API_SCOPE = "read_api";
    private static final String WRITE_API_SCOPE = "api";
    private static final String DIRECTORY_MODE = "040000";

    private static final int DEFAULT_ITEMS_PER_PAGE = 100;

    private static final TokenInfo UNKNOWN_TOKEN = new TokenInfo("unknown", false, false);

    private final ObjectMapper objectMapper = JsonMapper.builder()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .build();

    private final String clientId;
    private final String repoNamespace;
    private final String repoName;
    private final String repoPath;
    private final String projectPath;

    private final int connectTimeout;
    private final int readTimeout;

    private final GitLabApi gitLab;
    private final boolean canRead;
    private final boolean canWrite;

    private GitLabRepositoryClient(final Builder builder) throws FlowRegistryException {
        final String apiUrl = Objects.requireNonNull(builder.apiUrl, "API URL is required");
        final GitLabApi.ApiVersion apiVersion = Objects.requireNonNull(builder.apiVersion, "API version is required");
        final GitLabAuthenticationType authenticationType = Objects.requireNonNull(builder.authenticationType, "Authentication type is required");
        final String authToken = Objects.requireNonNull(builder.authToken, "Authentication token is required");

        final Constants.TokenType tokenType = authenticationType == GitLabAuthenticationType.ACCESS_TOKEN ? Constants.TokenType.ACCESS : null;

        clientId = Objects.requireNonNull(builder.clientId, "Client Id is required");
        repoNamespace = Objects.requireNonNull(builder.repoNamespace, "Repository Group is required");
        repoName = Objects.requireNonNull(builder.repoName, "Repository Name is required");
        repoPath = builder.repoPath;
        projectPath = repoNamespace + "/" + repoName;

        connectTimeout = builder.connectTimeout;
        readTimeout = builder.readTimeout;

        gitLab = new GitLabApi(apiVersion, apiUrl, tokenType, authToken);
        gitLab.setRequestTimeout(builder.connectTimeout, builder.readTimeout);
        gitLab.setDefaultPerPage(DEFAULT_ITEMS_PER_PAGE);

        // Get the info for the supplied token which determines the API access level
        final TokenInfo tokenInfo = getTokenInfo();

        // Retrieve the project or throws an exception if not found
        final Optional<Project> projectOptional = getProject();
        if (projectOptional.isPresent()) {
            // Project was successfully retrieved so must have read access, use returned access level + api permissions to determine write permissions
            final Project project = projectOptional.get();
            canRead = true;
            canWrite = tokenInfo.canWriteApi() && hasMinimumAccessLevel(project, AccessLevel.DEVELOPER);
        } else {
            // Couldn't retrieve project so can't read or write
            canRead = false;
            canWrite = false;
        }

        LOGGER.info("Created {} for clientId = [{}], repository [{}], canRead [{}], canWrite [{}]", getClass().getSimpleName(), clientId, projectPath, canRead, canWrite);
    }

    public String getRepoNamespace() {
        return repoNamespace;
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
    public Set<String> getBranches() throws FlowRegistryException {
        LOGGER.debug("Getting branches for repository [{}]", projectPath);
        final RepositoryApi repositoryApi = gitLab.getRepositoryApi();
        return execute(() -> repositoryApi.getBranchesStream(projectPath)
                .map(Branch::getName)
                .collect(Collectors.toSet())
        );
    }

    @Override
    public Set<String> getTopLevelDirectoryNames(final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath("");
        LOGGER.debug("Getting top-level directories for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, projectPath);

        final RepositoryApi repositoryApi = gitLab.getRepositoryApi();
        return execute(() -> repositoryApi.getTreeStream(projectPath, resolvedPath, branch)
                .filter(this::isDirectory)
                .map(TreeItem::getName)
                .collect(Collectors.toSet())
        );
    }

    @Override
    public Set<String> getFileNames(final String directory, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(directory);
        LOGGER.debug("Getting filenames for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, projectPath);

        final RepositoryApi repositoryApi = gitLab.getRepositoryApi();
        return execute(() -> repositoryApi.getTreeStream(projectPath, resolvedPath, branch)
                .filter(this::isFile)
                .map(TreeItem::getName)
                .collect(Collectors.toSet())
        );
    }

    @Override
    public List<GitCommit> getCommits(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting commits for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, projectPath);

        final CommitsApi commitsApi = gitLab.getCommitsApi();
        return execute(() -> commitsApi.getCommits(projectPath, branch, resolvedPath).stream()
                .map(this::toGitCommit)
                .toList()
        );
    }

    @Override
    public InputStream getContentFromBranch(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting content for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, projectPath);
        return execute(() -> gitLab.getRepositoryFileApi().getRawFile(projectPath, branch, resolvedPath));
    }

    @Override
    public InputStream getContentFromCommit(final String path, final String commitSha) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting content for path [{}] from commit [{}] in repository [{}]", resolvedPath, commitSha, projectPath);
        return execute(() -> gitLab.getRepositoryFileApi().getRawFile(projectPath, commitSha, resolvedPath));
    }

    @Override
    public Optional<String> getContentSha(final String path, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(path);
        LOGGER.debug("Getting content SHA for path [{}] on branch [{}] in repository [{}]", resolvedPath, branch, projectPath);
        return execute(() -> gitLab.getRepositoryFileApi().getOptionalFileInfo(projectPath, resolvedPath, branch).map(RepositoryFile::getCommitId));
    }

    @Override
    public String createContent(final GitCreateContentRequest request) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(request.getPath());
        final String branch = request.getBranch();
        LOGGER.debug("Creating content at path [{}] on branch [{}] in repository [{}] ", resolvedPath, branch, projectPath);

        return execute(() -> {
            final Optional<RepositoryFile> existingFileInfo = gitLab.getRepositoryFileApi().getOptionalFileInfo(projectPath, resolvedPath, branch);

            // Create commit action
            final CommitAction commitAction = new CommitAction();
            commitAction.setAction(existingFileInfo.isPresent() ? CommitAction.Action.UPDATE : CommitAction.Action.CREATE);
            commitAction.setFilePath(resolvedPath);
            commitAction.setEncoding(Encoding.BASE64);

            // Encode content to Base64
            final String encodedContent = Base64.getEncoder().encodeToString(request.getContent().getBytes(StandardCharsets.UTF_8));
            commitAction.setContent(encodedContent);

            // Create the commit
            final Commit commit = gitLab.getCommitsApi()
                    .createCommit(
                            projectPath,
                            branch,
                            request.getMessage(),
                            null, // start_branch - null means use the branch parameter
                            null, // author_email - null means use the authenticated user
                            null, // author_name - null means use the authenticated user
                            List.of(commitAction));

            final String commitId = commit.getId();
            LOGGER.debug("Successfully committed file [{}] with commit ID: {}", resolvedPath, commit.getId());

            return commitId;
        });
    }

    @Override
    public InputStream deleteContent(final String filePath, final String commitMessage, final String branch) throws FlowRegistryException {
        final String resolvedPath = getResolvedPath(filePath);
        LOGGER.debug("Deleting content at path [{}] on branch [{}] in repository [{}] ", resolvedPath, branch, projectPath);
        return execute(() -> {
            final InputStream content = gitLab.getRepositoryFileApi().getRawFile(projectPath, branch, resolvedPath);
            gitLab.getRepositoryFileApi().deleteFile(projectPath, resolvedPath, branch, commitMessage);
            return content;
        });
    }

    @Override
    public void close() {
        gitLab.close();
    }

    private Optional<Project> getProject() throws FlowRegistryException {
        try {
            final ProjectApi projectApi = gitLab.getProjectApi();
            final Project project = projectApi.getProject(repoNamespace, repoName);
            LOGGER.debug("Successfully retrieved project [{}] for client [{}]", projectPath, clientId);
            return Optional.of(project);
        } catch (final GitLabApiException e) {
            if (e.getHttpStatus() == HttpURLConnection.HTTP_UNAUTHORIZED) {
                LOGGER.warn("Client [{}] does not have permissions to access repository [{}]", clientId, projectPath);
                return Optional.empty();
            } else if (e.getHttpStatus() == HttpURLConnection.HTTP_NOT_FOUND) {
                throw new FlowRegistryException(String.format("Repository [%s] not found", projectPath), e);
            } else {
                throw new FlowRegistryException(e.getMessage(), e);
            }
        }
    }

    private boolean hasMinimumAccessLevel(final Project project, final AccessLevel accessLevel) {
        final Permissions permissions = project.getPermissions();
        LOGGER.debug("Checking if client [{}] has access level [{}] in project [{}]", clientId, accessLevel.name(), projectPath);

        final ProjectAccess projectAccess = permissions.getProjectAccess();
        if (projectAccess != null) {
            final AccessLevel projectAccessLevel = projectAccess.getAccessLevel();
            LOGGER.debug("Client [{}] has project access level [{}] for project [{}]", clientId, projectAccessLevel.name(), projectPath);
            if (projectAccessLevel.toValue() >= accessLevel.toValue()) {
                return true;
            }
        }

        final ProjectAccess groupAccess = permissions.getGroupAccess();
        if (groupAccess != null) {
            final AccessLevel groupAccessLevel = groupAccess.getAccessLevel();
            LOGGER.debug("Client [{}] has group access level [{}] for project [{}]", clientId, groupAccessLevel.name(), projectPath);
            if (groupAccessLevel.toValue() >= accessLevel.toValue()) {
                return true;
            }
        }

        LOGGER.debug("Client [{}] does not have minimum access level [{}] for project [{}]", clientId, accessLevel.name(), projectPath);
        return false;
    }

    private TokenInfo getTokenInfo() {
        final TokenInfo tokenInfo = getPersonalAccessToken().map(this::createTokenInfo).orElse(UNKNOWN_TOKEN);
        LOGGER.debug("Created token info {} for client [{}]", tokenInfo, clientId);
        return tokenInfo;
    }

    private TokenInfo createTokenInfo(final PersonalAccessToken personalAccessToken) {
        final Set<String> tokenScopes = new HashSet<>(personalAccessToken.scopes());
        final boolean canReadApi = tokenScopes.contains(READ_API_SCOPE) || tokenScopes.contains(WRITE_API_SCOPE);
        final boolean canWriteApi = tokenScopes.contains(WRITE_API_SCOPE);
        return new TokenInfo(personalAccessToken.name(), canReadApi, canWriteApi);
    }

    private Optional<PersonalAccessToken> getPersonalAccessToken() {
        try {
            final PersonalAccessToken personalAccessToken = retrievePersonalAccessToken();
            return Optional.of(personalAccessToken);
        } catch (final FlowRegistryException e) {
            LOGGER.warn("Failed to get personal access token for client [{}]", clientId, e);
            return Optional.empty();
        }
    }

    private PersonalAccessToken retrievePersonalAccessToken() throws FlowRegistryException {
        final int responseCode;
        final String responseContent;
        HttpURLConnection connection = null;
        try {
            connection = createConnection(PERSONAL_ACCESS_TOKENS_SELF_PATH);
            responseCode = connection.getResponseCode();
            responseContent = IOUtils.toString(connection.getInputStream(), StandardCharsets.UTF_8);
        } catch (final Exception e) {
            throw new FlowRegistryException("Unable to retrieve personal access token details", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }

        LOGGER.debug("Personal access token response code = {}", responseCode);
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new FlowRegistryException("Unable to retrieve personal access token details: " + responseContent);
        }

        try {
            return objectMapper.readValue(responseContent, PersonalAccessToken.class);
        } catch (final IOException e) {
            throw new FlowRegistryException("Unable to parse personal access token details", e);
        }
    }

    private HttpURLConnection createConnection(final String subPath) throws IOException {
        final String gitLabServerUrl = gitLab.getGitLabServerUrl().endsWith("/") ? gitLab.getGitLabServerUrl().replaceAll("/$", "") : gitLab.getGitLabServerUrl();
        final URL gitLabApiUrl = URI.create(gitLabServerUrl + gitLab.getApiVersion().getApiNamespace() + subPath).toURL();
        LOGGER.debug("Connecting to GitLab URL [{}] for client [{}]", gitLabApiUrl, clientId);

        final HttpURLConnection connection = (HttpURLConnection) gitLabApiUrl.openConnection();
        connection.setRequestProperty(PRIVATE_TOKEN_HEADER, gitLab.getAuthToken());
        connection.setConnectTimeout(connectTimeout);
        connection.setReadTimeout(readTimeout);
        return connection;
    }

    private String getResolvedPath(final String path) {
        return repoPath == null ? path : repoPath + "/" + path;
    }

    private boolean isDirectory(final TreeItem item) {
        return item.getMode().equals(DIRECTORY_MODE);
    }

    private boolean isFile(final TreeItem item) {
        return !isDirectory(item);
    }

    private GitCommit toGitCommit(final Commit commit) {
        return new GitCommit(
                commit.getId(),
                commit.getAuthorName(),
                commit.getMessage(),
                Instant.ofEpochMilli(commit.getCommittedDate().getTime())
        );
    }

    /**
     * Functional interface for making a request to GitLab which may throw GitLabApiException.
     *
     * @param <T> the result of the request
     */
    private interface GitLabRequest<T> {

        T execute() throws GitLabApiException, FlowRegistryException;

    }

    private <T> T execute(final GitLabRequest<T> action) throws FlowRegistryException {
        try {
            return action.execute();
        } catch (final GitLabApiException e) {
            switch (e.getHttpStatus()) {
                case HttpURLConnection.HTTP_UNAUTHORIZED -> throw new FlowRegistryException("Client does not have permission to perform the given action", e);
                case HttpURLConnection.HTTP_NOT_FOUND -> throw new FlowRegistryException("Path or Branch not found", e);
                default -> throw new FlowRegistryException(e.getMessage(), e);
            }
        }
    }

    /**
     * Holder for information about the permissions of the provided token.
     */
    private record TokenInfo(String name, boolean canReadApi, boolean canWriteApi) {
        @Override
        public String toString() {
            return new ToStringBuilder(this)
                    .append("name", name)
                    .append("canReadApi", canReadApi)
                    .append("canWriteApi", canWriteApi)
                    .toString();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String clientId;
        private String apiUrl;
        private GitLabApi.ApiVersion apiVersion;
        private GitLabAuthenticationType authenticationType;
        private String authToken;
        private String repoNamespace;
        private String repoName;
        private String repoPath;
        private int connectTimeout;
        private int readTimeout;

        public Builder clientId(final String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder apiUrl(final String apiUrl) {
            this.apiUrl = apiUrl;
            return this;
        }

        public Builder apiVersion(final GitLabApi.ApiVersion apiVersion) {
            this.apiVersion = apiVersion;
            return this;
        }

        public Builder authenticationType(final GitLabAuthenticationType authenticationType) {
            this.authenticationType = authenticationType;
            return this;
        }

        public Builder authToken(final String authToken) {
            this.authToken = authToken;
            return this;
        }

        public Builder repoNamespace(final String repoNamespace) {
            this.repoNamespace = repoNamespace;
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

        public Builder connectTimeout(final int connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder readTimeout(final int readTimeout) {
            this.readTimeout = readTimeout;
            return this;
        }

        public GitLabRepositoryClient build() throws FlowRegistryException {
            return new GitLabRepositoryClient(this);
        }
    }
}
