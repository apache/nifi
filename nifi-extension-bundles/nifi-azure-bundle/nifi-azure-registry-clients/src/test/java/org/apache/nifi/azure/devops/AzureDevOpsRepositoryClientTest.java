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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.oauth2.AccessToken;
import org.apache.nifi.oauth2.OAuth2AccessTokenProvider;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.web.client.api.HttpRequestBodySpec;
import org.apache.nifi.web.client.api.HttpRequestHeadersSpec;
import org.apache.nifi.web.client.api.HttpRequestUriSpec;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class AzureDevOpsRepositoryClientTest {

    private static final String BRANCH = "main";
    private static final String FILE_PATH = "flows/test.json";
    private static final String CONTENT = "{\"flow\":\"content\"}";
    private static final String MESSAGE = "test commit";
    private static final String EXPECTED_FILE_COMMIT = "file-commit-sha";
    private static final String BRANCH_HEAD_SHA = "branch-head-sha";
    private static final String NEW_COMMIT_SHA = "new-commit-sha";

    @Mock
    private WebClientServiceProvider webClientProvider;

    @Mock
    private WebClientService webClientService;

    @Mock
    private OAuth2AccessTokenProvider tokenProvider;

    @Mock
    private ComponentLog logger;

    private HttpUriBuilder uriBuilder;
    private OngoingStubbing<HttpResponseEntity> getStubbing;
    private OngoingStubbing<HttpResponseEntity> postStubbing;

    @BeforeEach
    void setUp() {
        final AccessToken accessToken = new AccessToken("test-token", null, "Bearer", 3600L, null);
        lenient().when(tokenProvider.getAccessDetails()).thenReturn(accessToken);
        lenient().when(webClientProvider.getWebClientService()).thenReturn(webClientService);
    }

    private AzureDevOpsRepositoryClient buildClient() throws FlowRegistryException {
        setupUriBuilder();
        return AzureDevOpsRepositoryClient.builder()
                .clientId("test-client")
                .apiUrl("https://dev.azure.com")
                .organization("test-org")
                .project("test-project")
                .repoName("test-repo")
                .oauthService(tokenProvider)
                .webClient(webClientProvider)
                .logger(logger)
                .build();
    }

    private void setupUriBuilder() {
        uriBuilder = mock(HttpUriBuilder.class);
        lenient().when(webClientProvider.getHttpUriBuilder()).thenReturn(uriBuilder);
        lenient().when(uriBuilder.scheme(anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.host(anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.port(any(int.class))).thenReturn(uriBuilder);
        lenient().when(uriBuilder.addPathSegment(anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.addQueryParameter(anyString(), anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.build()).thenReturn(URI.create("https://dev.azure.com/test"));
    }

    private void stubGetChain(final HttpResponseEntity... responses) {
        final HttpRequestUriSpec getSpec = mock(HttpRequestUriSpec.class);
        final HttpRequestBodySpec bodySpec = mock(HttpRequestBodySpec.class);
        lenient().when(webClientService.get()).thenReturn(getSpec);
        lenient().when(getSpec.uri(any(URI.class))).thenReturn(bodySpec);
        lenient().when(bodySpec.header(anyString(), anyString())).thenReturn(bodySpec);

        getStubbing = when(bodySpec.retrieve());
        for (final HttpResponseEntity response : responses) {
            getStubbing = getStubbing.thenReturn(response);
        }
    }

    private void stubPostChain(final HttpResponseEntity... responses) {
        final HttpRequestUriSpec postSpec = mock(HttpRequestUriSpec.class);
        final HttpRequestBodySpec postBodySpec = mock(HttpRequestBodySpec.class);
        final HttpRequestHeadersSpec afterBody = mock(HttpRequestHeadersSpec.class);
        lenient().when(webClientService.post()).thenReturn(postSpec);
        lenient().when(postSpec.uri(any(URI.class))).thenReturn(postBodySpec);
        lenient().when(postBodySpec.header(anyString(), anyString())).thenReturn(postBodySpec);
        lenient().when(postBodySpec.body(anyString())).thenReturn(afterBody);
        lenient().when(afterBody.header(anyString(), anyString())).thenReturn(postBodySpec);

        postStubbing = when(afterBody.retrieve());
        for (final HttpResponseEntity response : responses) {
            postStubbing = postStubbing.thenReturn(response);
        }
    }

    private HttpResponseEntity mockResponse(final int statusCode, final String body) {
        final HttpResponseEntity response = mock(HttpResponseEntity.class);
        lenient().when(response.statusCode()).thenReturn(statusCode);
        lenient().when(response.body()).thenReturn(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
        return response;
    }

    private HttpResponseEntity repoInfoResponse() {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"project\":{\"id\":\"proj-id\"},\"id\":\"repo-id\"}");
    }

    private HttpResponseEntity permissionsResponse() {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"value\":[true]}");
    }

    private HttpResponseEntity contentShaResponse(final String commitId) {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"value\":[{\"commitId\":\"%s\"}]}".formatted(commitId));
    }

    private HttpResponseEntity contentShaNotFoundResponse() {
        return mockResponse(HttpURLConnection.HTTP_NOT_FOUND, "{}");
    }

    private HttpResponseEntity branchHeadResponse(final String objectId) {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"value\":[{\"objectId\":\"%s\"}]}".formatted(objectId));
    }

    private HttpResponseEntity pushSuccessResponse(final String commitId) {
        return mockResponse(HttpURLConnection.HTTP_CREATED, "{\"commits\":[{\"commitId\":\"%s\"}]}".formatted(commitId));
    }

    private HttpResponseEntity conflictResponse() {
        return mockResponse(HttpURLConnection.HTTP_CONFLICT, "{\"message\":\"conflict\"}");
    }

    private HttpResponseEntity serverErrorResponse() {
        return mockResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, "{\"message\":\"internal error\"}");
    }

    private GitCreateContentRequest createRequest(final String existingContentSha, final String expectedCommitSha) {
        final GitCreateContentRequest.Builder builder = GitCreateContentRequest.builder()
                .branch(BRANCH)
                .path(FILE_PATH)
                .content(CONTENT)
                .message(MESSAGE);
        if (existingContentSha != null) {
            builder.existingContentSha(existingContentSha);
        }
        if (expectedCommitSha != null) {
            builder.expectedCommitSha(expectedCommitSha);
        }
        return builder.build();
    }

    @Test
    void testCreateContentSuccess() throws FlowRegistryException {
        stubGetChain(
                repoInfoResponse(),
                permissionsResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA)
        );
        stubPostChain(pushSuccessResponse(NEW_COMMIT_SHA));

        final AzureDevOpsRepositoryClient client = buildClient();
        final String commitSha = client.createContent(createRequest("existing-sha", null));
        assertEquals(NEW_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateContentRetryOn409ThenSuccess() throws FlowRegistryException {
        stubGetChain(
                repoInfoResponse(),
                permissionsResponse(),
                contentShaResponse(EXPECTED_FILE_COMMIT),
                branchHeadResponse(BRANCH_HEAD_SHA),
                contentShaResponse(EXPECTED_FILE_COMMIT),
                branchHeadResponse("new-branch-head")
        );
        stubPostChain(conflictResponse(), pushSuccessResponse(NEW_COMMIT_SHA));

        final AzureDevOpsRepositoryClient client = buildClient();
        final String commitSha = client.createContent(createRequest("existing-sha", EXPECTED_FILE_COMMIT));
        assertEquals(NEW_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateContentFileLevelConflict() throws FlowRegistryException {
        stubGetChain(
                repoInfoResponse(),
                permissionsResponse(),
                contentShaResponse("different-commit-sha")
        );

        final AzureDevOpsRepositoryClient client = buildClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createContent(createRequest("existing-sha", EXPECTED_FILE_COMMIT)));
        assertTrue(exception.getMessage().contains("has been modified by another commit"));
    }

    @Test
    void testCreateContentMaxRetriesExhausted() throws FlowRegistryException {
        stubGetChain(
                repoInfoResponse(),
                permissionsResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA),
                branchHeadResponse("head-2"),
                branchHeadResponse("head-3")
        );
        stubPostChain(conflictResponse(), conflictResponse(), conflictResponse());

        final AzureDevOpsRepositoryClient client = buildClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createContent(createRequest("existing-sha", null)));
        assertTrue(exception.getMessage().contains("Push failed after 3 attempts"));
    }

    @Test
    void testCreateContentNon409ErrorNotRetried() throws FlowRegistryException {
        stubGetChain(
                repoInfoResponse(),
                permissionsResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA)
        );
        stubPostChain(serverErrorResponse());

        final AzureDevOpsRepositoryClient client = buildClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createContent(createRequest("existing-sha", null)));
        assertTrue(exception.getMessage().contains("failed"));
    }

    @Test
    void testCreateContentNullExpectedCommitSha() throws FlowRegistryException {
        stubGetChain(
                repoInfoResponse(),
                permissionsResponse(),
                contentShaNotFoundResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA)
        );
        stubPostChain(pushSuccessResponse(NEW_COMMIT_SHA));

        final AzureDevOpsRepositoryClient client = buildClient();
        final String commitSha = client.createContent(createRequest(null, null));
        assertEquals(NEW_COMMIT_SHA, commitSha);
    }

    @Test
    void testDeleteContentUsesFetchBranchHead() throws FlowRegistryException, IOException {
        stubGetChain(
                repoInfoResponse(),
                permissionsResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA)
        );
        final HttpResponseEntity deleteResponse = mockResponse(HttpURLConnection.HTTP_CREATED,
                "{\"commits\":[{\"commitId\":\"delete-commit\"}]}");
        stubPostChain(deleteResponse);

        final AzureDevOpsRepositoryClient client = buildClient();
        try (final InputStream result = client.deleteContent(FILE_PATH, MESSAGE, BRANCH)) {
            final String body = new String(result.readAllBytes(), StandardCharsets.UTF_8);
            assertTrue(body.contains("delete-commit"));
        }
    }

    @Test
    void testCreateBranchSuccess() throws FlowRegistryException, IOException {
        final HttpResponseEntity emptyRefsResponse = mockResponse(HttpURLConnection.HTTP_OK, "{\"value\":[]}");
        stubGetChain(repoInfoResponse(), permissionsResponse(), emptyRefsResponse, branchHeadResponse(BRANCH_HEAD_SHA));
        stubPostChain(mockResponse(HttpURLConnection.HTTP_OK, "{\"value\":[{\"name\":\"refs/heads/feature\"}]}"));

        final AzureDevOpsRepositoryClient client = buildClient();
        client.createBranch("feature", "main", Optional.empty());
    }

    @Test
    void testCreateBranchWithSourceCommitSha() throws FlowRegistryException, IOException {
        final HttpResponseEntity emptyRefsResponse = mockResponse(HttpURLConnection.HTTP_OK, "{\"value\":[]}");
        stubGetChain(repoInfoResponse(), permissionsResponse(), emptyRefsResponse);
        stubPostChain(mockResponse(HttpURLConnection.HTTP_OK, "{\"value\":[{\"name\":\"refs/heads/feature\"}]}"));

        final AzureDevOpsRepositoryClient client = buildClient();
        client.createBranch("feature", "main", Optional.of("abc123"));
    }

    @Test
    void testCreateBranchAlreadyExists() throws FlowRegistryException {
        final HttpResponseEntity existingBranchResponse = mockResponse(HttpURLConnection.HTTP_OK,
                "{\"value\":[{\"name\":\"refs/heads/feature\",\"objectId\":\"abc123\"}]}");
        stubGetChain(repoInfoResponse(), permissionsResponse(), existingBranchResponse);

        final AzureDevOpsRepositoryClient client = buildClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createBranch("feature", "main", Optional.empty()));
        assertTrue(exception.getMessage().contains("already exists"));
    }

    @Test
    void testCreateBranchBlankNameRejected() throws FlowRegistryException {
        stubGetChain(repoInfoResponse(), permissionsResponse());
        final AzureDevOpsRepositoryClient client = buildClient();
        assertThrows(IllegalArgumentException.class, () -> client.createBranch("  ", "main", Optional.empty()));
    }

    @Test
    void testCreateBranchBlankSourceRejected() throws FlowRegistryException {
        stubGetChain(repoInfoResponse(), permissionsResponse());
        final AzureDevOpsRepositoryClient client = buildClient();
        assertThrows(IllegalArgumentException.class, () -> client.createBranch("feature", "  ", Optional.empty()));
    }
}
