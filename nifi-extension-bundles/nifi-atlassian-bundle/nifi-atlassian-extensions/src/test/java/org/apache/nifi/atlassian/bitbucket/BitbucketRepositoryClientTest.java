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

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
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
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class BitbucketRepositoryClientTest {

    private static final String BRANCH = "main";
    private static final String FILE_PATH = "flows/test.json";
    private static final String CONTENT = "{\"flow\":\"content\"}";
    private static final String MESSAGE = "test commit";
    private static final String EXPECTED_FILE_COMMIT = "file-commit-sha";
    private static final String BRANCH_HEAD_SHA = "branch-head-sha";
    private static final String RESULT_COMMIT_SHA = "result-commit-sha";

    @Mock
    private WebClientServiceProvider webClientProvider;

    @Mock
    private WebClientService webClientService;

    @Mock
    private ComponentLog logger;

    private HttpUriBuilder uriBuilder;

    private HttpRequestBodySpec getBodySpec;
    private HttpRequestBodySpec postAfterHeaders;

    private HttpRequestBodySpec postBodySpec;

    @BeforeEach
    void setUp() {
        lenient().when(webClientProvider.getWebClientService()).thenReturn(webClientService);
        setupUriBuilder();
    }

    private void setupUriBuilder() {
        uriBuilder = mock(HttpUriBuilder.class);
        lenient().when(webClientProvider.getHttpUriBuilder()).thenReturn(uriBuilder);
        lenient().when(uriBuilder.scheme(anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.host(anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.port(any(int.class))).thenReturn(uriBuilder);
        lenient().when(uriBuilder.addPathSegment(anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.addQueryParameter(anyString(), anyString())).thenReturn(uriBuilder);
        lenient().when(uriBuilder.build()).thenReturn(URI.create("https://api.bitbucket.org/test"));
    }

    private BitbucketRepositoryClient buildCloudClient() throws FlowRegistryException {
        return BitbucketRepositoryClient.builder()
                .clientId("test-client")
                .apiUrl("https://api.bitbucket.org")
                .formFactor(BitbucketFormFactor.CLOUD)
                .authenticationType(BitbucketAuthenticationType.ACCESS_TOKEN)
                .accessToken("test-token")
                .workspace("test-workspace")
                .repoName("test-repo")
                .webClient(webClientProvider)
                .logger(logger)
                .build();
    }

    private void stubGetChain(final HttpResponseEntity... responses) {
        final HttpRequestUriSpec getSpec = mock(HttpRequestUriSpec.class);
        getBodySpec = mock(HttpRequestBodySpec.class);
        lenient().when(webClientService.get()).thenReturn(getSpec);
        lenient().when(getSpec.uri(any(URI.class))).thenReturn(getBodySpec);
        lenient().when(getBodySpec.header(anyString(), anyString())).thenReturn(getBodySpec);

        OngoingStubbing<HttpResponseEntity> stubbing = when(getBodySpec.retrieve());
        for (final HttpResponseEntity response : responses) {
            stubbing = stubbing.thenReturn(response);
        }
    }

    private void stubPostChain(final HttpResponseEntity... responses) {
        final HttpRequestUriSpec postSpec = mock(HttpRequestUriSpec.class);
        postBodySpec = mock(HttpRequestBodySpec.class);
        final HttpRequestHeadersSpec afterBody = mock(HttpRequestHeadersSpec.class);
        postAfterHeaders = mock(HttpRequestBodySpec.class);
        lenient().when(webClientService.post()).thenReturn(postSpec);
        lenient().when(postSpec.uri(any(URI.class))).thenReturn(postBodySpec);
        lenient().when(postBodySpec.header(anyString(), anyString())).thenReturn(postBodySpec);
        lenient().when(postBodySpec.body(any(InputStream.class), any(OptionalLong.class))).thenReturn(afterBody);
        lenient().when(postBodySpec.body(anyString())).thenReturn(afterBody);
        lenient().when(afterBody.header(anyString(), anyString())).thenReturn(postAfterHeaders);
        lenient().when(postAfterHeaders.header(anyString(), anyString())).thenReturn(postAfterHeaders);

        OngoingStubbing<HttpResponseEntity> stubbing = lenient().when(postAfterHeaders.retrieve());
        for (final HttpResponseEntity response : responses) {
            stubbing = stubbing.thenReturn(response);
        }

        OngoingStubbing<HttpResponseEntity> directStubbing = lenient().when(afterBody.retrieve());
        for (final HttpResponseEntity response : responses) {
            directStubbing = directStubbing.thenReturn(response);
        }
    }

    private HttpResponseEntity mockResponse(final int statusCode, final String body) {
        return mockResponse(statusCode, body, "application/json");
    }

    private HttpResponseEntity mockResponse(final int statusCode, final String body, final String contentType) {
        final HttpResponseEntity response = mock(HttpResponseEntity.class);
        final HttpEntityHeaders headers = mock(HttpEntityHeaders.class);
        lenient().when(headers.getFirstHeader("Content-Type")).thenReturn(Optional.of(contentType));
        lenient().when(response.headers()).thenReturn(headers);
        lenient().when(response.statusCode()).thenReturn(statusCode);
        lenient().when(response.body()).thenReturn(new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8)));
        return response;
    }

    private HttpResponseEntity branchListResponse() {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"values\":[{\"name\":\"main\"}]}");
    }

    private HttpResponseEntity branchHeadResponse(final String hash) {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"target\":{\"hash\":\"%s\"}}".formatted(hash));
    }

    private HttpResponseEntity commitsCheckResponse() {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"values\":[{\"hash\":\"some-hash\"}]}");
    }

    private HttpResponseEntity commitsForPathResponse(final String hash) {
        return mockResponse(HttpURLConnection.HTTP_OK, "{\"values\":[{\"hash\":\"%s\"}]}".formatted(hash));
    }

    private HttpResponseEntity conflictResponse() {
        return mockResponse(HttpURLConnection.HTTP_CONFLICT, "{\"error\":{\"message\":\"conflict\"}}");
    }

    private HttpResponseEntity createdResponse() {
        return mockResponse(HttpURLConnection.HTTP_CREATED, "{}");
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
    void testCreateContentCloudSuccess() throws FlowRegistryException, IOException {
        stubGetChain(
                branchListResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA),
                commitsCheckResponse(),
                commitsForPathResponse(RESULT_COMMIT_SHA)
        );
        stubPostChain(createdResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        final String commitSha = client.createContent(createRequest("existing-sha", null));
        assertEquals(RESULT_COMMIT_SHA, commitSha);

        final ArgumentCaptor<InputStream> requestBody = ArgumentCaptor.forClass(InputStream.class);
        final ArgumentCaptor<OptionalLong> contentLength = ArgumentCaptor.forClass(OptionalLong.class);
        verify(postBodySpec).body(requestBody.capture(), contentLength.capture());
        assertTrue(contentLength.getValue().isPresent());
        assertTrue(contentLength.getValue().getAsLong() > CONTENT.length());
        final String multipartBody = new String(requestBody.getValue().readAllBytes(), StandardCharsets.UTF_8);
        assertTrue(multipartBody.contains("name=\"%s\"; filename=\"test.json\"".formatted(FILE_PATH)));
    }

    @Test
    void testCreateContentCloudRetryOn409() throws FlowRegistryException {
        stubGetChain(
                branchListResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA),
                branchHeadResponse("new-head"),
                commitsCheckResponse(),
                commitsForPathResponse(RESULT_COMMIT_SHA)
        );
        stubPostChain(conflictResponse(), createdResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        final String commitSha = client.createContent(createRequest("existing-sha", null));
        assertEquals(RESULT_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateContentCloudFileLevelConflict() throws FlowRegistryException {
        stubGetChain(
                branchListResponse(),
                commitsCheckResponse(),
                commitsForPathResponse("different-sha")
        );

        final BitbucketRepositoryClient client = buildCloudClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createContent(createRequest("existing-sha", EXPECTED_FILE_COMMIT)));
        assertTrue(exception.getMessage().contains("has been modified by another commit"));
    }

    @Test
    void testCreateContentCloudMaxRetriesExhausted() throws FlowRegistryException {
        stubGetChain(
                branchListResponse(),
                branchHeadResponse("head-1"),
                branchHeadResponse("head-2"),
                branchHeadResponse("head-3")
        );
        stubPostChain(conflictResponse(), conflictResponse(), conflictResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createContent(createRequest("existing-sha", null)));
        assertTrue(exception.getMessage().contains("Push failed after 3 attempts"));
    }

    @ParameterizedTest
    @MethodSource("nonJsonErrorResponses")
    void testCreateContentCloudNon409ErrorNotRetried(final String responseBody, final String expectedMessage, final String unexpectedMessage)
            throws FlowRegistryException {
        stubGetChain(
                branchListResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA)
        );
        stubPostChain(mockResponse(HttpURLConnection.HTTP_BAD_REQUEST, responseBody, "text/html"));

        final BitbucketRepositoryClient client = buildCloudClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createContent(createRequest("existing-sha", null)));
        assertTrue(exception.getMessage().contains("HTTP status 400"));
        assertTrue(exception.getMessage().contains(expectedMessage));
        assertFalse(exception.getMessage().contains(unexpectedMessage));
    }

    private static Stream<Arguments> nonJsonErrorResponses() {
        final String truncatedBody = "<html>" + "a".repeat(506);
        return Stream.of(
                Arguments.of("\n<html><body>Bad Request (400)</body></html>", "Content-Type [text/html], body [<html><body>Bad Request (400)</body></html>]", "Empty response"),
                Arguments.of("", "Empty response from Bitbucket API", "Non-JSON response"),
                Arguments.of(truncatedBody + "TRUNCATED", "body [%s]".formatted(truncatedBody), "TRUNCATED")
        );
    }

    @Test
    void testCreateContentCloudNullExpectedCommitSha() throws FlowRegistryException {
        stubGetChain(
                branchListResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA),
                commitsCheckResponse(),
                commitsForPathResponse(RESULT_COMMIT_SHA)
        );
        stubPostChain(createdResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        final String commitSha = client.createContent(createRequest(null, null));
        assertEquals(RESULT_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateContentDataCenterWithExpectedCommitSha() throws FlowRegistryException {
        stubDataCenterGetAndPutChains();

        final BitbucketRepositoryClient dcClient = buildDataCenterClient();
        final GitCreateContentRequest request = createRequest(null, EXPECTED_FILE_COMMIT);
        final String commitSha = dcClient.createContent(request);
        assertEquals(RESULT_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateContentDataCenterFallsBackToExistingContentSha() throws FlowRegistryException {
        stubDataCenterGetAndPutChains();

        final BitbucketRepositoryClient dcClient = buildDataCenterClient();
        final GitCreateContentRequest request = createRequest("existing-content-sha", null);
        final String commitSha = dcClient.createContent(request);
        assertEquals(RESULT_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateContentDataCenterPrefersExpectedCommitShaOverExistingContentSha() throws FlowRegistryException {
        stubDataCenterGetAndPutChains();

        final BitbucketRepositoryClient dcClient = buildDataCenterClient();
        final GitCreateContentRequest request = createRequest("existing-content-sha", EXPECTED_FILE_COMMIT);
        final String commitSha = dcClient.createContent(request);
        assertEquals(RESULT_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateContentDataCenterNoBothShas() throws FlowRegistryException {
        stubDataCenterGetAndPutChains();

        final BitbucketRepositoryClient dcClient = buildDataCenterClient();
        final GitCreateContentRequest request = createRequest(null, null);
        final String commitSha = dcClient.createContent(request);
        assertEquals(RESULT_COMMIT_SHA, commitSha);
    }

    @Test
    void testCreateBranchCloudSuccess() throws FlowRegistryException, IOException {
        stubGetChain(
                branchListResponse(),
                branchListResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA)
        );
        stubPostChain(createdResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        client.createBranch("feature", "main", Optional.empty());
    }

    @Test
    void testCreateBranchCloudWithCommitSha() throws FlowRegistryException, IOException {
        stubGetChain(branchListResponse(), branchListResponse());
        stubPostChain(createdResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        client.createBranch("feature", "main", Optional.of("abc123"));
    }

    @Test
    void testCreateBranchCloudAlreadyExists() throws FlowRegistryException {
        stubGetChain(branchListResponse(), branchListResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        final FlowRegistryException exception = assertThrows(FlowRegistryException.class,
                () -> client.createBranch("main", "main", Optional.empty()));
        assertTrue(exception.getMessage().contains("already exists"));
    }

    @Test
    void testCreateBranchBlankNameRejected() throws FlowRegistryException {
        stubGetChain(branchListResponse());
        final BitbucketRepositoryClient client = buildCloudClient();
        assertThrows(IllegalArgumentException.class, () -> client.createBranch("  ", "main", Optional.empty()));
    }

    @Test
    void testCreateBranchBlankSourceRejected() throws FlowRegistryException {
        stubGetChain(branchListResponse());
        final BitbucketRepositoryClient client = buildCloudClient();
        assertThrows(IllegalArgumentException.class, () -> client.createBranch("feature", "  ", Optional.empty()));
    }

    @Test
    void testGetBranchesCloudFollowsPagination() throws FlowRegistryException {
        final String firstPage = "{\"values\":[{\"name\":\"main\"},{\"name\":\"develop\"}],"
                + "\"next\":\"https://api.bitbucket.org/2.0/repositories/test-workspace/test-repo/refs/branches?page=2\"}";
        final String secondPage = "{\"values\":[{\"name\":\"feature-1\"},{\"name\":\"feature-2\"}]}";
        stubGetChain(
                branchListResponse(),
                mockResponse(HttpURLConnection.HTTP_OK, firstPage),
                mockResponse(HttpURLConnection.HTTP_OK, secondPage)
        );

        final BitbucketRepositoryClient client = buildCloudClient();
        final Set<String> branches = client.getBranches();

        assertEquals(Set.of("main", "develop", "feature-1", "feature-2"), branches);
    }

    private BitbucketRepositoryClient buildDataCenterClient() throws FlowRegistryException {
        return BitbucketRepositoryClient.builder()
                .clientId("test-client")
                .apiUrl("https://bitbucket.example.com")
                .formFactor(BitbucketFormFactor.DATA_CENTER)
                .authenticationType(BitbucketAuthenticationType.ACCESS_TOKEN)
                .accessToken("test-token")
                .projectKey("TEST")
                .repoName("test-repo")
                .webClient(webClientProvider)
                .logger(logger)
                .build();
    }

    private void stubDataCenterGetAndPutChains() {
        final HttpRequestUriSpec getSpec = mock(HttpRequestUriSpec.class);
        final HttpRequestBodySpec dcGetBody = mock(HttpRequestBodySpec.class);
        lenient().when(webClientService.get()).thenReturn(getSpec);
        lenient().when(getSpec.uri(any(URI.class))).thenReturn(dcGetBody);
        lenient().when(dcGetBody.header(anyString(), anyString())).thenReturn(dcGetBody);

        final HttpResponseEntity branchesResp = mockResponse(HttpURLConnection.HTTP_OK,
                "{\"values\":[{\"displayId\":\"main\"}],\"isLastPage\":true}");
        final HttpResponseEntity commitsCheckResp = mockResponse(HttpURLConnection.HTTP_OK,
                "{\"values\":[{\"id\":\"c1\"}],\"isLastPage\":true}");
        final HttpResponseEntity commitsResp = mockResponse(HttpURLConnection.HTTP_OK,
                "{\"values\":[{\"id\":\"%s\"}],\"isLastPage\":true}".formatted(RESULT_COMMIT_SHA));
        when(dcGetBody.retrieve()).thenReturn(branchesResp, commitsCheckResp, commitsResp);

        final HttpRequestUriSpec putSpec = mock(HttpRequestUriSpec.class);
        final HttpRequestBodySpec putBodySpec = mock(HttpRequestBodySpec.class);
        final HttpRequestHeadersSpec putAfterBody = mock(HttpRequestHeadersSpec.class);
        final HttpRequestBodySpec putAfterHeaders = mock(HttpRequestBodySpec.class);
        lenient().when(webClientService.put()).thenReturn(putSpec);
        lenient().when(putSpec.uri(any(URI.class))).thenReturn(putBodySpec);
        lenient().when(putBodySpec.body(any(InputStream.class), any(OptionalLong.class))).thenReturn(putAfterBody);
        lenient().when(putAfterBody.header(anyString(), anyString())).thenReturn(putAfterHeaders);
        lenient().when(putAfterHeaders.header(anyString(), anyString())).thenReturn(putAfterHeaders);
        final HttpResponseEntity putResponse = mockResponse(HttpURLConnection.HTTP_OK, "{}");
        lenient().when(putAfterHeaders.retrieve()).thenReturn(putResponse);
    }
}
