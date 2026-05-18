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
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.OptionalLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
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
        final HttpRequestBodySpec postBodySpec = mock(HttpRequestBodySpec.class);
        final HttpRequestHeadersSpec afterBody = mock(HttpRequestHeadersSpec.class);
        postAfterHeaders = mock(HttpRequestBodySpec.class);
        lenient().when(webClientService.post()).thenReturn(postSpec);
        lenient().when(postSpec.uri(any(URI.class))).thenReturn(postBodySpec);
        lenient().when(postBodySpec.body(any(InputStream.class), any(OptionalLong.class))).thenReturn(afterBody);
        lenient().when(afterBody.header(anyString(), anyString())).thenReturn(postAfterHeaders);
        lenient().when(postAfterHeaders.header(anyString(), anyString())).thenReturn(postAfterHeaders);

        OngoingStubbing<HttpResponseEntity> stubbing = when(postAfterHeaders.retrieve());
        for (final HttpResponseEntity response : responses) {
            stubbing = stubbing.thenReturn(response);
        }
    }

    private HttpResponseEntity mockResponse(final int statusCode, final String body) {
        final HttpResponseEntity response = mock(HttpResponseEntity.class);
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

    private HttpResponseEntity serverErrorResponse() {
        return mockResponse(HttpURLConnection.HTTP_INTERNAL_ERROR, "{\"error\":{\"message\":\"internal error\"}}");
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
    void testCreateContentCloudSuccess() throws FlowRegistryException {
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

    @Test
    void testCreateContentCloudNon409ErrorNotRetried() throws FlowRegistryException {
        stubGetChain(
                branchListResponse(),
                branchHeadResponse(BRANCH_HEAD_SHA)
        );
        stubPostChain(serverErrorResponse());

        final BitbucketRepositoryClient client = buildCloudClient();
        assertThrows(FlowRegistryException.class,
                () -> client.createContent(createRequest("existing-sha", null)));
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
