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
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.registry.flow.FlowRegistryException;
import org.apache.nifi.registry.flow.git.client.GitCommit;
import org.apache.nifi.registry.flow.git.client.GitCreateContentRequest;
import org.apache.nifi.web.client.StandardHttpUriBuilder;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.provider.api.WebClientServiceProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class GiteaRepositoryClientTest {

    private static final String OWNER = "nifi-owner";
    private static final String REPOSITORY = "nifi-flows";
    private static final String ACCESS_TOKEN = "access-token";
    private static final String BRANCH = "main";
    private static final String REPOSITORY_PATH = "/api/v1/repos/nifi-owner/nifi-flows";
    private static final String AUTHORIZATION_HEADER = "Authorization";
    private static final String TOTAL_COUNT_HEADER = "X-Total-Count";
    private static final String BLOB_SHA = "1111111111111111111111111111111111111111";
    private static final String COMMIT_SHA = "2222222222222222222222222222222222222222";
    private static final String FLOW_PATH = "bucket/flow.json";
    private static final String FLOW_CONTENT = "{\"flow\":\"content\"}";

    private static final String REPOSITORY_WRITABLE = """
            {"name":"nifi-flows","archived":false,"mirror":false,"permissions":{"admin":false,"push":true,"pull":true}}""";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Mock
    private WebClientServiceProvider webClientServiceProvider;

    @Mock
    private ComponentLog logger;

    private MockWebServer mockWebServer;

    private StandardWebClientService webClientService;

    @BeforeEach
    void startServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
        webClientService = new StandardWebClientService();
        lenient().when(webClientServiceProvider.getWebClientService()).thenReturn(webClientService);
        lenient().when(webClientServiceProvider.getHttpUriBuilder()).thenAnswer(invocation -> new StandardHttpUriBuilder());
    }

    @AfterEach
    void stopServer() {
        webClientService.close();
        mockWebServer.close();
    }

    @Test
    void testBuildPermissionsAndAuthorization() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);

        assertTrue(client.hasReadPermission());
        assertTrue(client.hasWritePermission());

        final RecordedRequest request = takeRequest();
        assertEquals("GET", request.getMethod());
        assertEquals(REPOSITORY_PATH, request.getTarget());
        assertEquals("token " + ACCESS_TOKEN, request.getHeaders().get(AUTHORIZATION_HEADER));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        """
        {"archived":true,"mirror":false,"permissions":{"push":true,"pull":true}}""",
        """
        {"archived":false,"mirror":true,"permissions":{"push":true,"pull":true}}""",
        """
        {"archived":false,"mirror":false,"permissions":{"push":false,"pull":true}}"""
    })
    void testBuildReadOnly(final String repository) throws Exception {
        final GiteaRepositoryClient client = buildClient(null, repository);

        assertTrue(client.hasReadPermission());
        assertFalse(client.hasWritePermission());
    }

    @Test
    void testBuildApiUrlContextPath() throws Exception {
        enqueue(HttpURLConnection.HTTP_OK, REPOSITORY_WRITABLE);
        final String apiUrl = mockWebServer.url("/gitea/api/v1/").toString();

        final GiteaRepositoryClient client = GiteaRepositoryClient.builder()
                .clientId("client")
                .apiUrl(apiUrl)
                .repoOwner(OWNER)
                .repoName(REPOSITORY)
                .accessToken(ACCESS_TOKEN)
                .webClient(webClientServiceProvider)
                .logger(logger)
                .build();

        assertEquals("/gitea" + REPOSITORY_PATH, takeRequest().getTarget());
        assertEquals(mockWebServer.url("/gitea").toString(), client.getApiUrl());
    }

    @Test
    void testBuildRepositoryNotFound() {
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"GetRepositoryByName\"}");

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, this::buildClientRequest);
        assertTrue(exception.getMessage().contains("HTTP 404: GetRepositoryByName"), exception.getMessage());
    }

    @Test
    void testGetBranchesPaging() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        final int total = GiteaRepositoryClient.BRANCH_PAGE_SIZE + 2;
        enqueue(HttpURLConnection.HTTP_OK, branches(0, GiteaRepositoryClient.BRANCH_PAGE_SIZE), TOTAL_COUNT_HEADER, Integer.toString(total));
        enqueue(HttpURLConnection.HTTP_OK, branches(GiteaRepositoryClient.BRANCH_PAGE_SIZE, total), TOTAL_COUNT_HEADER, Integer.toString(total));

        final Set<String> branches = client.getBranches();

        assertEquals(total, branches.size());
        assertTrue(branches.contains("branch-51"));
        takeRequest();
        assertEquals(REPOSITORY_PATH + "/branches?page=1&limit=50", takeRequest().getTarget());
        assertEquals(REPOSITORY_PATH + "/branches?page=2&limit=50", takeRequest().getTarget());
        assertEquals(3, mockWebServer.getRequestCount());
    }

    @Test
    void testGetTopLevelDirectoryNamesRepositoryPath() throws Exception {
        final GiteaRepositoryClient client = buildClient("flows/nifi");
        enqueue(HttpURLConnection.HTTP_OK, """
                [{"name":"bucket-a","type":"dir"},{"name":"README.md","type":"file"},{"name":"bucket-b","type":"dir"}]""");

        final Set<String> directories = client.getTopLevelDirectoryNames(BRANCH);

        assertEquals(Set.of("bucket-a", "bucket-b"), directories);
        takeRequest();
        assertEquals(REPOSITORY_PATH + "/contents/flows/nifi?ref=main", takeRequest().getTarget());
    }

    @Test
    void testGetTopLevelDirectoryNamesDirectoryNotFound() throws Exception {
        final GiteaRepositoryClient client = buildClient("flows");
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"GetContentsOrList\"}");
        enqueue(HttpURLConnection.HTTP_OK, "{\"name\":\"main\"}");

        assertTrue(client.getTopLevelDirectoryNames(BRANCH).isEmpty());
        takeRequest();
        assertEquals(REPOSITORY_PATH + "/contents/flows?ref=main", takeRequest().getTarget());
        assertEquals(REPOSITORY_PATH + "/branches/main", takeRequest().getTarget());
    }

    @Test
    void testGetTopLevelDirectoryNamesBranchNotFound() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"branch does not exist\"}");
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"branch does not exist\"}");
        enqueue(HttpURLConnection.HTTP_OK, REPOSITORY_WRITABLE);

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> client.getTopLevelDirectoryNames("develop"));
        assertTrue(exception.getMessage().contains("Branch [develop] not found"), exception.getMessage());
    }

    @Test
    void testGetTopLevelDirectoryNamesEmptyRepository() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"not found\"}");
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"branch does not exist\"}");
        enqueue(HttpURLConnection.HTTP_OK, """
                {"name":"nifi-flows","empty":true,"permissions":{"push":true,"pull":true}}""");

        assertTrue(client.getTopLevelDirectoryNames(BRANCH).isEmpty());
    }

    @Test
    void testGetFileNames() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, """
                [{"name":"flow.json","type":"file"},{"name":"nested","type":"dir"},{"name":".keep","type":"file"}]""");

        assertEquals(Set.of("flow.json", ".keep"), client.getFileNames("bucket", BRANCH));
    }

    @Test
    void testGetCommits() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        final int retrieved = GiteaRepositoryClient.COMMIT_PAGE_SIZE;
        enqueue(HttpURLConnection.HTTP_OK, commits(0, retrieved), "X-Total", "120");

        final List<GitCommit> commits = client.getCommits(FLOW_PATH, BRANCH);

        assertEquals(retrieved, commits.size());
        final GitCommit first = commits.getFirst();
        assertEquals(commitSha(0), first.id());
        assertEquals("Author 0", first.author());
        assertEquals("Message 0", first.message());
        assertEquals(Instant.parse("2025-01-01T08:00:00Z"), first.commitDate());
        assertEquals(commitSha(retrieved - 1), commits.getLast().id());

        takeRequest();
        assertEquals(REPOSITORY_PATH + "/commits?sha=main&path=bucket/flow.json&stat=false&verification=false&files=false&limit=50",
                takeRequest().getTarget());
        assertEquals(2, mockWebServer.getRequestCount());
    }

    @ParameterizedTest
    @ValueSource(ints = {HttpURLConnection.HTTP_NOT_FOUND, HttpURLConnection.HTTP_CONFLICT})
    void testGetCommitsNotFoundOrEmptyRepository(final int statusCode) throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(statusCode, "{\"message\":\"not found\"}");

        assertTrue(client.getCommits(FLOW_PATH, BRANCH).isEmpty());
    }

    @Test
    void testGetContentSha() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, fileResponse());

        assertEquals(Optional.of(BLOB_SHA), client.getContentSha(FLOW_PATH, BRANCH));
        takeRequest();
        assertEquals(REPOSITORY_PATH + "/contents/bucket/flow.json?ref=main", takeRequest().getTarget());
    }

    @Test
    void testGetContentShaAtCommit() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, fileResponse());

        assertEquals(Optional.of(BLOB_SHA), client.getContentShaAtCommit(FLOW_PATH, COMMIT_SHA));
        takeRequest();
        assertEquals(REPOSITORY_PATH + "/contents/bucket/flow.json?ref=" + COMMIT_SHA, takeRequest().getTarget());
    }

    @Test
    void testGetContentShaNotFile() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, "[{\"name\":\"flow.json\",\"type\":\"file\"}]");
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"not found\"}");

        assertTrue(client.getContentSha("bucket", BRANCH).isEmpty());
        assertTrue(client.getContentSha(FLOW_PATH, BRANCH).isEmpty());
    }

    @Test
    void testGetContentFromCommit() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, FLOW_CONTENT);

        try (InputStream content = client.getContentFromCommit(FLOW_PATH, COMMIT_SHA)) {
            assertEquals(FLOW_CONTENT, new String(content.readAllBytes(), StandardCharsets.UTF_8));
        }
        takeRequest();
        assertEquals(REPOSITORY_PATH + "/media/bucket/flow.json?ref=" + COMMIT_SHA, takeRequest().getTarget());
    }

    @Test
    void testGetContentFromBranchNotFound() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"not found\"}");

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> client.getContentFromBranch(FLOW_PATH, BRANCH));
        assertTrue(exception.getMessage().contains("not found"), exception.getMessage());
    }

    @Test
    void testCreateContentNewFile() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_CREATED, fileCommitResponse());

        final GitCreateContentRequest request = GitCreateContentRequest.builder()
                .branch(BRANCH)
                .path(FLOW_PATH)
                .content(FLOW_CONTENT)
                .message("Registering Flow")
                .build();

        assertEquals(COMMIT_SHA, client.createContent(request));

        takeRequest();
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals("POST", recordedRequest.getMethod());
        assertEquals(REPOSITORY_PATH + "/contents/bucket/flow.json", recordedRequest.getTarget());
        final JsonNode body = readBody(recordedRequest);
        assertEquals(FLOW_CONTENT, new String(Base64.getDecoder().decode(body.get("content").asText()), StandardCharsets.UTF_8));
        assertEquals(BRANCH, body.get("branch").asText());
        assertEquals("Registering Flow", body.get("message").asText());
        assertFalse(body.has("sha"));
        assertFalse(body.has("author"));
    }

    @Test
    void testCreateContentUpdateFileWithAuthor() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, fileCommitResponse());

        final GitCreateContentRequest request = GitCreateContentRequest.builder()
                .branch(BRANCH)
                .path(FLOW_PATH)
                .content(FLOW_CONTENT)
                .message("Updated")
                .existingContentSha(BLOB_SHA)
                .expectedCommitSha(COMMIT_SHA)
                .authorName("CN=admin, OU=NiFi")
                .authorEmail("CN=admin, OU=NiFi")
                .build();

        assertEquals(COMMIT_SHA, client.createContent(request));

        takeRequest();
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals("PUT", recordedRequest.getMethod());
        final JsonNode body = readBody(recordedRequest);
        assertEquals(BLOB_SHA, body.get("sha").asText());
        assertEquals("CN=admin, OU=NiFi", body.get("author").get("name").asText());
        assertEquals("CN=admin, OU=NiFi", body.get("author").get("email").asText());
    }

    @ParameterizedTest
    @ValueSource(ints = {HttpURLConnection.HTTP_CONFLICT, 422})
    void testCreateContentConflict(final int statusCode) throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(statusCode, "{\"message\":\"sha does not match\"}");

        final GitCreateContentRequest request = GitCreateContentRequest.builder()
                .branch(BRANCH)
                .path(FLOW_PATH)
                .content(FLOW_CONTENT)
                .message("Updated")
                .existingContentSha(BLOB_SHA)
                .build();

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> client.createContent(request));
        assertTrue(exception.getMessage().contains("modified by another commit"), exception.getMessage());
        assertTrue(exception.getMessage().contains("sha does not match"), exception.getMessage());
    }

    @Test
    void testCreateContentForbidden() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_FORBIDDEN, "{\"message\":\"token does not have at least one of required scope(s)\"}");

        final GitCreateContentRequest request = GitCreateContentRequest.builder()
                .branch(BRANCH)
                .path(FLOW_PATH)
                .content(FLOW_CONTENT)
                .message("Registering Flow")
                .build();

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> client.createContent(request));
        assertTrue(exception.getMessage().contains("write:repository"), exception.getMessage());
    }

    @Test
    void testCreateContentQuotaExceeded() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(413, "{\"message\":\"quota exceeded\"}");

        final GitCreateContentRequest request = GitCreateContentRequest.builder()
                .branch(BRANCH)
                .path(FLOW_PATH)
                .content(FLOW_CONTENT)
                .message("Registering Flow")
                .build();

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> client.createContent(request));
        assertTrue(exception.getMessage().contains("quota"), exception.getMessage());
    }

    @Test
    void testDeleteContent() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, fileResponse());
        enqueue(HttpURLConnection.HTTP_OK, fileCommitResponse());

        try (InputStream deleted = client.deleteContent(FLOW_PATH, "Deregistering Flow", BRANCH, "user", "user")) {
            assertEquals(FLOW_CONTENT, new String(deleted.readAllBytes(), StandardCharsets.UTF_8));
        }

        takeRequest();
        takeRequest();
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals("DELETE", recordedRequest.getMethod());
        assertEquals(REPOSITORY_PATH + "/contents/bucket/flow.json", recordedRequest.getTarget());
        final JsonNode body = readBody(recordedRequest);
        assertEquals(BLOB_SHA, body.get("sha").asText());
        assertEquals(BRANCH, body.get("branch").asText());
        assertEquals("user", body.get("author").get("name").asText());
    }

    @Test
    void testDeleteContentNotFound() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_NOT_FOUND, "{\"message\":\"not found\"}");

        assertThrows(FlowRegistryException.class, () -> client.deleteContent(FLOW_PATH, "Deregistering Flow", BRANCH));
    }

    @Test
    void testCreateBranchFromCommit() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_CREATED, "{\"name\":\"feature\"}");

        client.createBranch(" feature ", BRANCH, Optional.of(COMMIT_SHA));

        takeRequest();
        final RecordedRequest recordedRequest = takeRequest();
        assertEquals("POST", recordedRequest.getMethod());
        assertEquals(REPOSITORY_PATH + "/branches", recordedRequest.getTarget());
        final JsonNode body = readBody(recordedRequest);
        assertEquals("feature", body.get("new_branch_name").asText());
        assertEquals(COMMIT_SHA, body.get("old_ref_name").asText());
    }

    @Test
    void testCreateBranchFromBranchExists() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_CONFLICT, "{\"message\":\"The branch already exists.\"}");

        final FlowRegistryException exception = assertThrows(FlowRegistryException.class, () -> client.createBranch("feature", BRANCH, Optional.empty()));
        assertTrue(exception.getMessage().contains("already exists"), exception.getMessage());

        takeRequest();
        assertEquals(BRANCH, readBody(takeRequest()).get("old_ref_name").asText());
    }

    @Test
    void testPathSegmentEncoding() throws Exception {
        final GiteaRepositoryClient client = buildClient(null);
        enqueue(HttpURLConnection.HTTP_OK, "[]");

        client.getFileNames("bucket a&b+c%d", BRANCH);

        takeRequest();
        final RecordedRequest request = takeRequest();
        assertEquals(REPOSITORY_PATH + "/contents/bucket%20a&b+c%25d?ref=main", request.getTarget());
        assertEquals(List.of("api", "v1", "repos", OWNER, REPOSITORY, "contents", "bucket a&b+c%d"), request.getUrl().pathSegments());
    }

    @Test
    void testNormalizeApiUrl() {
        assertEquals("https://gitea.example.com", GiteaRepositoryClient.normalizeApiUrl("https://gitea.example.com/"));
        assertEquals("https://gitea.example.com", GiteaRepositoryClient.normalizeApiUrl(" https://gitea.example.com/api/v1/ "));
        assertEquals("https://example.com/gitea", GiteaRepositoryClient.normalizeApiUrl("https://example.com/gitea/api/v1"));
    }

    private GiteaRepositoryClient buildClient(final String repositoryPath) throws FlowRegistryException {
        return buildClient(repositoryPath, REPOSITORY_WRITABLE);
    }

    private GiteaRepositoryClient buildClient(final String repositoryPath, final String repository) throws FlowRegistryException {
        enqueue(HttpURLConnection.HTTP_OK, repository);
        return GiteaRepositoryClient.builder()
                .clientId("client")
                .apiUrl(mockWebServer.url("/").toString())
                .repoOwner(OWNER)
                .repoName(REPOSITORY)
                .repoPath(repositoryPath)
                .accessToken(ACCESS_TOKEN)
                .webClient(webClientServiceProvider)
                .logger(logger)
                .build();
    }

    private void buildClientRequest() throws FlowRegistryException {
        GiteaRepositoryClient.builder()
                .clientId("client")
                .apiUrl(mockWebServer.url("/").toString())
                .repoOwner(OWNER)
                .repoName(REPOSITORY)
                .accessToken(ACCESS_TOKEN)
                .webClient(webClientServiceProvider)
                .logger(logger)
                .build();
    }

    private void enqueue(final int statusCode, final String body) {
        mockWebServer.enqueue(new MockResponse.Builder().code(statusCode).body(body).build());
    }

    private void enqueue(final int statusCode, final String body, final String headerName, final String headerValue) {
        mockWebServer.enqueue(new MockResponse.Builder().code(statusCode).body(body).addHeader(headerName, headerValue).build());
    }

    private RecordedRequest takeRequest() throws InterruptedException {
        final RecordedRequest request = mockWebServer.takeRequest(1, TimeUnit.SECONDS);
        assertNotNull(request);
        return request;
    }

    private JsonNode readBody(final RecordedRequest request) throws IOException {
        assertNotNull(request.getBody());
        return MAPPER.readTree(request.getBody().utf8());
    }

    private static String branches(final int start, final int end) {
        return IntStream.range(start, end)
                .mapToObj(index -> "{\"name\":\"branch-%d\",\"commit\":{\"id\":\"%s\"}}".formatted(index, commitSha(index)))
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static String commits(final int start, final int end) {
        return IntStream.range(start, end)
                .mapToObj(index -> """
                        {"sha":"%s","commit":{"message":"Message %d\\n","author":{"name":"Author %d","email":"author@example.com","date":"2025-01-01T09:00:00+01:00"},\
                        "committer":{"name":"Committer","email":"committer@example.com","date":"2025-01-01T10:00:00+02:00"}}}"""
                        .formatted(commitSha(index), index, index))
                .collect(Collectors.joining(",", "[", "]"));
    }

    private static String commitSha(final int index) {
        return "%040d".formatted(index);
    }

    private static String fileResponse() {
        final String content = Base64.getMimeEncoder().encodeToString(FLOW_CONTENT.getBytes(StandardCharsets.UTF_8));
        return """
                {"name":"flow.json","path":"bucket/flow.json","sha":"%s","type":"file","encoding":"base64","content":"%s"}""".formatted(BLOB_SHA, content);
    }

    private static String fileCommitResponse() {
        return """
                {"content":{"name":"flow.json","sha":"%s"},"commit":{"sha":"%s"}}""".formatted(BLOB_SHA, COMMIT_SHA);
    }
}
