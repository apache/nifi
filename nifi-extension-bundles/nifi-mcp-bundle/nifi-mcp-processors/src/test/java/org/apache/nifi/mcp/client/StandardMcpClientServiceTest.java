/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.mcp.client;

import mockwebserver3.Dispatcher;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.mcp.McpCallResult;
import org.apache.nifi.mcp.McpClientException;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Wire-level test: a MockWebServer dispatcher answers the MCP Streamable HTTP JSON-RPC messages
 * sent by the client service under test.
 */
class StandardMcpClientServiceTest {

    private static final JsonMapper JSON_MAPPER = JsonMapper.builder().build();
    private static final String SESSION_HEADER = "Mcp-Session-Id";

    private final McpDispatcher dispatcher = new McpDispatcher();

    private MockWebServer server;
    private TestRunner runner;
    private StandardMcpClientService service;

    @BeforeEach
    void setUp() throws Exception {
        startServer(0);
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardMcpClientService();
        runner.addControllerService("mcp-client", service);
        runner.setProperty(service, StandardMcpClientService.SERVER_URL, serverUrl());
    }

    @AfterEach
    void tearDown() {
        if (service.isEnabled()) {
            runner.disableControllerService(service);
        }
        server.close();
    }

    @Test
    void callsToolsOverTheWire() throws Exception {
        runner.enableControllerService(service);

        final McpCallResult echo = service.callTool("echo", "{\"message\":\"ciao\"}");
        assertFalse(echo.isError());
        assertEquals("ciao", echo.text());
        assertTrue(echo.resultJson().contains("\"content\""), echo.resultJson());
        assertNull(echo.structuredContentJson());
        assertEquals("echo", dispatcher.lastToolCall.path("name").asString());
        assertEquals("ciao", dispatcher.lastToolCall.path("arguments").path("message").asString());

        final McpCallResult failing = service.callTool("always_fails", null);
        assertTrue(failing.isError());
        assertEquals("nope", failing.text());

        assertEquals("test-server/1.2.3", service.serverDescription());
    }

    @Test
    void exposesStructuredContent() throws Exception {
        runner.enableControllerService(service);

        final McpCallResult forecast = service.callTool("forecast", null);
        final JsonNode structured = JSON_MAPPER.readTree(forecast.structuredContentJson());
        assertEquals("Pisa", structured.path("city").asString());
        assertEquals(18.5, structured.path("temperature").asDouble());
    }

    @Test
    void unknownToolIsAClientExceptionNotAToolError() throws Exception {
        runner.enableControllerService(service);
        assertThrows(McpClientException.class, () -> service.callTool("does_not_exist", "{}"));
        // The JSON-RPC error must not have torn down the session.
        assertEquals("ciao", service.callTool("echo", "{\"message\":\"ciao\"}").text());
        assertEquals(1, dispatcher.initializeCount);
    }

    @Test
    void nonObjectArgumentsAreRejectedBeforeAnyRequest() {
        runner.enableControllerService(service);
        assertThrows(McpClientException.class, () -> service.callTool("echo", "[1,2,3]"));
        assertThrows(McpClientException.class, () -> service.callTool("echo", "not json"));
        assertNull(dispatcher.lastToolCall);
    }

    @Test
    void listsAndReadsResourcesAndPrompts() throws Exception {
        runner.enableControllerService(service);

        assertTrue(service.listTools().contains("\"echo\""), service.listTools());
        assertTrue(service.listResources().contains("memo://greeting"));
        assertTrue(service.listPrompts().contains("\"greet\""));

        assertEquals("hello from a resource", service.readResource("memo://greeting").text());
        assertEquals("Say hi to Alice", service.getPrompt("greet", "{\"name\":\"Alice\"}").text());
    }

    @Test
    void bearerTokenAndDynamicPropertiesAreSentAsHeaders() throws Exception {
        runner.setProperty(service, StandardMcpClientService.BEARER_TOKEN, "s3cret");
        runner.setProperty(service, "X-Tenant", "acme");
        runner.enableControllerService(service);

        service.callTool("echo", "{\"message\":\"x\"}");

        assertTrue(dispatcher.authorizationHeaders.contains("Bearer s3cret"), dispatcher.authorizationHeaders.toString());
        assertTrue(dispatcher.tenantHeaders.contains("acme"), dispatcher.tenantHeaders.toString());
    }

    @Test
    void enablingSucceedsWhileServerIsDownAndConnectsOnceItIsBack() throws Exception {
        final int originalPort = server.getPort();
        server.close();

        runner.enableControllerService(service);
        assertThrows(McpClientException.class, () -> service.callTool("echo", "{\"message\":\"x\"}"));

        startServer(originalPort);
        assertEquals("back", service.callTool("echo", "{\"message\":\"back\"}").text());
    }

    @Test
    void reconnectsAfterServerRestartInvalidatesTheSession() throws Exception {
        runner.enableControllerService(service);
        assertEquals("one", service.callTool("echo", "{\"message\":\"one\"}").text());

        // A restarted server no longer knows our Mcp-Session-Id and answers 404 to it.
        dispatcher.sessionId = "restarted-session";

        // First call after the restart may fail (stale session) - it must not keep failing.
        try {
            service.callTool("echo", "{\"message\":\"two\"}");
        } catch (final McpClientException expected) {
            // stale session dropped, next call reconnects
        }
        assertEquals("three", service.callTool("echo", "{\"message\":\"three\"}").text());
        // The SDK may re-initialize on the 404 by itself before the service drops the session too,
        // so one or two new handshakes are both valid.
        assertTrue(dispatcher.initializeCount >= 2, "initialize count " + dispatcher.initializeCount);
    }

    @Test
    void verifyReportsServerAndToolCount() {
        final MockConfigurationContext context = new MockConfigurationContext(service,
                Map.of(StandardMcpClientService.SERVER_URL, serverUrl()), null, Map.of());
        final List<ConfigVerificationResult> results = service.verify(context, runner.getLogger(), Map.of());

        assertEquals(2, results.size(), results.toString());
        assertTrue(results.stream().allMatch(r -> r.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL),
                results.toString());
        assertTrue(results.get(0).getExplanation().contains("test-server/1.2.3"));
        assertTrue(results.get(1).getExplanation().contains("3 tool(s)"));
    }

    private void startServer(final int port) throws IOException {
        server = new MockWebServer();
        server.setDispatcher(dispatcher);
        server.start(port);
    }

    private String serverUrl() {
        return server.url("/mcp").toString();
    }

    /**
     * Minimal MCP server: answers each JSON-RPC request with a JSON body, as the Streamable HTTP
     * transport allows, and rejects requests carrying an unknown session with 404.
     */
    private static final class McpDispatcher extends Dispatcher {

        private final Queue<String> authorizationHeaders = new ConcurrentLinkedQueue<>();
        private final Queue<String> tenantHeaders = new ConcurrentLinkedQueue<>();
        private volatile String sessionId = "test-session";
        private volatile int initializeCount;
        private volatile JsonNode lastToolCall;

        @Override
        public MockResponse dispatch(final RecordedRequest request) {
            final String authorization = request.getHeaders().get("Authorization");
            if (authorization != null) {
                authorizationHeaders.add(authorization);
            }
            final String tenant = request.getHeaders().get("X-Tenant");
            if (tenant != null) {
                tenantHeaders.add(tenant);
            }

            if (!"POST".equals(request.getMethod())) {
                // No server-initiated SSE stream; session DELETE on close is not supported either.
                return new MockResponse.Builder().code(405).build();
            }

            final JsonNode message = JSON_MAPPER.readTree(request.getBody().utf8());
            final String method = message.path("method").asString();
            final JsonNode params = message.path("params");
            if (!message.has("id")) {
                return new MockResponse.Builder().code(202).build(); // notification
            }

            if ("initialize".equals(method)) {
                initializeCount++;
                return jsonResponse(message, Map.of("result", Map.of(
                        "protocolVersion", params.path("protocolVersion").asString(),
                        "capabilities", Map.of("tools", Map.of(), "resources", Map.of(), "prompts", Map.of()),
                        "serverInfo", Map.of("name", "test-server", "version", "1.2.3"))))
                        .newBuilder()
                        .addHeader(SESSION_HEADER, sessionId)
                        .build();
            }
            if (!sessionId.equals(request.getHeaders().get(SESSION_HEADER))) {
                return new MockResponse.Builder().code(404).build();
            }

            return switch (method) {
                case "tools/list" -> jsonResponse(message, Map.of("result", Map.of("tools", List.of(
                        tool("echo", "Echoes 'message'"),
                        tool("always_fails", "Reports a tool error"),
                        tool("forecast", "Returns structured content")))));
                case "tools/call" -> callTool(message, params);
                case "resources/list" -> jsonResponse(message, Map.of("result", Map.of("resources", List.of(
                        Map.of("uri", "memo://greeting", "name", "greeting", "mimeType", "text/plain")))));
                case "resources/read" -> jsonResponse(message, Map.of("result", Map.of("contents", List.of(
                        Map.of("uri", params.path("uri").asString(), "mimeType", "text/plain", "text", "hello from a resource")))));
                case "prompts/list" -> jsonResponse(message, Map.of("result", Map.of("prompts", List.of(
                        Map.of("name", "greet", "description", "Greets someone",
                                "arguments", List.of(Map.of("name", "name", "description", "who", "required", true)))))));
                case "prompts/get" -> jsonResponse(message, Map.of("result", Map.of(
                        "description", "greeting",
                        "messages", List.of(Map.of("role", "user",
                                "content", text("Say hi to " + params.path("arguments").path("name").asString()))))));
                default -> jsonResponse(message, Map.of("error", Map.of("code", -32601, "message", "Method not found: " + method)));
            };
        }

        private MockResponse callTool(final JsonNode message, final JsonNode params) {
            lastToolCall = params;
            final String name = params.path("name").asString();
            return switch (name) {
                case "echo" -> jsonResponse(message, Map.of("result", Map.of(
                        "content", List.of(text(params.path("arguments").path("message").asString())),
                        "isError", false)));
                case "always_fails" -> jsonResponse(message, Map.of("result", Map.of(
                        "content", List.of(text("nope")),
                        "isError", true)));
                case "forecast" -> jsonResponse(message, Map.of("result", Map.of(
                        "content", List.of(text("{\"city\":\"Pisa\",\"temperature\":18.5}")),
                        "structuredContent", Map.of("city", "Pisa", "temperature", 18.5),
                        "isError", false)));
                default -> jsonResponse(message, Map.of("error", Map.of("code", -32602, "message", "Unknown tool: " + name)));
            };
        }

        private static Map<String, Object> tool(final String name, final String description) {
            return Map.of("name", name, "description", description, "inputSchema", Map.of("type", "object"));
        }

        private static Map<String, Object> text(final String text) {
            return Map.of("type", "text", "text", text);
        }

        private static MockResponse jsonResponse(final JsonNode request, final Map<String, Object> payload) {
            final Map<String, Object> body = new LinkedHashMap<>();
            body.put("jsonrpc", "2.0");
            body.put("id", request.get("id"));
            body.putAll(payload);
            return new MockResponse.Builder()
                    .code(200)
                    .addHeader("Content-Type", "application/json")
                    .body(JSON_MAPPER.writeValueAsString(body))
                    .build();
        }
    }
}
