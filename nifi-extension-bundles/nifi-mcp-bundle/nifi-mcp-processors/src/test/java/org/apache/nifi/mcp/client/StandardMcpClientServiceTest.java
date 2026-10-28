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

import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import io.modelcontextprotocol.server.McpServer;
import io.modelcontextprotocol.server.McpServerFeatures.SyncPromptSpecification;
import io.modelcontextprotocol.server.McpServerFeatures.SyncResourceSpecification;
import io.modelcontextprotocol.server.McpServerFeatures.SyncToolSpecification;
import io.modelcontextprotocol.server.McpSyncServer;
import io.modelcontextprotocol.server.transport.HttpServletStreamableServerTransportProvider;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.TextContent;
import jakarta.servlet.Filter;
import jakarta.servlet.http.HttpServletRequest;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.mcp.McpCallResult;
import org.apache.nifi.mcp.McpClientException;
import org.apache.nifi.processors.mcp.InvokeMcpServer;
import org.apache.nifi.util.MockConfigurationContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.ee11.servlet.FilterHolder;
import org.eclipse.jetty.ee11.servlet.ServletContextHandler;
import org.eclipse.jetty.ee11.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Real wire-protocol test: an MCP Java SDK server on Jetty (Streamable HTTP) and the client
 * service under test talking to it over localhost.
 */
class StandardMcpClientServiceTest {

    private final McpJsonMapper jsonMapper = new JacksonMcpJsonMapper(tools.jackson.databind.json.JsonMapper.builder().build());
    private final Queue<String> authorizationHeaders = new ConcurrentLinkedQueue<>();
    private final Queue<String> customHeaders = new ConcurrentLinkedQueue<>();

    private Server jetty;
    private McpSyncServer mcpServer;
    private int port;

    private TestRunner runner;
    private StandardMcpClientService service;

    @BeforeEach
    void setUp() throws Exception {
        startServer(0);
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        service = new StandardMcpClientService();
        runner.addControllerService("mcp-client", service);
        runner.setProperty(service, StandardMcpClientService.SERVER_URL, "http://localhost:" + port + "/mcp");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (service.isEnabled()) {
            runner.disableControllerService(service);
        }
        stopServer();
    }

    @Test
    void callsToolsOverTheWire() throws Exception {
        runner.enableControllerService(service);

        final McpCallResult echo = service.callTool("echo", "{\"message\":\"ciao\"}");
        assertFalse(echo.isError());
        assertEquals("ciao", echo.text());
        assertTrue(echo.resultJson().contains("\"content\""), echo.resultJson());

        final McpCallResult failing = service.callTool("always_fails", null);
        assertTrue(failing.isError());
        assertEquals("nope", failing.text());

        assertEquals("test-server/1.2.3", service.serverDescription());
        assertEquals(null, echo.structuredContentJson());
    }

    @Test
    void exposesStructuredContent() throws Exception {
        runner.enableControllerService(service);

        final McpCallResult forecast = service.callTool("forecast", null);
        final Map<?, ?> structured = jsonMapper.readValue(forecast.structuredContentJson(), Map.class);
        assertEquals(Map.of("city", "Pisa", "temperature", 18.5), structured);
    }

    @Test
    void unknownToolIsAClientExceptionNotAToolError() throws Exception {
        runner.enableControllerService(service);
        assertThrows(McpClientException.class, () -> service.callTool("does_not_exist", "{}"));
        // The JSON-RPC error must not have torn down the session.
        assertEquals("ciao", service.callTool("echo", "{\"message\":\"ciao\"}").text());
    }

    @Test
    void nonObjectArgumentsAreRejectedBeforeAnyRequest() {
        runner.enableControllerService(service);
        assertThrows(McpClientException.class, () -> service.callTool("echo", "[1,2,3]"));
        assertThrows(McpClientException.class, () -> service.callTool("echo", "not json"));
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

        assertTrue(authorizationHeaders.contains("Bearer s3cret"), authorizationHeaders.toString());
        assertTrue(customHeaders.contains("acme"), customHeaders.toString());
    }

    @Test
    void enablingSucceedsWhileServerIsDownAndConnectsOnceItIsBack() throws Exception {
        final int originalPort = port;
        stopServer();

        runner.enableControllerService(service);
        assertThrows(McpClientException.class, () -> service.callTool("echo", "{\"message\":\"x\"}"));

        startServer(originalPort);
        assertEquals("back", service.callTool("echo", "{\"message\":\"back\"}").text());
    }

    @Test
    void reconnectsAfterServerRestartInvalidatesTheSession() throws Exception {
        runner.enableControllerService(service);
        assertEquals("one", service.callTool("echo", "{\"message\":\"one\"}").text());

        final int originalPort = port;
        stopServer();
        startServer(originalPort); // fresh server: our Mcp-Session-Id is unknown to it

        // First call after the restart may fail (stale session) - it must not keep failing.
        try {
            service.callTool("echo", "{\"message\":\"two\"}");
        } catch (final McpClientException expected) {
            // stale session dropped, next call reconnects
        }
        assertEquals("three", service.callTool("echo", "{\"message\":\"three\"}").text());
    }

    @Test
    void verifyReportsServerAndToolCount() {
        final MockConfigurationContext context = new MockConfigurationContext(service,
                Map.of(StandardMcpClientService.SERVER_URL, "http://localhost:" + port + "/mcp"), null, Map.of());
        final List<ConfigVerificationResult> results =
                service.verify(context, runner.getLogger(), Map.of());

        assertEquals(2, results.size(), results.toString());
        assertTrue(results.stream().allMatch(r -> r.getOutcome() == ConfigVerificationResult.Outcome.SUCCESSFUL),
                results.toString());
        assertTrue(results.get(0).getExplanation().contains("test-server/1.2.3"));
        assertTrue(results.get(1).getExplanation().contains("3 tool(s)"));
    }

    @Test
    void invokeMcpServerEndToEnd() throws Exception {
        final TestRunner processor = TestRunners.newTestRunner(InvokeMcpServer.class);
        final StandardMcpClientService processorService = new StandardMcpClientService();
        processor.addControllerService("mcp-client", processorService);
        processor.setProperty(processorService, StandardMcpClientService.SERVER_URL, "http://localhost:" + port + "/mcp");
        processor.enableControllerService(processorService);
        processor.setProperty(InvokeMcpServer.MCP_CLIENT_SERVICE, "mcp-client");
        processor.setProperty(InvokeMcpServer.TOOL_NAME, "echo");
        processor.setProperty(InvokeMcpServer.OUTPUT_CONTENT, InvokeMcpServer.OutputContent.TEXT);

        processor.enqueue("{\"message\":\"through the canvas\"}");
        processor.run();

        processor.assertTransferCount(InvokeMcpServer.SUCCESS, 1);
        processor.assertTransferCount(InvokeMcpServer.ORIGINAL, 1);
        final MockFlowFile out = processor.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0);
        out.assertContentEquals("through the canvas");
        out.assertAttributeEquals("mcp.server", "test-server/1.2.3");
        processor.disableControllerService(processorService);
    }

    // ---------------------------------------------------------------------------------------------

    private void startServer(final int requestedPort) throws Exception {
        final HttpServletStreamableServerTransportProvider transport = HttpServletStreamableServerTransportProvider.builder()
                .mcpEndpoint("/mcp")
                .jsonMapper(jsonMapper)
                .build();

        final SyncToolSpecification echo = SyncToolSpecification.builder()
                .tool(McpSchema.Tool.builder().name("echo").description("Echoes 'message'")
                        .inputSchema(jsonMapper, "{\"type\":\"object\",\"properties\":{\"message\":{\"type\":\"string\"}}}")
                        .build())
                .callHandler((exchange, request) -> CallToolResult.builder()
                        .content(List.of(new TextContent(String.valueOf(request.arguments().get("message")))))
                        .isError(false)
                        .build())
                .build();
        final SyncToolSpecification fails = SyncToolSpecification.builder()
                .tool(McpSchema.Tool.builder().name("always_fails").description("Reports a tool error")
                        .inputSchema(jsonMapper, "{\"type\":\"object\"}")
                        .build())
                .callHandler((exchange, request) -> CallToolResult.builder()
                        .content(List.of(new TextContent("nope")))
                        .isError(true)
                        .build())
                .build();
        final SyncToolSpecification forecast = SyncToolSpecification.builder()
                .tool(McpSchema.Tool.builder().name("forecast").description("Returns structured content")
                        .inputSchema(jsonMapper, "{\"type\":\"object\"}")
                        .build())
                .callHandler((exchange, request) -> CallToolResult.builder()
                        .content(List.of(new TextContent("{\"city\":\"Pisa\",\"temperature\":18.5}")))
                        .structuredContent(Map.of("city", "Pisa", "temperature", 18.5))
                        .isError(false)
                        .build())
                .build();
        final SyncResourceSpecification resource = new SyncResourceSpecification(
                McpSchema.Resource.builder().uri("memo://greeting").name("greeting").mimeType("text/plain").build(),
                (exchange, request) -> new McpSchema.ReadResourceResult(List.of(
                        new McpSchema.TextResourceContents(request.uri(), "text/plain", "hello from a resource"))));
        final SyncPromptSpecification prompt = new SyncPromptSpecification(
                new McpSchema.Prompt("greet", "Greets someone",
                        List.of(new McpSchema.PromptArgument("name", "who", true))),
                (exchange, request) -> new McpSchema.GetPromptResult("greeting", List.of(
                        new McpSchema.PromptMessage(McpSchema.Role.USER,
                                new TextContent("Say hi to " + request.arguments().get("name"))))));

        mcpServer = McpServer.sync(transport)
                .serverInfo("test-server", "1.2.3")
                .jsonMapper(jsonMapper)
                .capabilities(McpSchema.ServerCapabilities.builder().tools(true).resources(false, false).prompts(false).build())
                .tools(echo, fails, forecast)
                .resources(resource)
                .prompts(prompt)
                .build();

        jetty = new Server();
        final ServerConnector connector = new ServerConnector(jetty);
        connector.setHost("localhost");
        connector.setPort(requestedPort);
        jetty.addConnector(connector);

        final ServletContextHandler handler = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        handler.setContextPath("/");
        final Filter headerCapture = (request, response, chain) -> {
            final HttpServletRequest http = (HttpServletRequest) request;
            if (http.getHeader("Authorization") != null) {
                authorizationHeaders.add(http.getHeader("Authorization"));
            }
            if (http.getHeader("X-Tenant") != null) {
                customHeaders.add(http.getHeader("X-Tenant"));
            }
            chain.doFilter(request, response);
        };
        handler.addFilter(new FilterHolder(headerCapture), "/*", EnumSet.of(jakarta.servlet.DispatcherType.REQUEST));
        handler.addServlet(new ServletHolder(transport), "/mcp/*");
        jetty.setHandler(handler);
        jetty.start();
        port = connector.getLocalPort();
    }

    private void stopServer() throws Exception {
        if (mcpServer != null) {
            mcpServer.close();
            mcpServer = null;
        }
        if (jetty != null) {
            jetty.stop();
            jetty = null;
        }
    }
}
