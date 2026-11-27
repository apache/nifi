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
package org.apache.nifi.processors.mcp;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.mcp.McpCallResult;
import org.apache.nifi.mcp.McpClientException;
import org.apache.nifi.mcp.McpClientService;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class InvokeMcpServerTest {

    static final class FakeMcpClientService extends AbstractControllerService implements McpClientService {

        String lastToolName;
        String lastArguments;
        String lastUri;
        McpCallResult nextResult = new McpCallResult("{\"content\":[{\"type\":\"text\",\"text\":\"ok\"}]}", "ok", false);
        McpClientException nextFailure;

        @Override
        public McpCallResult callTool(final String toolName, final String argumentsJson) throws McpClientException {
            lastToolName = toolName;
            lastArguments = argumentsJson;
            if (nextFailure != null) {
                throw nextFailure;
            }
            return nextResult;
        }

        @Override
        public String listTools() {
            return "{\"tools\":[]}";
        }

        @Override
        public String listResources() {
            return "{\"resources\":[]}";
        }

        @Override
        public McpCallResult readResource(final String uri) {
            lastUri = uri;
            return new McpCallResult("{\"contents\":[]}", "resource text", false);
        }

        @Override
        public String listPrompts() {
            return "{\"prompts\":[]}";
        }

        @Override
        public McpCallResult getPrompt(final String promptName, final String argumentsJson) {
            lastToolName = promptName;
            lastArguments = argumentsJson;
            return new McpCallResult("{\"messages\":[]}", "prompt text", false);
        }

        @Override
        public String serverDescription() {
            return "fake/1.0";
        }
    }

    private TestRunner runner;
    private FakeMcpClientService fake;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(InvokeMcpServer.class);
        fake = new FakeMcpClientService();
        runner.addControllerService("mcp-client", fake);
        runner.enableControllerService(fake);
        runner.setProperty(InvokeMcpServer.MCP_CLIENT_SERVICE, "mcp-client");
    }

    @Test
    void callsToolWithFlowFileContentAsArguments() {
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "${tool}");
        runner.enqueue("{\"q\":1}", Map.of("tool", "search", "keep", "me"));

        runner.run();

        assertEquals("search", fake.lastToolName);
        assertEquals("{\"q\":1}", fake.lastArguments);
        runner.assertTransferCount(InvokeMcpServer.SUCCESS, 1);
        runner.assertTransferCount(InvokeMcpServer.ORIGINAL, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0);
        out.assertContentEquals(fake.nextResult.resultJson());
        out.assertAttributeEquals("keep", "me");
        out.assertAttributeEquals("mcp.target", "search");
        out.assertAttributeEquals("mcp.is.error", "false");
        out.assertAttributeEquals("mcp.server", "fake/1.0");
        out.assertAttributeEquals("mime.type", "application/json");
    }

    @Test
    void argumentsPropertyIsEvaluatedAgainstTheFlowFile() {
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.setProperty(InvokeMcpServer.ARGUMENTS_SOURCE, InvokeMcpServer.ArgumentsSource.PROPERTY);
        runner.setProperty(InvokeMcpServer.ARGUMENTS, "{\"q\":\"${term:escapeJson()}\"}");
        runner.enqueue("ignored", Map.of("term", "a \"quoted\" term"));

        runner.run();

        assertEquals("{\"q\":\"a \\\"quoted\\\" term\"}", fake.lastArguments);
    }

    @Test
    void textOnlyOutputWritesJustTheText() {
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.setProperty(InvokeMcpServer.OUTPUT_CONTENT, InvokeMcpServer.OutputContent.TEXT);
        runner.enqueue("{}");

        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0);
        out.assertContentEquals("ok");
        out.assertAttributeEquals("mime.type", "text/plain");
    }

    @Test
    void toolErrorIsRoutedSeparatelyKeepingTheResult() {
        fake.nextResult = new McpCallResult("{\"isError\":true}", "boom", true);
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.enqueue("{}");

        runner.run();

        runner.assertTransferCount(InvokeMcpServer.TOOL_ERROR, 1);
        runner.assertTransferCount(InvokeMcpServer.ORIGINAL, 1);
        runner.assertTransferCount(InvokeMcpServer.SUCCESS, 0);
        runner.getFlowFilesForRelationship(InvokeMcpServer.TOOL_ERROR).get(0).assertAttributeEquals("mcp.is.error", "true");
    }

    @Test
    void clientFailureRoutesOriginalToFailureWithMessage() {
        fake.nextFailure = new McpClientException("connection refused");
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.enqueue("{}");

        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeMcpServer.FAILURE, 1);
        final MockFlowFile failed = runner.getFlowFilesForRelationship(InvokeMcpServer.FAILURE).get(0);
        failed.assertContentEquals("{}");
        failed.assertAttributeEquals("mcp.error.message", "McpClientException: connection refused");
        assertEquals(1, runner.getPenalizedFlowFiles().size());
    }

    @Test
    void readResourceUsesTheUriProperty() {
        runner.setProperty(InvokeMcpServer.OPERATION, InvokeMcpServer.Operation.READ_RESOURCE);
        runner.setProperty(InvokeMcpServer.RESOURCE_URI, "file:///${filename}");
        runner.enqueue("", Map.of("filename", "a.txt"));

        runner.run();

        assertEquals("file:///a.txt", fake.lastUri);
        runner.assertTransferCount(InvokeMcpServer.SUCCESS, 1);
    }

    @Test
    void getPromptPassesNameAndArguments() {
        runner.setProperty(InvokeMcpServer.OPERATION, InvokeMcpServer.Operation.GET_PROMPT);
        runner.setProperty(InvokeMcpServer.PROMPT_NAME, "greet");
        runner.enqueue("{\"name\":\"x\"}");

        runner.run();

        assertEquals("greet", fake.lastToolName);
        assertEquals("{\"name\":\"x\"}", fake.lastArguments);
    }

    @Test
    void sourceModeWithoutIncomingConnectionEmitsOnlyTheResult() {
        runner.setIncomingConnection(false);
        runner.setProperty(InvokeMcpServer.OPERATION, InvokeMcpServer.Operation.LIST_TOOLS);

        runner.run();

        runner.assertAllFlowFilesTransferred(InvokeMcpServer.SUCCESS, 1);
        runner.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0).assertContentEquals("{\"tools\":[]}");
        assertEquals(ProvenanceEventType.RECEIVE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    void sourceModeFailureYieldsWithoutEmittingAnything() {
        runner.setIncomingConnection(false);
        fake.nextFailure = new McpClientException("down");
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.setProperty(InvokeMcpServer.ARGUMENTS_SOURCE, InvokeMcpServer.ArgumentsSource.PROPERTY);

        runner.run();

        runner.assertTransferCount(InvokeMcpServer.FAILURE, 0);
        runner.assertTransferCount(InvokeMcpServer.SUCCESS, 0);
        assertNull(fake.lastArguments);
    }

    @Test
    void writesStructuredContentWhenPresent() {
        fake.nextResult = new McpCallResult("{\"full\":true}", "text", false, "{\"price\":1}");
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.setProperty(InvokeMcpServer.OUTPUT_CONTENT, InvokeMcpServer.OutputContent.STRUCTURED_CONTENT);
        runner.enqueue("{}");

        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0);
        out.assertContentEquals("{\"price\":1}");
        out.assertAttributeEquals("mime.type", "application/json");
    }

    @Test
    void structuredContentFallsBackToTextWhenAbsent() {
        fake.nextResult = new McpCallResult("{\"full\":true}", "{\"price\":2}", false);
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.setProperty(InvokeMcpServer.OUTPUT_CONTENT, InvokeMcpServer.OutputContent.STRUCTURED_CONTENT);
        runner.enqueue("{}");

        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0);
        out.assertContentEquals("{\"price\":2}");
        out.assertAttributeEquals("mime.type", "application/json");
    }

    @Test
    void structuredContentFallbackToNonJsonTextIsLabelledPlainText() {
        // e.g. a docs server returning markdown with no outputSchema
        fake.nextResult = new McpCallResult("{\"full\":true}", "# Title\n\nSome markdown", false);
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "read_docs");
        runner.setProperty(InvokeMcpServer.OUTPUT_CONTENT, InvokeMcpServer.OutputContent.STRUCTURED_CONTENT);
        runner.enqueue("{}");

        runner.run();

        final MockFlowFile out = runner.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0);
        out.assertContentEquals("# Title\n\nSome markdown");
        out.assertAttributeEquals("mime.type", "text/plain");
    }

    @Test
    void structuredContentFallbackWithTrailingGarbageIsNotJson() {
        fake.nextResult = new McpCallResult("{\"full\":true}", "{\"price\":2} and more", false);
        runner.setProperty(InvokeMcpServer.TOOL_NAME, "search");
        runner.setProperty(InvokeMcpServer.OUTPUT_CONTENT, InvokeMcpServer.OutputContent.STRUCTURED_CONTENT);
        runner.enqueue("{}");

        runner.run();

        runner.getFlowFilesForRelationship(InvokeMcpServer.SUCCESS).get(0).assertAttributeEquals("mime.type", "text/plain");
    }

    @Test
    void structuredContentIsInvalidForNonToolOperations() {
        runner.setProperty(InvokeMcpServer.OPERATION, InvokeMcpServer.Operation.READ_RESOURCE);
        runner.setProperty(InvokeMcpServer.RESOURCE_URI, "file:///x");
        runner.setProperty(InvokeMcpServer.OUTPUT_CONTENT, InvokeMcpServer.OutputContent.STRUCTURED_CONTENT);
        runner.assertNotValid();

        // Hidden by dependsOn for List operations, so a leftover value must not block them.
        runner.setProperty(InvokeMcpServer.OPERATION, InvokeMcpServer.Operation.LIST_TOOLS);
        runner.assertValid();
    }
}
