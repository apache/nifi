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

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.mcp.McpCallResult;
import org.apache.nifi.mcp.McpClientException;
import org.apache.nifi.mcp.McpClientService;
import org.apache.nifi.mcp.client.StandardMcpClientService;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * NiFi as an MCP <em>client</em>: performs one MCP request per FlowFile against the server held by
 * an {@link McpClientService} - typically {@code tools/call}, with the FlowFile content (or a
 * property, with Expression Language) as the tool's JSON arguments.
 *
 * <p>Shaped after {@code InvokeHTTP}: the request's result becomes a new FlowFile on
 * {@code success}, the input goes to {@code original}, and a request that never produced a result
 * (connection refused, timeout, JSON-RPC error, malformed arguments) penalizes the input and routes
 * it to {@code failure}. The one MCP-specific twist is {@code tool error}: a tool that ran and
 * answered {@code isError=true} is a successful protocol exchange carrying a business failure, so
 * its result is kept (it usually explains what went wrong) but routed separately.
 *
 * <p>Without an incoming connection the processor acts as a source and issues the request on its
 * own schedule - handy for polling a tool or a resource, or for {@code List Tools} discovery.
 */
@Tags({"mcp", "model context protocol", "ai", "agent", "tool", "client", "invoke"})
@CapabilityDescription("""
        Acts as an MCP (Model Context Protocol) client: calls a tool, reads a resource, gets a prompt or lists the
        tools, resources or prompts of the remote MCP server configured in an MCP Client Service.
        For Call Tool, the tool's arguments are a JSON object taken from the FlowFile content or from the Arguments property.
        The result is written as a new FlowFile and the incoming FlowFile is routed to 'original'.
        """)
@InputRequirement(Requirement.INPUT_ALLOWED)
@SupportsBatching
@SeeAlso(StandardMcpClientService.class)
@WritesAttributes({
        @WritesAttribute(attribute = InvokeMcpServer.ATTR_OPERATION, description = "The MCP operation performed"),
        @WritesAttribute(attribute = InvokeMcpServer.ATTR_TARGET, description = "Tool name, prompt name or resource URI"),
        @WritesAttribute(attribute = InvokeMcpServer.ATTR_IS_ERROR, description = "The tool's isError flag (Call Tool only)"),
        @WritesAttribute(attribute = InvokeMcpServer.ATTR_SERVER, description = "name/version of the MCP server"),
        @WritesAttribute(attribute = InvokeMcpServer.ATTR_DURATION, description = "Request duration in milliseconds"),
        @WritesAttribute(attribute = InvokeMcpServer.ATTR_ERROR_MESSAGE, description = "Why the request failed (failure relationship only)"),
        @WritesAttribute(attribute = "mime.type", description = """
                application/json or text/plain, depending on Output Content.
                For Structured Content falling back to text, application/json only if that text is valid JSON.
                """)
})
public class InvokeMcpServer extends AbstractProcessor {

    private static final JsonMapper JSON_MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .build();

    static final String ATTR_OPERATION = "mcp.operation";
    static final String ATTR_TARGET = "mcp.target";
    static final String ATTR_IS_ERROR = "mcp.is.error";
    static final String ATTR_SERVER = "mcp.server";
    static final String ATTR_DURATION = "mcp.duration.ms";
    static final String ATTR_ERROR_MESSAGE = "mcp.error.message";

    public enum Operation implements DescribedValue {
        CALL_TOOL("Call Tool", "tools/call - invoke a tool with JSON arguments"),
        READ_RESOURCE("Read Resource", "resources/read - read one resource by URI"),
        GET_PROMPT("Get Prompt", "prompts/get - render a prompt template with arguments"),
        LIST_TOOLS("List Tools", "tools/list - all tools with their input schemas"),
        LIST_RESOURCES("List Resources", "resources/list"),
        LIST_PROMPTS("List Prompts", "prompts/list");

        private final String displayName;
        private final String description;

        Operation(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public enum ArgumentsSource implements DescribedValue {
        CONTENT("FlowFile Content", "The incoming FlowFile content is the JSON arguments object"),
        PROPERTY("Arguments Property", "The Arguments property (Expression Language enabled) is the JSON arguments object");

        private final String displayName;
        private final String description;

        ArgumentsSource(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    public enum OutputContent implements DescribedValue {
        FULL_RESULT("Full Result (JSON)", "The complete MCP result object as JSON, including non-text content, structuredContent and _meta"),
        TEXT("Text Only", """
                Only the text parts of the result, joined by newlines.
                For a tool returning JSON as text this is that JSON, ready for record processors.
                """),
        STRUCTURED_CONTENT("Structured Content (JSON)", """
                Call Tool only: the tool's structuredContent object, ready for record processors.
                For a tool that returns no structuredContent, falls back to Text Only, which by the MCP specification
                should then hold the same JSON serialized as text; when it does not, mime.type is set to text/plain.
                """);

        private final String displayName;
        private final String description;

        OutputContent(final String displayName, final String description) {
            this.displayName = displayName;
            this.description = description;
        }

        @Override
        public String getValue() {
            return name();
        }

        @Override
        public String getDisplayName() {
            return displayName;
        }

        @Override
        public String getDescription() {
            return description;
        }
    }

    static final PropertyDescriptor MCP_CLIENT_SERVICE = new PropertyDescriptor.Builder()
            .name("MCP Client Service")
            .description("Holds the session to the MCP server to invoke.")
            .required(true)
            .identifiesControllerService(McpClientService.class)
            .build();

    static final PropertyDescriptor OPERATION = new PropertyDescriptor.Builder()
            .name("Operation")
            .description("The MCP request to perform.")
            .required(true)
            .allowableValues(Operation.class)
            .defaultValue(Operation.CALL_TOOL)
            .build();

    static final PropertyDescriptor TOOL_NAME = new PropertyDescriptor.Builder()
            .name("Tool Name")
            .description("Name of the tool to call, exactly as listed by the server.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(OPERATION, Operation.CALL_TOOL)
            .build();

    static final PropertyDescriptor PROMPT_NAME = new PropertyDescriptor.Builder()
            .name("Prompt Name")
            .description("Name of the prompt to get.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(OPERATION, Operation.GET_PROMPT)
            .build();

    static final PropertyDescriptor RESOURCE_URI = new PropertyDescriptor.Builder()
            .name("Resource URI")
            .description("URI of the resource to read, e.g. file:///logs/app.log.")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(OPERATION, Operation.READ_RESOURCE)
            .build();

    static final PropertyDescriptor ARGUMENTS_SOURCE = new PropertyDescriptor.Builder()
            .name("Arguments Source")
            .description("""
                    Where the JSON arguments object comes from. Empty content or an empty property means no arguments.
                    When the processor has no incoming connection, use Arguments Property.
                    """)
            .required(true)
            .allowableValues(ArgumentsSource.class)
            .defaultValue(ArgumentsSource.CONTENT)
            .dependsOn(OPERATION, Operation.CALL_TOOL, Operation.GET_PROMPT)
            .build();

    static final PropertyDescriptor ARGUMENTS = new PropertyDescriptor.Builder()
            .name("Arguments")
            .description("""
                    JSON object of arguments, such as {"path": "${absolute.path}${filename}", "limit": 10}.
                    Expression Language is evaluated first, so substituted values must be JSON-escaped,
                    such as ${attr:escapeJson()}.
                    """)
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .dependsOn(ARGUMENTS_SOURCE, ArgumentsSource.PROPERTY)
            .build();

    static final PropertyDescriptor OUTPUT_CONTENT = new PropertyDescriptor.Builder()
            .name("Output Content")
            .description("What to write as the content of the result FlowFile. List operations always produce JSON.")
            .required(true)
            .allowableValues(OutputContent.class)
            .defaultValue(OutputContent.FULL_RESULT)
            .dependsOn(OPERATION, Operation.CALL_TOOL, Operation.READ_RESOURCE, Operation.GET_PROMPT)
            .build();

    static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("The MCP result, one FlowFile per request (inheriting the incoming FlowFile's attributes).")
            .build();

    static final Relationship TOOL_ERROR = new Relationship.Builder()
            .name("tool error")
            .description("""
                    Call Tool only: the tool ran but answered isError=true.
                    The FlowFile holds its result, which normally describes the error.
                    """)
            .build();

    static final Relationship ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("The incoming FlowFile, once its request produced a result (success or tool error).")
            .build();

    static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("""
                    The incoming FlowFile, penalized, when the request produced no result: server unreachable, timeout,
                    JSON-RPC error such as an unknown tool, or arguments that are not a JSON object.
                    """)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            MCP_CLIENT_SERVICE, OPERATION, TOOL_NAME, PROMPT_NAME, RESOURCE_URI,
            ARGUMENTS_SOURCE, ARGUMENTS, OUTPUT_CONTENT);

    private static final Set<Relationship> RELATIONSHIPS = Set.of(SUCCESS, TOOL_ERROR, ORIGINAL, FAILURE);

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final List<ValidationResult> results = new ArrayList<>();
        final Operation operation = Operation.valueOf(context.getProperty(OPERATION).getValue());
        final String outputContent = context.getProperty(OUTPUT_CONTENT).getValue();
        // Only where Output Content applies: a leftover value hidden by dependsOn must not invalidate List operations.
        if ((operation == Operation.READ_RESOURCE || operation == Operation.GET_PROMPT)
                && OutputContent.STRUCTURED_CONTENT.getValue().equals(outputContent)) {
            results.add(new ValidationResult.Builder()
                    .subject(OUTPUT_CONTENT.getDisplayName())
                    .valid(false)
                    .explanation(OutputContent.STRUCTURED_CONTENT.getDisplayName() + " is only available for "
                            + Operation.CALL_TOOL.getDisplayName())
                    .build());
        }
        return results;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = session.get();
        if (input == null && context.hasIncomingConnection()) {
            return;
        }
        // input == null from here on means source mode: nothing upstream, the request is issued on
        // this processor's own schedule and there is no original FlowFile to route anywhere.

        final McpClientService client = context.getProperty(MCP_CLIENT_SERVICE).asControllerService(McpClientService.class);
        final Operation operation = Operation.valueOf(context.getProperty(OPERATION).getValue());
        final boolean producesResult = operation == Operation.CALL_TOOL || operation == Operation.READ_RESOURCE
                || operation == Operation.GET_PROMPT; // list operations have no Output Content choice
        final OutputContent outputContent = producesResult
                ? OutputContent.valueOf(context.getProperty(OUTPUT_CONTENT).getValue()) : OutputContent.FULL_RESULT;
        final boolean textOnly = outputContent == OutputContent.TEXT;

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(ATTR_OPERATION, operation.getDisplayName());

        final long start = System.nanoTime();
        final String output;
        boolean isError = false;
        boolean jsonOutput = !textOnly;
        try {
            switch (operation) {
                case CALL_TOOL -> {
                    final String tool = context.getProperty(TOOL_NAME).evaluateAttributeExpressions(input).getValue();
                    attributes.put(ATTR_TARGET, tool);
                    final McpCallResult result = client.callTool(tool, arguments(context, session, input));
                    isError = result.isError();
                    attributes.put(ATTR_IS_ERROR, String.valueOf(isError));
                    if (outputContent == OutputContent.STRUCTURED_CONTENT) {
                        // Structured output is JSON by definition; the text fallback is JSON per the spec.
                        if (result.structuredContentJson() != null) {
                            output = result.structuredContentJson();
                        } else {
                            // Tools without an outputSchema often return plain text or markdown: don't label it JSON.
                            output = result.text();
                            jsonOutput = isJson(output);
                        }
                    } else {
                        output = textOnly ? result.text() : result.resultJson();
                    }
                }
                case GET_PROMPT -> {
                    final String prompt = context.getProperty(PROMPT_NAME).evaluateAttributeExpressions(input).getValue();
                    attributes.put(ATTR_TARGET, prompt);
                    final McpCallResult result = client.getPrompt(prompt, arguments(context, session, input));
                    output = textOnly ? result.text() : result.resultJson();
                }
                case READ_RESOURCE -> {
                    final String uri = context.getProperty(RESOURCE_URI).evaluateAttributeExpressions(input).getValue();
                    attributes.put(ATTR_TARGET, uri);
                    final McpCallResult result = client.readResource(uri);
                    output = textOnly ? result.text() : result.resultJson();
                }
                case LIST_TOOLS -> output = client.listTools();
                case LIST_RESOURCES -> output = client.listResources();
                case LIST_PROMPTS -> output = client.listPrompts();
                default -> throw new IllegalStateException("Unhandled operation " + operation);
            }
        } catch (final McpClientException | IOException e) {
            if (input == null) {
                getLogger().error("MCP {} failed; yielding", operation.getDisplayName(), e);
                context.yield();
                return;
            }
            getLogger().error("MCP {} failed for {}; routing to failure", operation.getDisplayName(), input, e);
            input = session.putAttribute(input, ATTR_ERROR_MESSAGE, describe(e));
            session.transfer(session.penalize(input), FAILURE);
            return;
        }
        final long durationMillis = (System.nanoTime() - start) / 1_000_000;

        attributes.put(ATTR_DURATION, String.valueOf(durationMillis));
        attributes.put(ATTR_SERVER, client.serverDescription());
        attributes.put("mime.type", jsonOutput ? "application/json" : "text/plain");

        FlowFile result = input == null ? session.create() : session.create(input);
        final byte[] bytes = output == null ? new byte[0] : output.getBytes(StandardCharsets.UTF_8);
        result = session.write(result, out -> out.write(bytes));
        result = session.putAllAttributes(result, attributes);

        final String transitUri = "mcp://" + client.serverDescription() + "/" + operation.name().toLowerCase(Locale.ROOT)
                + (attributes.containsKey(ATTR_TARGET) ? "/" + attributes.get(ATTR_TARGET) : "");
        if (input == null) {
            session.getProvenanceReporter().receive(result, transitUri, durationMillis); // brand-new FlowFile
        } else {
            session.getProvenanceReporter().fetch(result, transitUri, durationMillis);
        }
        session.transfer(result, isError ? TOOL_ERROR : SUCCESS);
        if (input != null) {
            session.transfer(input, ORIGINAL);
        }
    }

    private static boolean isJson(final String text) {
        if (text == null || text.isBlank()) {
            return false;
        }
        try {
            JSON_MAPPER.readTree(text);
            return true;
        } catch (final JacksonException e) {
            return false;
        }
    }

    private static String arguments(final ProcessContext context, final ProcessSession session, final FlowFile input)
            throws IOException {
        if (ArgumentsSource.valueOf(context.getProperty(ARGUMENTS_SOURCE).getValue()) == ArgumentsSource.PROPERTY) {
            return context.getProperty(ARGUMENTS).evaluateAttributeExpressions(input).getValue();
        }
        if (input == null) {
            return null; // source mode: no content to read arguments from
        }
        try (InputStream in = session.read(input)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static String describe(final Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current.getMessage() != null) {
                return current.getClass().getSimpleName() + ": " + current.getMessage();
            }
            current = current.getCause();
        }
        return t.getClass().getSimpleName();
    }
}
