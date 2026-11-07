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

import io.modelcontextprotocol.client.McpClient;
import io.modelcontextprotocol.client.McpSyncClient;
import io.modelcontextprotocol.client.transport.HttpClientStreamableHttpTransport;
import io.modelcontextprotocol.client.transport.customizer.McpSyncHttpClientRequestCustomizer;
import io.modelcontextprotocol.json.McpJsonMapper;
import io.modelcontextprotocol.json.TypeRef;
import io.modelcontextprotocol.json.jackson3.JacksonMcpJsonMapper;
import io.modelcontextprotocol.spec.McpClientTransport;
import io.modelcontextprotocol.spec.McpError;
import io.modelcontextprotocol.spec.McpSchema;
import io.modelcontextprotocol.spec.McpSchema.CallToolRequest;
import io.modelcontextprotocol.spec.McpSchema.CallToolResult;
import io.modelcontextprotocol.spec.McpSchema.GetPromptRequest;
import io.modelcontextprotocol.spec.McpSchema.GetPromptResult;
import io.modelcontextprotocol.spec.McpSchema.ListPromptsResult;
import io.modelcontextprotocol.spec.McpSchema.ListResourcesResult;
import io.modelcontextprotocol.spec.McpSchema.ListToolsResult;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceRequest;
import io.modelcontextprotocol.spec.McpSchema.ReadResourceResult;
import org.apache.nifi.annotation.behavior.SupportsSensitiveDynamicProperties;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.VerifiableControllerService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.mcp.McpCallResult;
import org.apache.nifi.mcp.McpClientException;
import org.apache.nifi.mcp.McpClientService;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.ssl.SSLContextProvider;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.net.ssl.SSLContext;

/**
 * Holds one MCP client session towards a remote MCP server and shares it across every
 * processor referencing this service.
 *
 * <p>The session is opened eagerly in {@code onEnabled} but failures there are only logged: a
 * remote server that is briefly down must not leave the service stuck in ENABLING and every
 * referencing processor invalid. Instead each call goes through {@link #client()}, which
 * (re)connects lazily. Any failure that is not a JSON-RPC error answered by the server
 * ({@link McpError}) drops the session, so a restarted server - which no longer knows our
 * {@code Mcp-Session-Id} - is picked up on the next call rather than failing forever. Calls are
 * never retried automatically: a {@code tools/call} is not guaranteed idempotent, so whether to
 * retry is a flow decision (the processor's {@code failure} relationship), not this service's.
 */
@Tags({"mcp", "model context protocol", "ai", "agent", "client"})
@CapabilityDescription("""
        Maintains an MCP (Model Context Protocol) client session towards a remote MCP server over the Streamable HTTP
        transport, and shares it with processors such as InvokeMcpServer.
        Dynamic properties are sent as HTTP headers on every request.
        """)
@SupportsSensitiveDynamicProperties
public class StandardMcpClientService extends AbstractControllerService
        implements McpClientService, VerifiableControllerService {

    static final PropertyDescriptor SERVER_URL = new PropertyDescriptor.Builder()
            .name("Server URL")
            .description("Full URL of the MCP Streamable HTTP endpoint, including its path, such as http://localhost:8080/mcp")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.URL_VALIDATOR)
            .build();

    static final PropertyDescriptor SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
            .name("SSL Context Service")
            .description("""
                    TLS configuration for https Server URLs. A configured keystore is presented as a client certificate
                    for mutual TLS.
                    """)
            .required(false)
            .identifiesControllerService(SSLContextProvider.class)
            .build();

    static final PropertyDescriptor BEARER_TOKEN = new PropertyDescriptor.Builder()
            .name("Bearer Token")
            .description("Token sent as 'Authorization: Bearer <token>' on every HTTP request")
            .required(false)
            .sensitive(true)
            .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Connect Timeout")
            .description("Maximum time to establish the HTTP connection and complete the MCP initialize handshake")
            .required(true)
            .defaultValue("10 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor REQUEST_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Request Timeout")
            .description("""
                    Maximum time to wait for the response to a single MCP request, such as one tool call.
                    Long-running tools need a correspondingly long value.
                    """)
            .required(true)
            .defaultValue("60 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    static final PropertyDescriptor CLIENT_NAME = new PropertyDescriptor.Builder()
            .name("Client Name")
            .description("Name this client reports as clientInfo.name in the MCP initialize handshake")
            .required(true)
            .defaultValue("apache-nifi")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = List.of(
            SERVER_URL, SSL_CONTEXT_SERVICE, BEARER_TOKEN, CONNECT_TIMEOUT, REQUEST_TIMEOUT, CLIENT_NAME);

    private static final String CLIENT_VERSION = Objects.requireNonNullElse(
            StandardMcpClientService.class.getPackage().getImplementationVersion(), "unknown");
    private static final TypeRef<Map<String, Object>> MAP_TYPE = new TypeRef<>() { };

    private final McpJsonMapper jsonMapper = new JacksonMcpJsonMapper(tools.jackson.databind.json.JsonMapper.builder().build());

    /** Everything needed to build a fresh client; captured once in onEnabled. */
    private volatile ConnectionSettings settings;
    private McpSyncClient client; // guarded by this

    private record ConnectionSettings(String serverUrl, SSLContext sslContext,
                                      Map<String, String> headers, Duration connectTimeout,
                                      Duration requestTimeout, String clientName) {
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .description("Sent as the HTTP header '" + propertyDescriptorName + "' on every MCP request.")
                .dynamic(true)
                .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .build();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        settings = readSettings(context);
        try {
            client();
        } catch (final McpClientException e) {
            getLogger().warn("Could not connect to MCP server yet ({}); will retry on first use", describe(e));
        }
    }

    @OnDisabled
    public synchronized void onDisabled() {
        dropClient();
        settings = null;
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger,
                                                 final Map<String, String> variables) {
        final List<ConfigVerificationResult> results = new ArrayList<>();
        McpSyncClient probe = null;
        try {
            probe = connect(readSettings(context));
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Connect and initialize")
                    .outcome(Outcome.SUCCESSFUL)
                    .explanation("Connected to " + describeServer(probe) + " (protocol "
                            + probe.getCurrentInitializationResult().protocolVersion() + ")")
                    .build());
            if (probe.getServerCapabilities() != null && probe.getServerCapabilities().tools() != null) {
                final int count = listAll(probe::listTools, ListToolsResult::tools, ListToolsResult::nextCursor).size();
                results.add(new ConfigVerificationResult.Builder()
                        .verificationStepName("List tools")
                        .outcome(Outcome.SUCCESSFUL)
                        .explanation("Server exposes " + count + " tool(s)")
                        .build());
            }
        } catch (final Exception e) {
            verificationLogger.warn("MCP verification failed", e);
            results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Connect and initialize")
                    .outcome(Outcome.FAILED)
                    .explanation(describe(e))
                    .build());
        } finally {
            if (probe != null) {
                closeQuietly(probe);
            }
        }
        return results;
    }

    @Override
    public McpCallResult callTool(final String toolName, final String argumentsJson) throws McpClientException {
        final Map<String, Object> arguments = parseArguments(argumentsJson);
        final CallToolResult result = execute(c -> c.callTool(CallToolRequest.builder(toolName).arguments(arguments).build()));
        final String text = joinText(result.content() == null ? List.of() : result.content().stream()
                .map(StandardMcpClientService::textOf).toList());
        final String structured = result.structuredContent() == null ? null : toJson(result.structuredContent());
        return new McpCallResult(toJson(result), text, Boolean.TRUE.equals(result.isError()), structured);
    }

    @Override
    public String listTools() throws McpClientException {
        return toJson(Map.of("tools", execute(c -> listAll(c::listTools, ListToolsResult::tools, ListToolsResult::nextCursor))));
    }

    @Override
    public String listResources() throws McpClientException {
        return toJson(Map.of("resources", execute(c -> listAll(c::listResources, ListResourcesResult::resources,
                ListResourcesResult::nextCursor))));
    }

    @Override
    public McpCallResult readResource(final String uri) throws McpClientException {
        final ReadResourceResult result = execute(c -> c.readResource(ReadResourceRequest.builder(uri).build()));
        final String text = joinText(result.contents() == null ? List.of() : result.contents().stream()
                .map(StandardMcpClientService::textOf).toList());
        return new McpCallResult(toJson(result), text, false);
    }

    @Override
    public String listPrompts() throws McpClientException {
        return toJson(Map.of("prompts", execute(c -> listAll(c::listPrompts, ListPromptsResult::prompts,
                ListPromptsResult::nextCursor))));
    }

    @Override
    public McpCallResult getPrompt(final String promptName, final String argumentsJson) throws McpClientException {
        final Map<String, Object> arguments = parseArguments(argumentsJson);
        final GetPromptResult result = execute(c -> c.getPrompt(GetPromptRequest.builder(promptName).arguments(arguments).build()));
        final String text = joinText(result.messages() == null ? List.of() : result.messages().stream()
                .map(m -> textOf(m.content())).toList());
        return new McpCallResult(toJson(result), text, false);
    }

    @Override
    public String serverDescription() {
        final McpSyncClient current;
        synchronized (this) {
            current = client;
        }
        return current == null ? "" : describeServer(current);
    }

    @Override
    public String toString() {
        return "StandardMcpClientService[id=" + getIdentifier() + "]";
    }

    // ---------------------------------------------------------------------------------------------

    private <T> T execute(final Function<McpSyncClient, T> request) throws McpClientException {
        final McpSyncClient current = client();
        try {
            return request.apply(current);
        } catch (final McpError e) {
            // The server answered with a JSON-RPC error (unknown tool, bad params...): the session
            // itself is fine, keep it.
            throw new McpClientException("MCP server returned an error: " + describe(e), e);
        } catch (final RuntimeException e) {
            // Transport-level: timeout, connection refused, session expired after a server restart.
            // Drop the session so the next call starts a fresh one instead of failing forever.
            invalidate(current);
            throw new McpClientException("MCP request failed: " + describe(e), e);
        }
    }

    private synchronized McpSyncClient client() throws McpClientException {
        if (client != null) {
            return client;
        }
        final ConnectionSettings current = settings;
        if (current == null) {
            throw new McpClientException("MCP Client Service is not enabled");
        }
        try {
            client = connect(current);
            getLogger().info("Connected to MCP server {}", describeServer(client));
            return client;
        } catch (final RuntimeException e) {
            throw new McpClientException("Failed to connect to MCP server: " + describe(e), e);
        }
    }

    private synchronized void invalidate(final McpSyncClient failed) {
        // Only drop the session the failed call actually used - another thread may already have
        // replaced it with a fresh one.
        if (client == failed) {
            dropClient();
        }
    }

    private synchronized void dropClient() {
        if (client != null) {
            closeQuietly(client);
            client = null;
        }
    }

    private McpSyncClient connect(final ConnectionSettings s) {
        final McpSyncClient newClient = McpClient.sync(buildTransport(s))
                .clientInfo(McpSchema.Implementation.builder(s.clientName(), CLIENT_VERSION).build())
                .initializationTimeout(s.connectTimeout())
                .requestTimeout(s.requestTimeout())
                .build();
        try {
            newClient.initialize();
            return newClient;
        } catch (final RuntimeException e) {
            closeQuietly(newClient);
            throw e;
        }
    }

    private McpClientTransport buildTransport(final ConnectionSettings s) {
        final URI uri = URI.create(s.serverUrl());
        final String base = uri.getScheme() + "://" + uri.getRawAuthority();
        final String path = (uri.getRawPath() == null || uri.getRawPath().isEmpty() ? "/" : uri.getRawPath())
                + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
        final McpSyncHttpClientRequestCustomizer headers = (builder, method, endpoint, body, ctx) ->
                s.headers().forEach(builder::header);

        return HttpClientStreamableHttpTransport.builder(base)
                .endpoint(path)
                .jsonMapper(jsonMapper)
                .connectTimeout(s.connectTimeout())
                .customizeClient(b -> applySsl(b, s))
                .httpRequestCustomizer(headers)
                .build();
    }

    private static void applySsl(final java.net.http.HttpClient.Builder builder, final ConnectionSettings s) {
        if (s.sslContext() != null) {
            builder.sslContext(s.sslContext());
        }
    }

    private ConnectionSettings readSettings(final ConfigurationContext context) {
        final String serverUrl = context.getProperty(SERVER_URL).evaluateAttributeExpressions().getValue();
        final SSLContextProvider sslProvider =
                context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextProvider.class);
        final SSLContext sslContext = sslProvider == null ? null : sslProvider.createContext();

        final Map<String, String> headers = new LinkedHashMap<>();
        context.getProperties().keySet().stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(d -> headers.put(d.getName(), context.getProperty(d).evaluateAttributeExpressions().getValue()));
        final String token = context.getProperty(BEARER_TOKEN).evaluateAttributeExpressions().getValue();
        if (token != null && !token.isBlank()) {
            headers.put("Authorization", "Bearer " + token.trim());
        }

        return new ConnectionSettings(serverUrl, sslContext, Map.copyOf(headers),
                Duration.ofMillis(context.getProperty(CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                Duration.ofMillis(context.getProperty(REQUEST_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)),
                context.getProperty(CLIENT_NAME).getValue());
    }

    private Map<String, Object> parseArguments(final String argumentsJson) throws McpClientException {
        if (argumentsJson == null || argumentsJson.isBlank()) {
            return Map.of();
        }
        try {
            final Map<String, Object> parsed = jsonMapper.readValue(argumentsJson, MAP_TYPE);
            return parsed == null ? Map.of() : parsed;
        } catch (final Exception e) {
            throw new McpClientException("Arguments are not a JSON object: " + describe(e), e);
        }
    }

    private String toJson(final Object value) throws McpClientException {
        try {
            return jsonMapper.writeValueAsString(value);
        } catch (final Exception e) {
            throw new McpClientException("Failed to serialize MCP result: " + describe(e), e);
        }
    }

    /** Follows {@code nextCursor} until the server stops returning one. */
    private static <R, T> List<T> listAll(final Function<String, R> page, final Function<R, List<T>> items,
                                          final Function<R, String> nextCursor) {
        final List<T> all = new ArrayList<>();
        String cursor = null;
        do {
            final R result = page.apply(cursor);
            if (items.apply(result) != null) {
                all.addAll(items.apply(result));
            }
            cursor = nextCursor.apply(result);
        } while (cursor != null && !cursor.isEmpty());
        return all;
    }

    private static String textOf(final Object content) {
        if (content instanceof McpSchema.TextContent text) {
            return text.text();
        }
        if (content instanceof McpSchema.EmbeddedResource embedded) {
            return textOf(embedded.resource());
        }
        if (content instanceof McpSchema.TextResourceContents resource) {
            return resource.text();
        }
        return null; // images, audio, blobs: only available in the full JSON result
    }

    private static String joinText(final List<String> parts) {
        return String.join("\n", parts.stream().filter(p -> p != null).toList());
    }

    private static String describeServer(final McpSyncClient c) {
        final McpSchema.Implementation info = c.getServerInfo();
        return info == null ? "" : info.name() + "/" + info.version();
    }

    private void closeQuietly(final McpSyncClient c) {
        try {
            if (!c.closeGracefully()) {
                c.close();
            }
        } catch (final RuntimeException e) {
            getLogger().debug("Error closing MCP client", e);
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
