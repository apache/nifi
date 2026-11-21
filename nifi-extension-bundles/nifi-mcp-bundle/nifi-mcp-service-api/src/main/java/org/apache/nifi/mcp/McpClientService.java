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
package org.apache.nifi.mcp;

import org.apache.nifi.controller.ControllerService;

/**
 * A live MCP client session towards one remote MCP server, shared by every processor that
 * references the same controller service instance.
 *
 * <p>Deliberately SDK-agnostic: arguments go in as JSON text and results come back as JSON text,
 * so processors (and their tests) never touch MCP Java SDK types, and the JSON written to
 * FlowFiles is exactly what the server put on the wire.
 *
 * <p>Implementations must be thread-safe: one session is shared by all concurrent tasks of all
 * referencing processors.
 */
public interface McpClientService extends ControllerService {

    /**
     * {@code tools/call}. A tool that ran but reported failure is NOT an exception - it comes back
     * with {@link McpCallResult#isError()} set, exactly as the protocol models it.
     *
     * @param argumentsJson JSON object text; {@code null} or blank means no arguments
     * @throws McpClientException transport, protocol or argument-parsing failure
     */
    McpCallResult callTool(String toolName, String argumentsJson) throws McpClientException;

    /** {@code tools/list}, following pagination; returns {@code {"tools":[...]}}. */
    String listTools() throws McpClientException;

    /** {@code resources/list}, following pagination; returns {@code {"resources":[...]}}. */
    String listResources() throws McpClientException;

    /** {@code resources/read}; returns the full ReadResourceResult as JSON. */
    McpCallResult readResource(String uri) throws McpClientException;

    /** {@code prompts/list}, following pagination; returns {@code {"prompts":[...]}}. */
    String listPrompts() throws McpClientException;

    /**
     * {@code prompts/get}.
     *
     * @param argumentsJson JSON object of string values; {@code null} or blank means no arguments
     */
    McpCallResult getPrompt(String promptName, String argumentsJson) throws McpClientException;

    /** {@code name/version} the remote server reported during initialize, for provenance/attributes. */
    String serverDescription();
}
