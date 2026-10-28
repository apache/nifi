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

/**
 * Outcome of a single MCP request.
 *
 * @param resultJson            the full result object as JSON, exactly as received
 * @param text                  all text parts of the result concatenated with newlines (tool content,
 *                              resource contents or prompt messages); empty if there are none
 * @param isError               the tool-level {@code isError} flag; always false for non-tool requests
 * @param structuredContentJson the tool's {@code structuredContent} as JSON, or {@code null} when the
 *                              server sent none; always null for non-tool requests
 */
public record McpCallResult(String resultJson, String text, boolean isError, String structuredContentJson) {

    public McpCallResult(final String resultJson, final String text, final boolean isError) {
        this(resultJson, text, isError, null);
    }
}
