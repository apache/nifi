<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# InvokeMcpServer

### Description

InvokeMcpServer performs one Model Context Protocol (MCP) request against a remote MCP server for each incoming
FlowFile: it calls a tool, reads a resource, renders a prompt, or lists the server's tools, resources or prompts. The
connection itself (transport, URL, TLS, authentication headers, timeouts) is configured once in a
StandardMcpClientService, which holds a single MCP session shared by every processor that references it.

This allows MCP servers to be consumed deterministically, as regular flow steps, without an LLM in the loop: repetitive
calls such as polling a tool on a schedule or fetching a resource every day cost no tokens, and benefit from NiFi's
usual routing, retries and provenance.

### Arguments

Tool and prompt arguments are a JSON object. With **Arguments Source** set to *FlowFile Content*, the content of the
incoming FlowFile is used as-is. With *Arguments Property*, the **Arguments** property is evaluated with Expression
Language against the FlowFile attributes; values substituted into JSON strings should be escaped with `escapeJson()`:

```json
{"repoName": "${repo:escapeJson()}"}
```

### Output Content

* **Full Result (JSON)**: the complete MCP result object, including non-text content, `structuredContent` and `_meta`.
* **Text Only**: only the text parts of the result, joined by newlines (`mime.type` is `text/plain`).
* **Structured Content (JSON)** (Call Tool only): the tool's `structuredContent` object, ready for record processors.
  When the tool returns no `structuredContent`, the text parts are written instead, which by the MCP specification
  should hold the same JSON serialized as text; if they are not valid JSON, `mime.type` is set to `text/plain`.

### Relationships and retries

A tool that runs but reports a failure (`isError: true`) is routed to **tool error** together with its result, which
usually explains the problem. **failure** receives the incoming FlowFile, penalized, when no result was obtained at all:
server unreachable, timeout, JSON-RPC error such as an unknown tool, or arguments that are not a JSON object; the reason
is written to the `mcp.error.message` attribute.

Requests are never retried automatically, because a tool call is not necessarily idempotent. Whether routing
**failure** back to the processor is safe depends on the tool being called.

### Running without an incoming connection

With no incoming connection the processor acts as a source: each scheduled run performs the configured request (use
*Arguments Property* for arguments) and emits only the result. This is useful for polling a tool or a resource, or for
periodic discovery with List Tools. On failure in this mode the processor yields and emits nothing.
