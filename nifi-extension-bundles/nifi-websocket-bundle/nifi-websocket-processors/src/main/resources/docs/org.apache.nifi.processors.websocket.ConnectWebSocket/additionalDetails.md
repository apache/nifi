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

# ConnectWebSocket

## Summary

This processor acts as a WebSocket client endpoint to interact with a remote WebSocket server. It is capable of
receiving messages from a websocket server and it transfers them to downstream relationships according to the received
message types.

The processor may have an incoming relationship, in which case flowfile attributes are passed down to its WebSocket
Client Service. This can be used to fine-tune the connection configuration (url and headers for example). For example "
dynamic.url = currentValue" flowfile attribute can be referenced in the WebSocket Client Service with the ${dynamic.url}
expression.

You can define custom websocket headers in the incoming flowfile as additional attributes. The attribute key shall start
with "header." and continue with they header key. For example: "header.Authorization". The attribute value will be the
corresponding header value. If a new flowfile is passed to the processor, the previous sessions will be closed, and any
data being sent will be aborted.

1. header.Autorization | Basic base64UserNamePassWord
2. header.Content-Type | application, audio, example

For multiple header values provide a comma separated list.