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
package org.apache.nifi.fn.runtimes.OpenWhisk;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.nifi.fn.core.FnFlow;
import org.apache.nifi.fn.core.FnFlowFile;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Queue;

public class NiFiFnOpenWhiskAction {

    private HttpServer server;
    private boolean initialized = false;
    private FnFlow flow = null;

    public NiFiFnOpenWhiskAction(int port) throws IOException {
        this.server = HttpServer.create(new InetSocketAddress(port), -1);

        this.server.createContext("/init", new InitHandler());
        this.server.createContext("/run", new RunHandler());
        this.server.setExecutor(null); // creates a default executor
    }

    public void start() {
        server.start();
    }

    private static void writeResponse(HttpExchange t, int code, String content) throws IOException {
        byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
        t.sendResponseHeaders(code, bytes.length);
        OutputStream os = t.getResponseBody();
        os.write(bytes);
        os.close();
    }

    private static void writeError(HttpExchange t, String errorMessage) throws IOException {
        JsonObject message = new JsonObject();
        message.addProperty("error", errorMessage);
        writeResponse(t, 502, message.toString());
    }

    private static void writeLogMarkers() {
        System.out.println("XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX");
        System.err.println("XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX");
        System.out.flush();
        System.err.flush();
    }

    private class InitHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            initialized = true;
            writeResponse(t, 200,"Initialized");

                InputStream is = t.getRequestBody();
            JsonParser parser = new JsonParser();
            JsonObject body = parser.parse(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))).getAsJsonObject();
            System.out.println("Init input: " + body);
            String code = body.get("value").getAsJsonObject().get("code").getAsString();

            if(code.equals("GENERIC")){
                initialized = true;
                writeResponse(t, 200,"Initialized Generic Action");
            } else {
                JsonObject flowDefinition = parser.parse(code).getAsJsonObject();
                try {
                    flow = FnFlow.createAndEnqueueFromJSON(flowDefinition);
                    initialized = true;
                    writeResponse(t, 200, "Initialized "+flow);
                } catch (Exception e) {
                    e.printStackTrace(System.err);
                    writeResponse(t, 400, "Error: " + e.getMessage());
                }
            }
        }
    }

    private class RunHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            if (!initialized) {
                NiFiFnOpenWhiskAction.writeError(t, "Cannot invoke an uninitialized action.");
                return;
            }

            try {
                InputStream is = t.getRequestBody();
                JsonParser parser = new JsonParser();
                JsonObject body = parser.parse(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))).getAsJsonObject();
                JsonObject inputObject = body.getAsJsonObject("value");
                /*
                input body :
                    {
                        "activation_id":"e212d293aa73479d92d293aa73c79dc9",
                        "action_name":"/guest/nififn",
                        "deadline":"1541729057462",
                        "api_key":"23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP",
                        "value":{"registry":"http://172.26.224.116:61080","SourceCluster":"hdfs://172.26.224.119:8020","SourceFile":"test.txt","SourceDirectory":"hdfs://172.26.224.119:8020/tmp/nififn/input/","flow":"6cf8277a-c402-4957-8623-0fa9890dd45d","bucket":"e53b8a0d-5c85-4fcd-912a-1c549a586c83","DestinationDirectory":"hdfs://172.26.224.119:8020/tmp/nififn/output"},
                        "namespace":"guest"
                    }
                input headers:
                    Accept-encoding: [gzip,deflate]
                    Accept: [application/json]
                    Connection: [Keep-Alive]
                    Host: [10.0.0.166:8080]
                    User-agent: [Apache-HttpClient/4.5.5 (Java/1.8.0-adoptopenjdk)]
                    Content-type: [application/json]
                    Content-length: [595]
                 */


                // Run Flow
                Queue<FnFlowFile> output = new LinkedList<>();
                boolean successful;
                if(flow == null) {
                    FnFlow tempFlow = FnFlow.createAndEnqueueFromJSON(inputObject);
                    successful = tempFlow.runOnce(output);
                } else {
                    flow.enqueueFromJSON(inputObject);
                    successful = flow.runOnce(output);
                }

                StringBuilder response = new StringBuilder();
                for(FnFlowFile file : output)
                    response.append("\n").append(file);
                NiFiFnOpenWhiskAction.writeResponse(t, successful ? 200 : 400, response.toString());

            } catch (Exception e) {
                e.printStackTrace(System.err);
                NiFiFnOpenWhiskAction.writeError(t, "An error has occurred (see logs for details): " + e);
            } finally {
                writeLogMarkers();
            }
        }
    }
}

