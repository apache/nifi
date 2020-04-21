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
package org.apache.nifi.stateless.runtimes.openwhisk;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import org.apache.nifi.stateless.bootstrap.InMemoryFlowFile;
import org.apache.nifi.stateless.bootstrap.RunnableFlow;
import org.apache.nifi.stateless.core.StatelessFlow;
import org.apache.nifi.stateless.core.security.StatelessSecurityUtility;

public class StatelessNiFiOpenWhiskAction {

    private HttpServer server;
    private boolean initialized = false;
    private RunnableFlow flow = null;
    private ClassLoader systemClassLoader;
    private File narWorkingDirectory;

    public StatelessNiFiOpenWhiskAction(int port, final ClassLoader systemClassLoader, final File narWorkingDirectory) throws IOException {

        this.systemClassLoader = systemClassLoader;
        this.narWorkingDirectory = narWorkingDirectory;

        // TODO: This runs a plaintext HTTP server
        this.server = HttpServer.create(new InetSocketAddress(port), -1);

        this.server.createContext("/init", new InitHandler());
        this.server.createContext("/run", new RunHandler());
        this.server.setExecutor(null); // creates a default executor
    }


    public void start() {
        server.start();
    }

    private static void writeResponse(HttpExchange t, int code, String content) throws IOException {
        if(content.isEmpty())
            content = "success";

        JsonObject message = new JsonObject();
        message.addProperty("result", content);
        byte[] bytes = message.toString().getBytes(StandardCharsets.UTF_8);
        t.sendResponseHeaders(code, bytes.length);
        OutputStream os = t.getResponseBody();
        os.write(bytes);
        os.close();
    }
    private static void writeLogMarkers() {
        System.out.println("XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX");
        System.err.println("XXX_THE_END_OF_A_WHISK_ACTIVATION_XXX");
        System.out.flush();
        System.err.flush();
    }

    private class InitHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            System.out.println("Initializing");

            try {
                InputStream is = t.getRequestBody();
                JsonParser parser = new JsonParser();
                JsonObject body = parser.parse(new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))).getAsJsonObject();
                System.out.println("Init input: " + body);
                String code = body.get("value").getAsJsonObject().get("code").getAsJsonPrimitive().getAsString();
                System.out.println("Code input: " + code);

                if (code.equals("GENERIC")) {
                    initialized = true;
                    writeResponse(t, 200, "Initialized Generic Action");
                } else {

                    final JsonObject config = new JsonParser().parse(code).getAsJsonObject();
                    flow = StatelessFlow.createAndEnqueueFromJSON(config, systemClassLoader, narWorkingDirectory);

                    initialized = true;
                    writeResponse(t, 200, "Initialized Flow");
                }
            } catch (Exception e) {
                e.printStackTrace(System.err);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                String sStackTrace = sw.toString();
                writeResponse(t, 500, "Error: " + e.getMessage()+"\n"+sStackTrace);
            }
        }
    }

    private class RunHandler implements HttpHandler {
        public void handle(HttpExchange t) throws IOException {
            if (!initialized) {
                writeResponse(t, 500, "Cannot invoke an uninitialized action.");
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
                        "action_name":"/guest/nifistateless",
                        "deadline":"1541729057462",
                        "api_key":"23bc46b1-71f6-4ed5-8c54-816aa4f8c500:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP",
                        "value":{"registry":"http://172.26.224.116:61080","SourceCluster":"hdfs://172.26.224.119:8020","SourceFile":"test.txt",
                        "SourceDirectory":"hdfs://172.26.224.119:8020/tmp/nifistateless/input/","flow":"6cf8277a-c402-4957-8623-0fa9890dd45d","bucket":"e53b8a0d-5c85-4fcd-912a-1c549a586c83",
                        "DestinationDirectory":"hdfs://172.26.224.119:8020/tmp/nifistateless/output"},
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
                Queue<InMemoryFlowFile> output = new LinkedList<>();
                boolean successful;
                if (flow == null) {
                    System.out.println(StatelessSecurityUtility.formatJson(inputObject));

                    final JsonObject config = new JsonParser().parse(inputObject.get("code").getAsJsonPrimitive().getAsString()).getAsJsonObject();
                    RunnableFlow tempFlow = StatelessFlow.createAndEnqueueFromJSON(config, systemClassLoader, narWorkingDirectory);
                    successful = tempFlow.runOnce(output);
                } else {
                    System.out.println("Input: " + StatelessSecurityUtility.formatJson(inputObject));

                    Map<String,String> Attributes = inputObject.entrySet()
                            .stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, item -> item.getValue().getAsString()));
                    ((StatelessFlow)flow).enqueueFlowFile(new byte[0],Attributes);
                    successful = flow.runOnce(output);
                }

                StringBuilder response = new StringBuilder();
                for (InMemoryFlowFile flowFile : output) {
                    response.append("\n").append(flowFile);
                }

                writeResponse(t, successful ? 200 : 500, response.toString());

            } catch (Exception e) {
                e.printStackTrace(System.err);
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter(sw);
                e.printStackTrace(pw);
                String sStackTrace = sw.toString();
                // TODO: This leaks the stacktrace in the HTTP response
                writeResponse(t, 500, "An error has occurred (see logs for details): " + e.getMessage()+"\n"+sStackTrace);
            } finally {
                writeLogMarkers();
            }
        }
    }
}

