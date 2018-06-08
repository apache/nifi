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
package org.apache.nifi.processors.livy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.web.util.TestServer;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

class ExecuteSparkInteractiveTestBase {

    public static class LivyAPIHandler extends AbstractHandler {

        int session1Requests = 0;

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException {
            baseRequest.setHandled(true);

            int responseStatus = 404;
            String responseContentType = "text/plain";
            String responseBody = "Not found";

            if ("GET".equalsIgnoreCase(request.getMethod())) {
                responseStatus = 200;
                responseBody = "{}";
                responseContentType = "application/json";
                if ("/sessions".equalsIgnoreCase(target)) {
                    responseBody = "{\"sessions\": [{\"id\": 1, \"kind\": \"spark\", \"state\": \"idle\"}]}";
                } else if (target.startsWith("/sessions/") && !target.contains("statement")) {
                    responseBody = "{\"id\": 1, \"kind\": \"spark\", \"state\": \"idle\"}";
                } else if ("/sessions/1/statements/7".equalsIgnoreCase(target)) {
                    switch (session1Requests) {
                        case 0:
                            responseBody = "{\"state\": \"waiting\"}";
                            break;
                        case 1:
                            responseBody = "{\"state\": \"running\"}";
                            break;
                        case 2:
                            responseBody = "{\"state\": \"available\", \"output\": {\"data\": {\"text/plain\": \"Hello world\"}}}";
                            break;
                        default:
                            responseBody = "{\"state\": \"error\"}";
                            break;
                    }
                    session1Requests++;
                }
            } else if ("POST".equalsIgnoreCase(request.getMethod())) {
                String requestBody = IOUtils.toString(request.getReader());
                try {
                    // validate JSON payload
                    new ObjectMapper().readTree(requestBody);

                    responseStatus = 200;
                    responseBody = "{}";
                    responseContentType = "application/json";
                    if ("/sessions".equalsIgnoreCase(target)) {
                        responseBody = "{\"id\": 1, \"kind\": \"spark\", \"state\": \"idle\"}";
                    } else if ("/sessions/1/statements".equalsIgnoreCase(target)) {
                        responseBody = "{\"id\": 7}";
                    }
                } catch (JsonProcessingException e) {
                    responseStatus = 400;
                    responseContentType = "text/plain";
                    responseBody = "Bad request";
                }
            }

            response.setStatus(responseStatus);
            response.setContentType(responseContentType);
            response.setContentLength(responseBody.length());

            try (PrintWriter writer = response.getWriter()) {
                writer.print(responseBody);
                writer.flush();
            }

        }
    }

    TestRunner runner;

    void testCode(TestServer server, String code) throws Exception {
        server.addHandler(new LivyAPIHandler());

        runner.enqueue(code);
        runner.run();
        List<MockFlowFile> waitingFlowfiles = runner.getFlowFilesForRelationship(ExecuteSparkInteractive.REL_WAIT);
        while (!waitingFlowfiles.isEmpty()) {
          Thread.sleep(1000);
          runner.clearTransferState();
          runner.enqueue(code);
          runner.run();
          waitingFlowfiles = runner.getFlowFilesForRelationship(ExecuteSparkInteractive.REL_WAIT);
        }
        runner.assertTransferCount(ExecuteSparkInteractive.REL_SUCCESS, 1);
    }

}
