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

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class ExecuteSparkInteractiveTestBase {

    public static class LivyAPIHandler extends AbstractHandler {

        int session1Requests = 0;

        @Override
        public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
            baseRequest.setHandled(true);

            response.setStatus(200);

            if ("GET".equalsIgnoreCase(request.getMethod())) {

                String responseBody = "{}";
                response.setContentType("application/json");

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

                response.setContentLength(responseBody.length());

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(responseBody);
                    writer.flush();
                }

            } else if ("POST".equalsIgnoreCase(request.getMethod())) {

                String responseBody = "{}";
                response.setContentType("application/json");

                if ("/sessions".equalsIgnoreCase(target)) {
                    responseBody = "{\"id\": 1, \"kind\": \"spark\", \"state\": \"idle\"}";
                } else if ("/sessions/1/statements".equalsIgnoreCase(target)) {
                    responseBody = "{\"id\": 7}";
                }

                response.setContentLength(responseBody.length());

                try (PrintWriter writer = response.getWriter()) {
                    writer.print(responseBody);
                    writer.flush();
                }

            }
        }
    }
}
