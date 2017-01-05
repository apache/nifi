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
package org.apache.nifi.processors.standard;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PredictableResponseServlet extends HttpServlet {
    public int statusCode = 200;
    public String responseBody = "";

    public Map<String, String> headers = new HashMap<String, String>();
    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        response.setStatus(statusCode);
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            response.setHeader(entry.getKey(), entry.getValue());
        }
        response.getWriter().print(responseBody);
        response.flushBuffer();
    }
    @Override
    protected void doHead(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        response.setStatus(statusCode);
        response.setHeader("Accept", "application/flowfile-v3,application/flowfile-v2");
        response.setHeader("x-nifi-transfer-protocol-version", "1");
        // Unless an acceptGzip parameter is explicitly set to false, respond that this server accepts gzip
        if (!Boolean.toString(false).equalsIgnoreCase(request.getParameter("acceptGzip"))) {
            response.setHeader("Accept-Encoding", "gzip");
        }
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            response.setHeader(entry.getKey(), entry.getValue());
        }
    }
}