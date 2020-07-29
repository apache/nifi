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
package org.apache.nifi.processors.slack;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.file.FileUtils;

public class CaptureServlet extends HttpServlet {

    private static final long serialVersionUID = 8402271018449653919L;

    private volatile byte[] lastPost;
    private volatile Map<String, String> lastPostHeaders;

    public byte[] getLastPost() {
        return lastPost;
    }

    public Map<String, String> getLastPostHeaders() {
        return lastPostHeaders;
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        // Capture all the headers for reference.  Intentionally choosing to not special handling for headers with multiple values for clarity
        final Enumeration<String> headerNames = request.getHeaderNames();
        lastPostHeaders = new HashMap<>();
        while (headerNames.hasMoreElements()) {
            final String nextHeader = headerNames.nextElement();
            lastPostHeaders.put(nextHeader, request.getHeader(nextHeader));
        }

        try {
            StreamUtils.copy(request.getInputStream(), baos);
            this.lastPost = baos.toByteArray();
        } finally {
            FileUtils.closeQuietly(baos);
        }
        response.setStatus(Status.OK.getStatusCode());
    }
}
