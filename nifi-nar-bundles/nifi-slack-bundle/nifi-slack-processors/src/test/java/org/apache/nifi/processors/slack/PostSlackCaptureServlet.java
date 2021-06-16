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

import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.nifi.stream.io.StreamUtils;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class PostSlackCaptureServlet extends HttpServlet {

    static final String REQUEST_PATH_SUCCESS_TEXT_MSG = "/success/text_msg";
    static final String REQUEST_PATH_SUCCESS_FILE_MSG = "/success/file_msg";
    static final String REQUEST_PATH_WARNING = "/warning";
    static final String REQUEST_PATH_ERROR = "/error";
    static final String REQUEST_PATH_EMPTY_JSON = "/empty-json";
    static final String REQUEST_PATH_INVALID_JSON = "/invalid-json";

    private static final String RESPONSE_SUCCESS_TEXT_MSG = "{\"ok\": true}";
    private static final String RESPONSE_SUCCESS_FILE_MSG = "{\"ok\": true, \"file\": {\"url_private\": \"slack-file-url\"}}";
    private static final String RESPONSE_WARNING = "{\"ok\": true, \"warning\": \"slack-warning\"}";
    private static final String RESPONSE_ERROR = "{\"ok\": false, \"error\": \"slack-error\"}";
    private static final String RESPONSE_EMPTY_JSON = "{}";
    private static final String RESPONSE_INVALID_JSON = "{invalid-json}";

    private static final Map<String, String> RESPONSE_MAP;

    static  {
        RESPONSE_MAP = new HashMap<>();

        RESPONSE_MAP.put(REQUEST_PATH_SUCCESS_TEXT_MSG, RESPONSE_SUCCESS_TEXT_MSG);
        RESPONSE_MAP.put(REQUEST_PATH_SUCCESS_FILE_MSG, RESPONSE_SUCCESS_FILE_MSG);
        RESPONSE_MAP.put(REQUEST_PATH_WARNING, RESPONSE_WARNING);
        RESPONSE_MAP.put(REQUEST_PATH_ERROR, RESPONSE_ERROR);
        RESPONSE_MAP.put(REQUEST_PATH_EMPTY_JSON, RESPONSE_EMPTY_JSON);
        RESPONSE_MAP.put(REQUEST_PATH_INVALID_JSON, RESPONSE_INVALID_JSON);
    }

    private volatile boolean interacted;
    private volatile Map<String, String> lastPostHeaders;
    private volatile byte[] lastPostBody;

    public Map<String, String> getLastPostHeaders() {
        return lastPostHeaders;
    }

    public byte[] getLastPostBody() {
        return lastPostBody;
    }

    public boolean hasBeenInteracted() {
        return interacted;
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws IOException {
        interacted = true;

        Enumeration<String> headerNames = request.getHeaderNames();
        lastPostHeaders = new HashMap<>();
        while (headerNames.hasMoreElements()) {
            String headerName = headerNames.nextElement();
            lastPostHeaders.put(headerName, request.getHeader(headerName));
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamUtils.copy(request.getInputStream(), baos);
        lastPostBody = baos.toByteArray();

        String responseJson = RESPONSE_MAP.get(request.getPathInfo());
        if (responseJson != null) {
            response.setContentType(ContentType.APPLICATION_JSON.toString());
            PrintWriter out = response.getWriter();
            out.print(responseJson);
            out.close();
        } else {
            response.setStatus(HttpStatus.SC_BAD_REQUEST);
        }
    }
}
