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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response.Status;

import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.nifi.stream.io.StreamUtils;

public class CaptureServlet extends HttpServlet {

    private static final long serialVersionUID = 8402271018449653919L;

    private volatile byte[] lastPost;

    public byte[] getLastPost() {
        return lastPost;
    }

    @Override
    protected void doPost(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamUtils.copy(request.getInputStream(), baos);
        this.lastPost = baos.toByteArray();

        response.setStatus(Status.OK.getStatusCode());
    }

    @Override
    protected void doHead(final HttpServletRequest request, final HttpServletResponse response) throws ServletException, IOException {
        response.
                setHeader("Accept", "application/flowfile-v3,application/flowfile-v2");
        response.setHeader("x-nifi-transfer-protocol-version", "1");
        response.setHeader("Accept-Encoding", "gzip");
    }
}
