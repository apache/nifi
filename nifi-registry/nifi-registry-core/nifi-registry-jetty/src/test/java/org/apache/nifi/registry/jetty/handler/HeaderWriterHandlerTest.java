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
package org.apache.nifi.registry.jetty.handler;

import org.eclipse.jetty.server.Request;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HeaderWriterHandlerTest {
    private static final String TARGET = HeaderWriterHandler.class.getSimpleName();

    @Mock
    private Request request;

    @Mock
    private HttpServletRequest httpServletRequest;

    @Mock
    private HttpServletResponse httpServletResponse;

    private HeaderWriterHandler handler;

    @BeforeEach
    void setHandler() {
        handler = new HeaderWriterHandler();
    }

    @Test
    void testDoHandle() {
        handler.doHandle(TARGET, request, httpServletRequest, httpServletResponse);

        verifyStandardHeaders();
    }

    @Test
    void testDoHandleSecure() {
        when(httpServletRequest.isSecure()).thenReturn(true);

        handler.doHandle(TARGET, request, httpServletRequest, httpServletResponse);

        verifyStandardHeaders();
        verify(httpServletResponse).setHeader(eq(HeaderWriterHandler.STRICT_TRANSPORT_SECURITY_HEADER), any());
    }

    private void verifyStandardHeaders() {
        verify(httpServletResponse).setHeader(eq(HeaderWriterHandler.CONTENT_SECURITY_POLICY_HEADER), any());
        verify(httpServletResponse).setHeader(eq(HeaderWriterHandler.FRAME_OPTIONS_HEADER), any());
        verify(httpServletResponse).setHeader(eq(HeaderWriterHandler.XSS_PROTECTION_HEADER), any());
    }
}
