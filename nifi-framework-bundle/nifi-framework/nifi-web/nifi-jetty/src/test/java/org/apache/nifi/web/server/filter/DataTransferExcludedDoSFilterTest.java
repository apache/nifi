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
package org.apache.nifi.web.server.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class DataTransferExcludedDoSFilterTest {
    private static final String DATA_TRANSFER_URI = "/nifi-api/data-transfer";

    private static final String CONFIGURATION_URI = "/nifi-api/authentication/configuration";

    @Mock
    private FilterConfig filterConfig;

    @Mock
    private FilterChain filterChain;

    @Mock
    private HttpServletRequest request;

    @Mock
    private HttpServletResponse response;

    private DataTransferExcludedDoSFilter filter;

    @BeforeEach
    public void setFilter() throws ServletException {
        filter = new DataTransferExcludedDoSFilter();
        filter.init(filterConfig);
    }

    @Test
    public void testDoFilterChain() throws ServletException, IOException {
        when(request.getRequestURI()).thenReturn(CONFIGURATION_URI);

        filter.doFilterChain(filterChain, request, response);

        verify(request, never()).setAttribute(eq(DataTransferExcludedDoSFilter.DATA_TRANSFER_URI_ATTRIBUTE), eq(DATA_TRANSFER_URI));
    }

    @Test
    public void testDoFilterChainDataTransfer() throws ServletException, IOException {
        when(request.getRequestURI()).thenReturn(DATA_TRANSFER_URI);

        filter.doFilterChain(filterChain, request, response);

        verify(request).setAttribute(eq(DataTransferExcludedDoSFilter.DATA_TRANSFER_URI_ATTRIBUTE), eq(DATA_TRANSFER_URI));
    }
}
