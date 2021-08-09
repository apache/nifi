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
package org.apache.nifi.web.security.saml.impl.http;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensaml.ws.transport.http.HttpServletRequestAdapter;

import javax.servlet.http.HttpServletRequest;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TestHttpServletRequestWithParameters {

    @Mock
    private HttpServletRequest request;

    @Test
    public void testGetParameterValueWhenNoExtraParameters() {
        final String paramName = "fooParam";
        final String paramValue = "fooValue";
        when(request.getParameter(eq(paramName))).thenReturn(paramValue);

        final HttpServletRequestAdapter requestAdapter = new HttpServletRequestWithParameters(request, Collections.emptyMap());
        final String result = requestAdapter.getParameterValue(paramName);
        assertEquals(paramValue, result);
    }

    @Test
    public void testGetParameterValueWhenExtraParameters() {
        final String paramName = "fooParam";
        final String paramValue = "fooValue";

        final Map<String,String> extraParams = new HashMap<>();
        extraParams.put(paramName, paramValue);

        when(request.getParameter(any())).thenReturn(null);

        final HttpServletRequestAdapter requestAdapter = new HttpServletRequestWithParameters(request, extraParams);
        final String result = requestAdapter.getParameterValue(paramName);
        assertEquals(paramValue, result);
    }

    @Test
    public void testGetParameterValuesWhenNoExtraParameters() {
        final String paramName = "fooParam";
        final String paramValue = "fooValue";
        when(request.getParameterValues(eq(paramName))).thenReturn(new String[] {paramValue});

        final HttpServletRequestAdapter requestAdapter = new HttpServletRequestWithParameters(request, Collections.emptyMap());
        final List<String> results = requestAdapter.getParameterValues(paramName);
        assertEquals(1, results.size());
        assertEquals(paramValue, results.get(0));
    }

    @Test
    public void testGetParameterValuesWhenExtraParameters() {
        final String paramName = "fooParam";
        final String paramValue1 = "fooValue1";
        when(request.getParameterValues(eq(paramName))).thenReturn(new String[] {paramValue1});

        final String paramValue2 = "fooValue2";
        final Map<String,String> extraParams = new HashMap<>();
        extraParams.put(paramName, paramValue2);

        final HttpServletRequestAdapter requestAdapter = new HttpServletRequestWithParameters(request, extraParams);
        final List<String> results = requestAdapter.getParameterValues(paramName);
        assertEquals(2, results.size());
        assertTrue(results.contains(paramValue1));
        assertTrue(results.contains(paramValue2));
    }
}
