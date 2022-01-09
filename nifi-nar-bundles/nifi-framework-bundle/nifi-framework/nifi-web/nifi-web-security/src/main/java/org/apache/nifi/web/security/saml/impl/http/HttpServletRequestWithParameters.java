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

import org.opensaml.ws.transport.http.HttpServletRequestAdapter;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Extends the HttpServletRequestAdapter with a provided set of parameters.
 */
public class HttpServletRequestWithParameters extends HttpServletRequestAdapter {

    private final Map<String, String> providedParameters;

    public HttpServletRequestWithParameters(final HttpServletRequest request, final Map<String, String> providedParameters) {
        super(request);
        this.providedParameters = providedParameters == null ? Collections.emptyMap() : providedParameters;
    }

    @Override
    public String getParameterValue(final String name) {
        String value = super.getParameterValue(name);
        if (value == null) {
            value = providedParameters.get(name);
        }
        return value;
    }

    @Override
    public List<String> getParameterValues(final String name) {
        List<String> combinedValues = new ArrayList<>();

        List<String> initialValues = super.getParameterValues(name);
        if (initialValues != null) {
            combinedValues.addAll(initialValues);
        }

        String providedValue = providedParameters.get(name);
        if (providedValue != null) {
            combinedValues.add(providedValue);
        }

        return combinedValues;
    }
}
