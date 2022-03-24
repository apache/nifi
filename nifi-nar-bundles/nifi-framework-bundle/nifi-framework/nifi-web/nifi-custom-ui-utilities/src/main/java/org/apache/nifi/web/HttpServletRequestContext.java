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
package org.apache.nifi.web;

import javax.servlet.http.HttpServletRequest;

/**
 * An implementation of the ConfigurationRequestContext that retrieves configuration
 * from a HttpServletRequest instance.
 */
@SuppressWarnings("deprecation")
public class HttpServletRequestContext implements NiFiWebRequestContext {

    private static final String ID_PARAM = "id";

    private final UiExtensionType extensionType;
    private final HttpServletRequest request;

    public HttpServletRequestContext(final UiExtensionType extensionType, final HttpServletRequest request) {
        this.extensionType = extensionType;
        this.request = request;
    }

    @Override
    public UiExtensionType getExtensionType() {
        return extensionType;
    }

    @Override
    public String getProxiedEntitiesChain() {
        return null;
    }

    /**
     * @return the protocol scheme of the HttpServletRequest instance.
     */
    @Override
    public String getScheme() {
        return request.getScheme();
    }

    /**
     * @return the ID retrieved from the request parameter with key
     * equal to "id".
     */
    @Override
    public String getId() {
        return request.getParameter(ID_PARAM);
    }

}
