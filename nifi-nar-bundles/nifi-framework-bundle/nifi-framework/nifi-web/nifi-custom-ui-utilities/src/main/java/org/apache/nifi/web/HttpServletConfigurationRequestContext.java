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
public class HttpServletConfigurationRequestContext extends HttpServletRequestContext implements NiFiWebConfigurationRequestContext {

    private static final String ID_PARAM = "id";
    private static final String CLIENT_ID_PARAM = "clientId";
    private static final String REVISION_PARAM = "revision";

    private final HttpServletRequest request;

    public HttpServletConfigurationRequestContext(final UiExtensionType extensionType, final HttpServletRequest request) {
        super(extensionType, request);
        this.request = request;
    }

    /**
     * @return the revision retrieved from the request parameters with keys
     * equal to "clientId", "revision", and "id".
     */
    @Override
    public Revision getRevision() {
        final String revisionParamVal = request.getParameter(REVISION_PARAM);
        Long revision;
        try {
            revision = Long.parseLong(revisionParamVal);
        } catch (final Exception ex) {
            revision = null;
        }

        final String clientId = request.getParameter(CLIENT_ID_PARAM);
        final String componentId = request.getParameter(ID_PARAM);

        return new Revision(revision, clientId, componentId);
    }

}
