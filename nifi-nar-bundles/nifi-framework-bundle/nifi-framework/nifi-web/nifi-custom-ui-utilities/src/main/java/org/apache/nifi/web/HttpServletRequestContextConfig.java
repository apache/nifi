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
 * An implementation of the NiFiWebContextConfig that retrieves configuration
 * from a HttpServletRequest instance.
 */
@Deprecated
public class HttpServletRequestContextConfig implements NiFiWebContextConfig {

    public static final String PROCESSOR_ID_PARAM = "processorId";

    public static final String CLIENT_ID_PARAM = "clientId";

    public static final String REVISION_PARAM = "revision";

    private final HttpServletRequest request;

    public HttpServletRequestContextConfig(final HttpServletRequest request) {
        this.request = request;
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
     * @return the processor ID retrieved from the request parameter with key
     * equal to "processorId".
     */
    @Override
    public String getProcessorId() {
        return request.getParameter(PROCESSOR_ID_PARAM);
    }

    /**
     * @return the revision retrieved from the request parameters with keys
     * equal to "clientId" and "revision".
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

        return new Revision(revision, clientId);
    }
}
