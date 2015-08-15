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

/**
 * Context configuration for methods invoked from the NiFiWebContext.
 */
@Deprecated
public interface NiFiWebContextConfig {

    /**
     * The request protocol scheme (http or https). When scheme is https, the
     * X509Certificate can be used for subsequent remote requests.
     *
     * @return the protocol scheme
     */
    String getScheme();

    /**
     * @return the processor ID
     */
    String getProcessorId();

    /**
     * @return the revision
     */
    Revision getRevision();

    /**
     * Returns the proxied entities chain. The format of the chain is as
     * follows:
     *
     * <code>
     * &lt;CN=original-proxied-entity&gt;&lt;CN=first-proxy&gt;&lt;CN=second-proxy&gt;...
     * </code>
     *
     * @return the proxied entities chain or null if no chain
     */
    String getProxiedEntitiesChain();

}
