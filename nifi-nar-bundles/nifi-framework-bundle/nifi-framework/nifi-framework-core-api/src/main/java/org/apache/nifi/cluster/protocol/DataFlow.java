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
package org.apache.nifi.cluster.protocol;

import java.util.Set;

public interface DataFlow {

    /**
     * @return the raw byte array of the flow
     */
    public byte[] getFlow();

    /**
     * @return the raw byte array of the snippets
     */
    public byte[] getSnippets();

    /**
     * @return the raw byte array of the Authorizer's fingerprint,
     *              null when not using a ManagedAuthorizer
     */
    public byte[] getAuthorizerFingerprint();

    /**
     * @return the component ids of components that were created as a missing ghost component
     */
    public Set<String> getMissingComponents();

}
