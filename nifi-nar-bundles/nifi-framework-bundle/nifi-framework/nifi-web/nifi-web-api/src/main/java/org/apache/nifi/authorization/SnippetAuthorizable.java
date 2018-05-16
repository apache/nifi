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
package org.apache.nifi.authorization;

import org.apache.nifi.authorization.resource.Authorizable;

import java.util.Set;

/**
 * Authorizable for a Snippet.
 */
public interface SnippetAuthorizable {
    /**
     * The authorizable for the parent process group of this snippet.
     *
     * @return authorizable for parent process group of this snippet
     */
    Authorizable getParentProcessGroup();

    /**
     * The authorizables for selected processors. Non null
     *
     * @return processors
     */
    Set<ComponentAuthorizable> getSelectedProcessors();

    /**
     * The authorizables for selected connections. Non null
     *
     * @return  connections
     */
    Set<ConnectionAuthorizable> getSelectedConnections();

    /**
     * The authorizables for selected input ports. Non null
     *
     * @return input ports
     */
    Set<Authorizable> getSelectedInputPorts();

    /**
     * The authorizables for selected output ports. Non null
     *
     * @return output ports
     */
    Set<Authorizable> getSelectedOutputPorts();

    /**
     * The authorizables for selected funnels. Non null
     *
     * @return funnels
     */
    Set<Authorizable> getSelectedFunnels();

    /**
     * The authorizables for selected labels. Non null
     *
     * @return labels
     */
    Set<Authorizable> getSelectedLabels();

    /**
     * The authorizables for selected process groups. Non null
     *
     * @return process groups
     */
    Set<ProcessGroupAuthorizable> getSelectedProcessGroups();

    /**
     * The authorizables for selected remote process groups. Non null
     *
     * @return remote process groups
     */
    Set<Authorizable> getSelectedRemoteProcessGroups();
}
