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
package org.apache.nifi.controller;

import java.util.Set;

/**
 * A Snippet represents a segment of the flow
 */
public interface Snippet {

    /**
     * The id of this snippet.
     *
     * @return
     */
    public String getId();

    /**
     * Whether or not this snippet is linked to the data flow. If the Snippet is
     * deleted and is linked, then the underlying components will also be
     * deleted. If the Snippet is deleted and is NOT linked, only the Snippet is
     * removed
     *
     * @return
     */
    public boolean isLinked();

    /**
     * The parent group id of the components in this snippet.
     *
     * @return
     */
    public String getParentGroupId();

    /**
     * The connections in this snippet.
     *
     * @return
     */
    public Set<String> getConnections();

    /**
     * The funnels in this snippet.
     *
     * @return
     */
    public Set<String> getFunnels();

    /**
     * The input ports in this snippet.
     *
     * @return
     */
    public Set<String> getInputPorts();

    /**
     * The output ports in this snippet.
     *
     * @return
     */
    public Set<String> getOutputPorts();

    /**
     * The labels in this snippet.
     *
     * @return
     */
    public Set<String> getLabels();

    /**
     * Returns the identifiers of all ProcessGroups in this Snippet
     *
     * @return
     */
    public Set<String> getProcessGroups();

    /**
     * Returns the identifiers of all Processors in this Snippet
     *
     * @return
     */
    public Set<String> getProcessors();

    /**
     * Returns the identifiers of all RemoteProcessGroups in this Snippet
     *
     * @return
     */
    public Set<String> getRemoteProcessGroups();

    /**
     * Determines if this snippet is empty.
     *
     * @return
     */
    public boolean isEmpty();

}
