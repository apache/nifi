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
     * @return id of this snippet
     */
    public String getId();

    /**
     * @return Whether or not this snippet is linked to the data flow. If the Snippet is
     * deleted and is linked, then the underlying components will also be
     * deleted. If the Snippet is deleted and is NOT linked, only the Snippet is
     * removed
     */
    public boolean isLinked();

    /**
     * @return parent group id of the components in this snippet
     */
    public String getParentGroupId();

    /**
     * @return connections in this snippet
     */
    public Set<String> getConnections();

    /**
     * @return funnels in this snippet
     */
    public Set<String> getFunnels();

    /**
     * @return input ports in this snippet
     */
    public Set<String> getInputPorts();

    /**
     * @return output ports in this snippet
     */
    public Set<String> getOutputPorts();

    /**
     * @return labels in this snippet
     */
    public Set<String> getLabels();

    /**
     * @return the identifiers of all ProcessGroups in this Snippet
     */
    public Set<String> getProcessGroups();

    /**
     * @return the identifiers of all Processors in this Snippet
     */
    public Set<String> getProcessors();

    /**
     * @return the identifiers of all RemoteProcessGroups in this Snippet
     */
    public Set<String> getRemoteProcessGroups();

    /**
     * @return Determines if this snippet is empty
     */
    public boolean isEmpty();

}
