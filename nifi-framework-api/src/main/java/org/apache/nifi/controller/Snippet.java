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

import org.apache.nifi.web.Revision;

import java.util.Map;

/**
 * A Snippet represents a segment of the flow
 */
public interface Snippet {

    /**
     * @return id of this snippet
     */
    public String getId();

    /**
     * @return parent group id of the components in this snippet
     */
    public String getParentGroupId();

    /**
     * @return connections in this snippet
     */
    public Map<String, Revision> getConnections();

    /**
     * @return funnels in this snippet
     */
    public Map<String, Revision> getFunnels();

    /**
     * @return input ports in this snippet
     */
    public Map<String, Revision> getInputPorts();

    /**
     * @return output ports in this snippet
     */
    public Map<String, Revision> getOutputPorts();

    /**
     * @return labels in this snippet
     */
    public Map<String, Revision> getLabels();

    /**
     * @return the identifiers of all ProcessGroups in this Snippet
     */
    public Map<String, Revision> getProcessGroups();

    /**
     * @return the identifiers of all Processors in this Snippet
     */
    public Map<String, Revision> getProcessors();

    /**
     * @return the identifiers of all RemoteProcessGroups in this Snippet
     */
    public Map<String, Revision> getRemoteProcessGroups();

    /**
     * @return Determines if this snippet is empty
     */
    public boolean isEmpty();

}
