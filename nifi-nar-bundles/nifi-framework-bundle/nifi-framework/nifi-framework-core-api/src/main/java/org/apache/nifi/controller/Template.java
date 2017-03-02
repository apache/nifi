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

import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ComponentAuthorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.api.dto.TemplateDTO;

public class Template implements ComponentAuthorizable {

    private final TemplateDTO dto;
    private volatile ProcessGroup processGroup;

    public Template(final TemplateDTO dto) {
        this.dto = dto;
    }

    @Override
    public String getIdentifier() {
        return dto.getId();
    }

    @Override
    public String getProcessGroupIdentifier() {
        final ProcessGroup procGroup = getProcessGroup();
        return procGroup == null ? null : procGroup.getIdentifier();
    }

    /**
     * Returns a TemplateDTO object that describes the contents of this Template
     *
     * @return template dto
     */
    public TemplateDTO getDetails() {
        return dto;
    }

    public void setProcessGroup(final ProcessGroup group) {
        this.processGroup = group;
    }

    public ProcessGroup getProcessGroup() {
        return processGroup;
    }


    @Override
    public Authorizable getParentAuthorizable() {
        return processGroup;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Template, dto.getId(), dto.getName());
    }

    @Override
    public String toString() {
        return "Template[id=" + getIdentifier() + "]";
    }
}
