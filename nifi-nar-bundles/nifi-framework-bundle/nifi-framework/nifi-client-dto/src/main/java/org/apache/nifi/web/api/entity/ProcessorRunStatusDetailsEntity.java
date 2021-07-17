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

package org.apache.nifi.web.api.entity;

import io.swagger.annotations.ApiModelProperty;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.ProcessorRunStatusDetailsDTO;
import org.apache.nifi.web.api.dto.RevisionDTO;

import javax.xml.bind.annotation.XmlType;

@XmlType(name = "processorRunStatusDetails")
public class ProcessorRunStatusDetailsEntity extends Entity {
    private RevisionDTO revision;
    private PermissionsDTO permissions;
    private ProcessorRunStatusDetailsDTO runStatusDetails;

    @ApiModelProperty("The revision for the Processor.")
    public RevisionDTO getRevision() {
        return revision;
    }

    public void setRevision(final RevisionDTO revision) {
        this.revision = revision;
    }

    @ApiModelProperty("The permissions for the Processor.")
    public PermissionsDTO getPermissions() {
        return permissions;
    }

    public void setPermissions(final PermissionsDTO permissions) {
        this.permissions = permissions;
    }

    @ApiModelProperty("The details of a Processor's run status")
    public ProcessorRunStatusDetailsDTO getRunStatusDetails() {
        return runStatusDetails;
    }

    public void setRunStatusDetails(final ProcessorRunStatusDetailsDTO runStatusDetails) {
        this.runStatusDetails = runStatusDetails;
    }
}
