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
package org.apache.nifi.web.api.dto.flow;

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;

import jakarta.xml.bind.annotation.XmlType;

/**
 * Breadcrumb for the flow.
 */
@XmlType(name = "flowBreadcrumb")
public class FlowBreadcrumbDTO {

    private String id;
    private String name;
    private VersionControlInformationDTO versionControlInformation;

    /**
     * The id for this group.
     *
     * @return The id
     */
    @Schema(description = "The id of the group."
    )
    public String getId() {
        return this.id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    /**
     * The name for this group.
     *
     * @return The name
     */
    @Schema(description = "The id of the group."
    )
    public String getName() {
        return this.name;
    }

    public void setName(final String name) {
        this.name = name;
    }

    /**
     * @return the process group version control information or null if not version controlled
     */
    @Schema(description = "The process group version control information or null if not version controlled."
    )
    public VersionControlInformationDTO getVersionControlInformation() {
        return versionControlInformation;
    }

    public void setVersionControlInformation(VersionControlInformationDTO versionControlInformation) {
        this.versionControlInformation = versionControlInformation;
    }
}
