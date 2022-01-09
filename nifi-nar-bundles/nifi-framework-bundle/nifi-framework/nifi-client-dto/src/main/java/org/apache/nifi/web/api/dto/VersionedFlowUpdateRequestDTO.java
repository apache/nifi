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

package org.apache.nifi.web.api.dto;

import io.swagger.annotations.ApiModelProperty;

import javax.xml.bind.annotation.XmlType;

@XmlType(name = "versionedFlowUpdateRequest")
public class VersionedFlowUpdateRequestDTO extends FlowUpdateRequestDTO {
    private VersionControlInformationDTO versionControlInformation;

    @ApiModelProperty(value = "The VersionControlInformation that describes where the Versioned Flow is located; this may not be populated until the request is completed.",
            accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public VersionControlInformationDTO getVersionControlInformation() {
        return versionControlInformation;
    }

    public void setVersionControlInformation(VersionControlInformationDTO versionControlInformation) {
        this.versionControlInformation = versionControlInformation;
    }
}
