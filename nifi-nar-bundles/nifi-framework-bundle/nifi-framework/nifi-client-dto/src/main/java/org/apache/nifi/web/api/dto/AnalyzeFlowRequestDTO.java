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

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * A request to analyze (a part) of the flow.
 */
@XmlType(name = "analyzeFlowRequest")
public class AnalyzeFlowRequestDTO extends AsynchronousRequestDTO<AnalyzeFlowRequestUpdateStepDTO> {
    public AnalyzeFlowRequestDTO() {
    }

    private String processGroupId;

    /**
     * The id of the process group representing (a part of) the flow to be analyzed.
     *
     * @return The id
     */
    @Schema(description = "The id of the process group representing (a part of) the flow to be analyzed.")
    public String getProcessGroupId() {
        return this.processGroupId;
    }

    public void setProcessGroupId(final String processGroupId) {
        this.processGroupId = processGroupId;
    }
}
