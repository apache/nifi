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

package org.apache.nifi.web.api.dto.status;

import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.xml.bind.annotation.XmlType;

/**
 * DTO for serializing the status of a ReportingTask.
 */
@XmlType(name = "reportingTaskStatus")
public class ReportingTaskStatusDTO extends ComponentStatusDTO {

    public static final String RUNNING = "RUNNING";
    public static final String STOPPED = "STOPPED";
    public static final String DISABLED = "DISABLED";

    @Schema(description = "The run status of this ReportingTask",
            accessMode = Schema.AccessMode.READ_ONLY,
            allowableValues = {"RUNNING", "STOPPED", "DISABLED"})
    @Override
    public String getRunStatus() {
        return super.getRunStatus();
    }
}
