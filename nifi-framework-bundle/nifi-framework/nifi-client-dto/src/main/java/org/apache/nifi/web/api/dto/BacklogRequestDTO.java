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
 * A request to asynchronously determine the backlog of a Processor or Connector. Determining backlog typically
 * requires issuing a request to an external source system, which may take an indeterminate amount of time or
 * may never complete if the source system is unavailable. Rather than blocking the HTTP request thread for that
 * duration, the framework creates a {@code BacklogRequestDTO}, performs the determination in the background, and
 * expects the client to poll {@code GET} on the request's {@code uri} until {@code complete} is {@code true}, and
 * then to {@code DELETE} the request. Deleting a request that has not yet completed cancels the background
 * determination.
 */
@XmlType(name = "backlogRequest")
public class BacklogRequestDTO extends AsynchronousRequestDTO<BacklogUpdateStepDTO> {

    private String componentId;
    private BacklogDTO backlog;

    @Schema(description = "The ID of the Processor or Connector whose backlog is being determined")
    public String getComponentId() {
        return componentId;
    }

    public void setComponentId(final String componentId) {
        this.componentId = componentId;
    }

    @Schema(description = "The backlog reported by the component, populated once the request has completed successfully.",
            accessMode = Schema.AccessMode.READ_ONLY)
    public BacklogDTO getBacklog() {
        return backlog;
    }

    public void setBacklog(final BacklogDTO backlog) {
        this.backlog = backlog;
    }
}
