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

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlRootElement;
import org.apache.nifi.web.api.dto.BacklogDTO;

/**
 * <p>
 *     Response entity wrapping a single {@link BacklogDTO}. The {@code backlog} field is {@code null}
 *     only when the underlying component supports backlog reporting but currently has no value to
 *     report (for example, the component opted out of reporting at this moment, or its source has
 *     not yet been polled).
 * </p>
 *
 * <p>
 *     A component that does not support backlog reporting at all is rejected before this entity is
 *     produced: the framework checks support up front — Processors must implement
 *     {@code BacklogReportingProcessor} and Connectors must implement
 *     {@code BacklogReportingConnector} — and returns HTTP {@code 409 Conflict} when the component
 *     does not implement the corresponding capability interface. Clients can therefore treat a
 *     2xx response with {@code backlog == null} as "supported but no value right now" and a 409
 *     as "this component does not support backlog reporting".
 * </p>
 */
@XmlRootElement(name = "backlogEntity")
public class BacklogEntity extends Entity {

    private BacklogDTO backlog;

    @Schema(description = "The backlog reported by the component, or null if the component has no value to report at this time.")
    public BacklogDTO getBacklog() {
        return backlog;
    }

    public void setBacklog(final BacklogDTO backlog) {
        this.backlog = backlog;
    }
}
