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

@XmlType(name = "migrationRequest")
public class MigrationRequestDTO extends AsynchronousRequestDTO<MigrationUpdateStepDTO> {
    private String connectorId;
    private MigrationRequestLocalSourceDTO localSource;
    private String payloadId;

    @Schema(description = "The identifier of the Connector receiving the migration.")
    public String getConnectorId() {
        return connectorId;
    }

    public void setConnectorId(final String connectorId) {
        this.connectorId = connectorId;
    }

    @Schema(description = "The local Process Group source for the migration request.")
    public MigrationRequestLocalSourceDTO getLocalSource() {
        return localSource;
    }

    public void setLocalSource(final MigrationRequestLocalSourceDTO localSource) {
        this.localSource = localSource;
    }

    @Schema(description = "The identifier of a previously uploaded migration payload.")
    public String getPayloadId() {
        return payloadId;
    }

    public void setPayloadId(final String payloadId) {
        this.payloadId = payloadId;
    }
}
